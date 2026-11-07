package ratelimit

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// discardLogger 静音 go-redis 内部日志：故障注入测试（Close miniredis /
// flaky 包装）必然产生连接池重拨告警，属预期噪声，静音保持测试输出洁净.
type discardLogger struct{}

func (discardLogger) Printf(context.Context, string, ...any) {}

func TestMain(m *testing.M) {
	redis.SetLogger(discardLogger{})
	os.Exit(m.Run())
}

// newTestStore 启动 miniredis 并返回真实 go-redis Scripter 客户端.
func newTestStore(t *testing.T) (redis.Scripter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c, mr
}

// newTestStoreFastFail 同 newTestStore，但客户端禁用重试——故障注入场景
// （Close miniredis）下避免 dial 重试退避把单次命令拖到 ~1.7s.
func newTestStoreFastFail(t *testing.T) (redis.Scripter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	return c, mr
}

// stubScripter 固定应答的 Scripter 桩，用于构造非预期应答路径.
type stubScripter struct {
	val any
	err error
}

func (s stubScripter) Eval(_ context.Context, _ string, _ []string, _ ...any) *redis.Cmd {
	return redis.NewCmdResult(s.val, s.err)
}

func (s stubScripter) EvalSha(_ context.Context, _ string, _ []string, _ ...any) *redis.Cmd {
	return redis.NewCmdResult(s.val, s.err)
}

func (s stubScripter) EvalRO(_ context.Context, _ string, _ []string, _ ...any) *redis.Cmd {
	return redis.NewCmdResult(s.val, s.err)
}

func (s stubScripter) EvalShaRO(_ context.Context, _ string, _ []string, _ ...any) *redis.Cmd {
	return redis.NewCmdResult(s.val, s.err)
}

func (s stubScripter) ScriptExists(_ context.Context, _ ...string) *redis.BoolSliceCmd {
	return redis.NewBoolSliceResult(nil, s.err)
}

func (s stubScripter) ScriptLoad(_ context.Context, _ string) *redis.StringCmd {
	return redis.NewStringResult("", s.err)
}

func TestPeriodLimit_TakeStateValues(t *testing.T) {
	// 三态常量取值与 go-zero core/limit 对齐（API 稳定性）
	if Unknown != 0 || Allowed != 1 || HitQuota != 2 || OverQuota != 3 {
		t.Fatalf("unexpected state values: Unknown=%d Allowed=%d HitQuota=%d OverQuota=%d",
			Unknown, Allowed, HitQuota, OverQuota)
	}
}

func TestPeriodLimit_Take_QuotaBoundaries(t *testing.T) {
	store, _ := newTestStore(t)
	l := NewPeriodLimit(time.Minute, 3, store, "sms")
	ctx := context.Background()

	// quota-1 次全部 Allowed
	for i := 1; i <= 2; i++ {
		code, err := l.Take(ctx, "13800000000")
		if err != nil {
			t.Fatalf("take %d: unexpected err: %v", i, err)
		}
		if code != Allowed {
			t.Fatalf("take %d: expected Allowed, got %d", i, code)
		}
	}

	// 第 quota 次 HitQuota（恰好用完，本次仍放行）
	if code, err := l.Take(ctx, "13800000000"); err != nil || code != HitQuota {
		t.Fatalf("expected HitQuota, got code=%d err=%v", code, err)
	}

	// 第 quota+1 次 OverQuota
	if code, err := l.Take(ctx, "13800000000"); err != nil || code != OverQuota {
		t.Fatalf("expected OverQuota, got code=%d err=%v", code, err)
	}

	// 不同 key 配额独立
	if code, err := l.Take(ctx, "13900000000"); err != nil || code != Allowed {
		t.Fatalf("expected independent quota for other key, got code=%d err=%v", code, err)
	}
}

func TestPeriodLimit_Take_WindowResetViaFastForward(t *testing.T) {
	store, mr := newTestStore(t)
	l := NewPeriodLimit(time.Minute, 2, store, "sms")
	ctx := context.Background()

	if code, _ := l.Take(ctx, "u1"); code != Allowed {
		t.Fatalf("expected Allowed, got %d", code)
	}
	if code, _ := l.Take(ctx, "u1"); code != HitQuota {
		t.Fatalf("expected HitQuota, got %d", code)
	}
	if code, _ := l.Take(ctx, "u1"); code != OverQuota {
		t.Fatalf("expected OverQuota, got %d", code)
	}

	// key 布局：keyPrefix + ":" + key；首请求已设置窗口 TTL
	if !mr.Exists("sms:u1") {
		t.Fatal("expected key sms:u1 to exist")
	}
	if ttl := mr.TTL("sms:u1"); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("expected TTL in (0, 1m], got %v", ttl)
	}

	// FastForward 越过窗口边界 → 计数过期，配额重置
	mr.FastForward(time.Minute + time.Second)
	if code, err := l.Take(ctx, "u1"); err != nil || code != Allowed {
		t.Fatalf("expected Allowed after window reset, got code=%d err=%v", code, err)
	}
}

func TestPeriodLimit_Take_EmptyPrefixUsesKeyAsIs(t *testing.T) {
	store, mr := newTestStore(t)
	l := NewPeriodLimit(time.Minute, 1, store, "")

	if _, err := l.Take(context.Background(), "bare"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !mr.Exists("bare") {
		t.Fatalf("expected key 'bare', got keys %v", mr.Keys())
	}
}

func TestPeriodLimit_Take_RedisDownReturnsUnknown(t *testing.T) {
	store, mr := newTestStoreFastFail(t)
	l := NewPeriodLimit(time.Minute, 1, store, "pfx")

	mr.Close()
	// PeriodLimit 不做本地兜底（固定窗口计数无法本地近似）：
	// 故障时返回 Unknown + err，由调用方决定 fail-open / fail-closed
	code, err := l.Take(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error when redis is down")
	}
	if code != Unknown {
		t.Fatalf("expected Unknown, got %d", code)
	}
}

func TestPeriodLimit_Take_CtxCanceled(t *testing.T) {
	store, _ := newTestStore(t)
	l := NewPeriodLimit(time.Minute, 10, store, "pfx")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	code, err := l.Take(ctx, "k")
	if code != Unknown {
		t.Fatalf("expected Unknown, got %d", code)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestPeriodLimit_Take_UnknownCode(t *testing.T) {
	l := NewPeriodLimit(time.Minute, 10, stubScripter{val: "not-an-int"}, "pfx")

	code, err := l.Take(context.Background(), "k")
	if code != Unknown {
		t.Fatalf("expected Unknown, got %d", code)
	}
	if !errors.Is(err, ErrUnknownCode) {
		t.Fatalf("expected ErrUnknownCode, got %v", err)
	}
}

func TestPeriodLimit_WithPeriodAlign(t *testing.T) {
	store, mr := newTestStore(t)
	l := NewPeriodLimit(time.Hour, 5, store, "pfx", WithPeriodAlign())

	// 对齐自然周期：过期秒数 ∈ [1, period]
	exp := l.calcExpireSeconds()
	if exp < 1 || exp > 3600 {
		t.Fatalf("aligned expire out of range [1,3600]: %d", exp)
	}

	if _, err := l.Take(context.Background(), "k"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ttl := mr.TTL("pfx:k"); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("expected aligned TTL in (0, 1h], got %v", ttl)
	}

	// 普通模式：过期秒数即窗口本身
	plain := NewPeriodLimit(time.Hour, 5, store, "pfx")
	if got := plain.calcExpireSeconds(); got != 3600 {
		t.Fatalf("expected 3600, got %d", got)
	}
}

// TestPeriodLimit_PanicOnInvalidQuota 回归（#92）：quota<=0 属于编程错误，
// fail-fast panic（与 NewTokenLimiter 对 rate/burst 的校验口径一致）——
// 静默接受会让 Lua 的 current < limit / current == limit 均不成立，
// 每次 Take 都 OverQuota，流量 100% 被拒且无任何报错。
func TestPeriodLimit_PanicOnInvalidQuota(t *testing.T) {
	store, _ := newTestStore(t)

	for name, fn := range map[string]func(){
		"zero quota":     func() { NewPeriodLimit(time.Minute, 0, store, "x") },
		"negative quota": func() { NewPeriodLimit(time.Minute, -1, store, "x") },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("%s: expected panic", name)
				}
			}()
			fn()
		}()
	}
}

func TestPeriodLimit_SubSecondPeriodClampedToOneSecond(t *testing.T) {
	store, _ := newTestStore(t)
	l := NewPeriodLimit(300*time.Millisecond, 1, store, "pfx")

	if got := l.calcExpireSeconds(); got != 1 {
		t.Fatalf("expected sub-second period clamped to 1s, got %d", got)
	}
}
