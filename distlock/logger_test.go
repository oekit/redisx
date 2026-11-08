package distlock

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/logx"
	"github.com/redis/go-redis/v9"
)

// captureLogger 是实现完整 logx.Logger 的测试捕获器（#120）：记录 Warn/Error
// 事件；notify 在首次 Warn/Error 时关闭，作为 watchdog 异步告警的同步点
// （不用 sleep 凑绿）。
type captureLogger struct {
	mu     sync.Mutex
	events []captureEvent
	notify chan struct{}
}

type captureEvent struct {
	level string
	msg   string
	kv    []any
}

func newCaptureLogger() *captureLogger {
	return &captureLogger{notify: make(chan struct{})}
}

func (c *captureLogger) record(level, msg string, kv []any) {
	c.mu.Lock()
	if c.events == nil {
		close(c.notify)
	}
	c.events = append(c.events, captureEvent{level: level, msg: msg, kv: kv})
	c.mu.Unlock()
}

func (c *captureLogger) countByMsg(msg string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.events {
		if e.msg == msg {
			n++
		}
	}
	return n
}

func (c *captureLogger) Debug(msg string, kv ...any)                               {}
func (c *captureLogger) Info(msg string, kv ...any)                                {}
func (c *captureLogger) Warn(msg string, kv ...any)                                { c.record("warn", msg, kv) }
func (c *captureLogger) Error(msg string, kv ...any)                               { c.record("error", msg, kv) }
func (c *captureLogger) Debugf(format string, args ...any)                         {}
func (c *captureLogger) Infof(format string, args ...any)                          {}
func (c *captureLogger) Warnf(format string, args ...any)                          {}
func (c *captureLogger) Errorf(format string, args ...any)                         {}
func (c *captureLogger) CtxDebug(ctx context.Context, msg string, kv ...any)       {}
func (c *captureLogger) CtxInfo(ctx context.Context, msg string, kv ...any)        {}
func (c *captureLogger) CtxWarn(ctx context.Context, msg string, kv ...any)        {}
func (c *captureLogger) CtxError(ctx context.Context, msg string, kv ...any)       {}
func (c *captureLogger) CtxDebugf(ctx context.Context, format string, args ...any) {}
func (c *captureLogger) CtxInfof(ctx context.Context, format string, args ...any)  {}
func (c *captureLogger) CtxWarnf(ctx context.Context, format string, args ...any)  {}
func (c *captureLogger) CtxErrorf(ctx context.Context, format string, args ...any) {}
func (c *captureLogger) WithCtx(ctx context.Context) logx.Logger                   { return c }
func (c *captureLogger) WithService(name string) logx.Logger                       { return c }
func (c *captureLogger) WithAttrs(attrs ...logx.Attr) logx.Logger                  { return c }
func (c *captureLogger) WithGroup(name string) logx.Logger                         { return c }
func (c *captureLogger) SetLevel(level logx.Level)                                 {}

// 编译期契约：captureLogger 满足完整 logx.Logger。
var _ logx.Logger = (*captureLogger)(nil)

// swapDefault 暂时把进程级 logx.Default 指向捕获 logger，返回还原函数
// （t.Cleanup 还原）。用于 #120 的双向验收：注入实例的告警不得落 Default，
// 未注入实例的告警必须仍落 Default。
func swapDefault(t *testing.T) *captureLogger {
	t.Helper()
	prev := logx.Default()
	captured := newCaptureLogger()
	logx.SetDefault(captured)
	t.Cleanup(func() { logx.SetDefault(prev) })
	return captured
}

// waitWarn 等待捕获器收到任意 Warn/Error（watchdog 异步告警的确定性同步）。
func waitWarn(t *testing.T, c *captureLogger) {
	t.Helper()
	select {
	case <-c.notify:
	case <-time.After(3 * time.Second):
		t.Fatal("expected a warning within 3s (watchdog lifetime expiry), got none")
	}
}

// newShortLifetimeLocker 构造「续期安全网很快到期」的 locker：TryLock 后
// watchdog 在 maxLifetime 到期时必打 "auto renew exceeded max lifetime"。
func newShortLifetimeLocker(t *testing.T, mrAddr string, lg logx.Logger) *RedisLocker {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: mrAddr})
	t.Cleanup(func() { _ = c.Close() })
	return NewRedisLocker(c,
		WithLogger(lg),
		WithAutoRenew(),
		WithRenewInterval(20*time.Millisecond),
		WithMaxAutoRenew(60*time.Millisecond),
	)
}

// newQuietLocker 构造只安静续期、不触发安全网告警的 locker（对照组）：
// 同样注入独立 logger 并持有锁，但 maxLifetime 足够长，测试窗口内零告警。
func newQuietLocker(t *testing.T, mrAddr string, lg logx.Logger) *RedisLocker {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: mrAddr})
	t.Cleanup(func() { _ = c.Close() })
	return NewRedisLocker(c,
		WithLogger(lg),
		WithAutoRenew(),
		WithRenewInterval(20*time.Millisecond),
		WithMaxAutoRenew(time.Hour),
	)
}

// TestRedisLocker_InjectedLoggerOnly 验收判据①：注入自定义 logger 后，
// watchdog 告警只进该 logger；进程级 Default（负对照面）零输出。
func TestRedisLocker_InjectedLoggerOnly(t *testing.T) {
	mr := miniredis.RunT(t)
	injected := newCaptureLogger()
	defaultCap := swapDefault(t) // 负对照面

	l := newShortLifetimeLocker(t, mr.Addr(), injected)
	ctx := context.Background()
	if _, err := l.TryLock(ctx, "lk", time.Second); err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	waitWarn(t, injected)
	const msg = "auto renew exceeded max lifetime, giving up lock"
	if got := injected.countByMsg(msg); got != 1 {
		t.Fatalf("injected logger got %d %q events, want 1", got, msg)
	}
	if got := defaultCap.countByMsg(msg); got != 0 {
		t.Fatalf("logx.Default got %d %q events, want 0 (per-instance injection must bypass global)", got, msg)
	}
	waitForNoAutoRenewGoroutines(t)
}

// TestRedisLocker_DefaultLoggerFallback 验收判据②（负对照）：不注入 logger
// 时行为不回归——watchdog 告警仍落 logx.Default()。
func TestRedisLocker_DefaultLoggerFallback(t *testing.T) {
	mr := miniredis.RunT(t)
	defaultCap := swapDefault(t)

	l := newShortLifetimeLocker(t, mr.Addr(), nil) // nil = 缺省（Delegating 行为不变）
	ctx := context.Background()
	if _, err := l.TryLock(ctx, "lk", time.Second); err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	waitWarn(t, defaultCap)
	const msg = "auto renew exceeded max lifetime, giving up lock"
	if got := defaultCap.countByMsg(msg); got != 1 {
		t.Fatalf("logx.Default got %d %q events, want 1 (default behavior must not regress)", got, msg)
	}
	waitForNoAutoRenewGoroutines(t)
}

// TestRedisLocker_TwoInstancesNoCrossTalk 验收判据③：两个实例各注入不同
// logger 同时运行，watchdog 告警不串——证明注入是 per-instance 而非进程级。
// A 的安全网到期告警；B（对照组）安静续期、同窗口零告警。
func TestRedisLocker_TwoInstancesNoCrossTalk(t *testing.T) {
	mr := miniredis.RunT(t)
	logA := newCaptureLogger()
	logB := newCaptureLogger()

	lockerA := newShortLifetimeLocker(t, mr.Addr(), logA)
	lockerB := newQuietLocker(t, mr.Addr(), logB)
	ctx := context.Background()
	ownerA, err := lockerA.TryLock(ctx, "ka", time.Second)
	if err != nil {
		t.Fatalf("TryLock A: %v", err)
	}
	ownerB, err := lockerB.TryLock(ctx, "kb", time.Second)
	if err != nil {
		t.Fatalf("TryLock B: %v", err)
	}
	defer func() {
		// 清理：显式停掉两个 watchdog（续期本可持续 1h，不清理会泄漏
		// goroutine 并污染后续 goroutine 计数类用例的基线）。
		lockerA.stopRenewState("ka", ownerA)
		lockerB.stopRenewState("kb", ownerB)
		waitForNoAutoRenewGoroutines(t)
	}()

	waitWarn(t, logA) // A 的安全网到期即告警；B 的不触发（各自实例 logger）
	const msg = "auto renew exceeded max lifetime, giving up lock"
	if got := logA.countByMsg(msg); got == 0 {
		t.Fatal("logger A must receive its own watchdog warning")
	}
	if got := logB.countByMsg(msg); got != 0 {
		t.Fatalf("logger B got %d events, want 0 (per-instance, not process-wide)", got)
	}
}
