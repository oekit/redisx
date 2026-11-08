package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	xrate "golang.org/x/time/rate"
)

// newLocalBucket 构造 rate=每秒 rate 个、burst=burst 的本地令牌桶（xrate.Limiter
// 天然满足 LocalLimiter）。
func newLocalBucket(rate, burst int) *xrate.Limiter {
	return xrate.NewLimiter(xrate.Every(time.Second/time.Duration(rate)), burst)
}

// TestResilientLimiter_RedisNormal Redis 正常路径：三态收敛进 Decision 且
// Source 恒为 SourceRedis、err 为 nil——配额拒绝（OverQuota）与后续的故障
// 降级必须可区分。
func TestResilientLimiter_RedisNormal(t *testing.T) {
	store, mr := newTestStore(t)
	l := NewResilientLimiter(NewPeriodLimit(time.Minute, 2, store, "rl"), nil)
	ctx := context.Background()

	for i := 1; i <= 2; i++ {
		d, err := l.Allow(ctx, "k")
		if err != nil {
			t.Fatalf("allow %d: %v", i, err)
		}
		if !d.Allowed || d.Source != SourceRedis {
			t.Fatalf("allow %d = %+v, want {true SourceRedis}", i, d)
		}
	}
	_ = mr
	d, err := l.Allow(ctx, "k")
	if err != nil {
		t.Fatalf("allow 3: %v", err)
	}
	if d.Allowed || d.Source != SourceRedis {
		t.Fatalf("over-quota = %+v, want {false SourceRedis} (quota denial, not failure)", d)
	}
}

// TestResilientLimiter_FallbackLocalBucket 验收判据①（Redis 不可用形态）：
// 故障时降级到本地令牌桶，Source=SourceLocalFallback，且**不误放行超额流量**
// ——桶 burst=1、rate=1/s，第二发必须被本地桶拒掉。工厂按 key 缓存桶
// （消费方真实形态，如 KeyedLimiterOf；桶随用随建则限速形同虚设）。
func TestResilientLimiter_FallbackLocalBucket(t *testing.T) {
	store, mr := newTestStoreFastFail(t)
	var (
		mu      sync.Mutex
		buckets = map[string]LocalLimiter{}
	)
	l := NewResilientLimiter(NewPeriodLimit(time.Minute, 100, store, "rl"),
		func(key string) LocalLimiter {
			mu.Lock()
			defer mu.Unlock()
			b, ok := buckets[key]
			if !ok {
				b = newLocalBucket(1, 1) // 每 key 每秒 1 个、容量 1
				buckets[key] = b
			}
			return b
		})
	ctx := context.Background()

	mr.Close() // Redis 故障

	d, err := l.Allow(ctx, "k")
	if err == nil {
		t.Fatal("expected underlying redis error to surface")
	}
	if !d.Allowed || d.Source != SourceLocalFallback {
		t.Fatalf("first fallback = %+v, want {true SourceLocalFallback}", d)
	}
	// 第二发：同一 key 的桶已空（burst=1，refill 1s 后才有余量）——不得误放行。
	d, err = l.Allow(ctx, "k")
	if err == nil {
		t.Fatal("expected underlying redis error to surface")
	}
	if d.Allowed || d.Source != SourceLocalFallback {
		t.Fatalf("second fallback = %+v, want {false SourceLocalFallback} (local bucket must still rate-limit)", d)
	}
}

// TestResilientLimiter_SwitchBackToRedis 验收判据②（负对照）：Redis 恢复后
// 必须切回 redis 源，不能永久钉在本地兜底——否则多实例限流等于失效。
// 故障形态用 miniredis Close/Restart（同端口、数据保留）真实往返。
func TestResilientLimiter_SwitchBackToRedis(t *testing.T) {
	store, mr := newTestStoreFastFail(t)
	l := NewResilientLimiter(NewPeriodLimit(time.Minute, 100, store, "rl"),
		func(string) LocalLimiter { return newLocalBucket(1000, 1000) })
	ctx := context.Background()

	mr.Close()
	if d, _ := l.Allow(ctx, "k"); d.Source != SourceLocalFallback {
		t.Fatalf("during outage source = %v, want SourceLocalFallback", d.Source)
	}
	if err := mr.Restart(); err != nil {
		t.Fatalf("restart miniredis: %v", err)
	}
	// 恢复后下一次判定必须回 redis 源（每次 Allow 都先试 Redis，无粘滞状态）。
	d, err := l.Allow(ctx, "k")
	if err != nil {
		t.Fatalf("after recovery: %v", err)
	}
	if !d.Allowed || d.Source != SourceRedis {
		t.Fatalf("after recovery = %+v, want {true SourceRedis} (must switch back, not stay pinned on local)", d)
	}
}

// TestResilientLimiter_FailClosed 严格口径：故障一律拒绝（SourceFailClosed），
// 本地桶存在也不参与（不被消耗）。
func TestResilientLimiter_FailClosed(t *testing.T) {
	store, mr := newTestStoreFastFail(t)
	bucket := newLocalBucket(1000, 1000)
	l := NewResilientLimiter(NewPeriodLimit(time.Minute, 100, store, "rl"),
		func(string) LocalLimiter { return bucket },
		WithFailMode(FailModeClosed))
	ctx := context.Background()

	mr.Close()
	d, err := l.Allow(ctx, "k")
	if err == nil {
		t.Fatal("expected underlying redis error to surface")
	}
	if d.Allowed || d.Source != SourceFailClosed {
		t.Fatalf("fail-closed = %+v, want {false SourceFailClosed}", d)
	}
	// 本地桶未被消耗：直接判定应仍有满额余量。
	if !bucket.AllowN(time.Now(), 1) {
		t.Fatal("local bucket must not be consumed under FailModeClosed")
	}
}

// TestResilientLimiter_FailOpenWithoutLocalBucket 未提供本地桶的 fail-open：
// 故障时直接放行（SourceFailOpen），不 panic、不触碰任何桶。
func TestResilientLimiter_FailOpenWithoutLocalBucket(t *testing.T) {
	store, mr := newTestStoreFastFail(t)
	l := NewResilientLimiter(NewPeriodLimit(time.Minute, 100, store, "rl"), nil)
	ctx := context.Background()

	mr.Close()
	d, err := l.Allow(ctx, "k")
	if err == nil {
		t.Fatal("expected underlying redis error to surface")
	}
	if !d.Allowed || d.Source != SourceFailOpen {
		t.Fatalf("fail-open without bucket = %+v, want {true SourceFailOpen}", d)
	}
}

// TestResilientLimiter_FallbackMetrics 验收判据③的观测面：非 Redis 判定逐次
// 回调 Source，Redis 判定不回调——「现在在 Redis 限还是在本地限」可进 metrics。
func TestResilientLimiter_FallbackMetrics(t *testing.T) {
	store, mr := newTestStoreFastFail(t)
	var mu sync.Mutex
	var sources []Source
	l := NewResilientLimiter(NewPeriodLimit(time.Minute, 100, store, "rl"), nil,
		WithFallbackMetrics(func(s Source) {
			mu.Lock()
			defer mu.Unlock()
			sources = append(sources, s)
		}))
	ctx := context.Background()

	if _, err := l.Allow(ctx, "k"); err != nil {
		t.Fatalf("redis path: %v", err)
	}
	mu.Lock()
	if len(sources) != 0 {
		t.Fatalf("redis verdicts must not fire fallback metrics, got %v", sources)
	}
	mu.Unlock()

	mr.Close()
	_, _ = l.Allow(ctx, "k") // SourceFailOpen（无本地桶）
	mu.Lock()
	defer mu.Unlock()
	if len(sources) != 1 || sources[0] != SourceFailOpen {
		t.Fatalf("fallback metrics = %v, want [fail-open]", sources)
	}
}

// TestResilientLimiter_ConcurrentNoOverrun 验收判据④：同一 key 多 goroutine
// 并发判定，redis 路径在 tick 边界不超发——放行总数不得超过配额
// （PeriodLimit 的 Lua 原子性 + ResilientLimiter 自身无共享可变状态）。
func TestResilientLimiter_ConcurrentNoOverrun(t *testing.T) {
	store, _ := newTestStore(t)
	const quota = 10
	l := NewResilientLimiter(NewPeriodLimit(time.Minute, quota, store, "cc"), nil)
	ctx := context.Background()

	const goroutines = 40
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		allowed int
	)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := l.Allow(ctx, "k")
			if err != nil {
				t.Errorf("concurrent allow: %v", err)
				return
			}
			if d.Source != SourceRedis {
				t.Errorf("source = %v, want SourceRedis", d.Source)
				return
			}
			if d.Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if allowed > quota {
		t.Fatalf("allowed %d exceeds quota %d (#118: must not overrun at tick boundary)", allowed, quota)
	}
}

// TestResilientLimiter_NilDistPanics dist 为 nil 属编程错误，fail-fast
// （与 NewPeriodLimit 对 quota<=0 的 panic 口径一致）。
func TestResilientLimiter_NilDistPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil dist must panic")
		}
	}()
	NewResilientLimiter(nil, nil)
}

// TestSourceString 观测标识稳定（metrics/日志标签勿漂移）。
func TestSourceString(t *testing.T) {
	cases := map[Source]string{
		SourceRedis:         "redis",
		SourceLocalFallback: "local-fallback",
		SourceFailOpen:      "fail-open",
		SourceFailClosed:    "fail-closed",
		Source(42):          "unknown",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("Source(%d).String() = %q, want %q", int(s), got, want)
		}
	}
}
