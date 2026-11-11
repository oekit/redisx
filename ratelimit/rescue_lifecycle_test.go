package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- redisx#165：探活 watchdog 停机口 ---

// monitorRunning 判定探活监控是否在跑（monitorStarted 由 rescueLock 保护，
// 测试读取必须持锁——直接读是 data race）。
func monitorRunning(m *redisMonitor) bool {
	m.rescueLock.Lock()
	defer m.rescueLock.Unlock()
	return m.monitorStarted
}

// TestPeriodLimit_Close_StopsProbeLoop 回归（#165）：Close 后探活 goroutine
// 必须退出——Redis 永久宕机且 limiter 被宿主丢弃时不再永久轮询。
// 断言口径：Close 后 monitorStarted 复位、探活调用计数冻结。
func TestPeriodLimit_Close_StopsProbeLoop(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewPeriodLimit(time.Minute, 10, flaky, "pc1",
		WithRescue(RescueFailOpen), WithRescuePingInterval(2*time.Millisecond))
	ctx := context.Background()

	flaky.setFail(true)
	if _, err := l.Take(ctx, "k"); err != nil {
		t.Fatalf("rescued take: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !monitorRunning(l.redisMonitor) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !monitorRunning(l.redisMonitor) {
		t.Fatal("expected probe monitor to be running after first failure")
	}

	if err := l.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// 幂等：重复 Close 不 panic。
	if err := l.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for monitorRunning(l.redisMonitor) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if monitorRunning(l.redisMonitor) {
		t.Fatal("probe monitor must exit after Close (#165)")
	}

	// 探活调用冻结：观察窗口内不再对（宕机的）Redis 拨号。
	probesBefore := flaky.calls() - flaky.bizCalls()
	time.Sleep(60 * time.Millisecond)
	if probesAfter := flaky.calls() - flaky.bizCalls(); probesAfter != probesBefore {
		t.Fatalf("probe loop still dialing dead redis after Close: %d -> %d probes (#165)", probesBefore, probesAfter)
	}
}

// TestPeriodLimit_Close_NoMonitorRestart 回归（#165）：Close 后再次故障不再
// 拉起新监控（否则停机口形同虚设），降级口径照常给出。
func TestPeriodLimit_Close_NoMonitorRestart(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewPeriodLimit(time.Minute, 10, flaky, "pc2", WithRescue(RescueFailOpen))
	ctx := context.Background()

	if err := l.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	flaky.setFail(true)
	code, err := l.Take(ctx, "k")
	if err != nil || code != Allowed {
		t.Fatalf("take after close = (%d, %v), want (Allowed, nil) (rescue answer unchanged)", code, err)
	}
	time.Sleep(30 * time.Millisecond)
	if monitorRunning(l.redisMonitor) {
		t.Fatal("startMonitor must be a no-op after Close (#165)")
	}
}

// TestTokenLimiter_Close_StopsProbeLoop 回归（#165）：TokenLimiter 同一停机
// 语义——Close 后探活循环退出，故障时本地兜底口径照常。
func TestTokenLimiter_Close_StopsProbeLoop(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewTokenLimiter(100, 100, flaky, "pc3",
		WithTokenFallback(), WithTokenPingInterval(2*time.Millisecond))
	ctx := context.Background()

	flaky.setFail(true)
	if !l.Allow(ctx) {
		t.Fatal("fallback allow must return true")
	}
	deadline := time.Now().Add(2 * time.Second)
	for !monitorRunning(l.redisMonitor) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !monitorRunning(l.redisMonitor) {
		t.Fatal("expected probe monitor to be running after first failure")
	}

	if err := l.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for monitorRunning(l.redisMonitor) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if monitorRunning(l.redisMonitor) {
		t.Fatal("probe monitor must exit after Close (#165)")
	}
	probesBefore := flaky.calls() - flaky.bizCalls()
	time.Sleep(60 * time.Millisecond)
	if probesAfter := flaky.calls() - flaky.bizCalls(); probesAfter != probesBefore {
		t.Fatalf("probe loop still dialing dead redis after Close: %d -> %d probes (#165)", probesBefore, probesAfter)
	}

	// 降级口径照常；startMonitor 停机后是 no-op。
	if !l.Allow(ctx) {
		t.Fatal("fallback allow must keep working after Close")
	}
	if monitorRunning(l.redisMonitor) {
		t.Fatal("startMonitor must be a no-op after Close (#165)")
	}
}

// --- redisx#166：降级桶工厂 per-key singleflight ---

// TestPeriodLimit_RescueBoundedLocal_FactoryConcurrentOnce 回归（#166）：并发
// 首次降级时工厂必须只被调用一次——历史实现 Load miss 与 LoadOrStore 之间无
// 同步，落选者的桶实例被丢弃、自定义工厂被重复调用（副作用/计数语义漂移）。
func TestPeriodLimit_RescueBoundedLocal_FactoryConcurrentOnce(t *testing.T) {
	store, _ := newTestStoreFastFail(t)
	flaky := newFlakyScripter(store)
	var calls atomic.Int64
	l := NewPeriodLimit(time.Minute, 100, flaky, "pc4",
		WithRescue(RescueBoundedLocal),
		WithRescueBucket(func(string) LocalLimiter {
			calls.Add(1)
			return newLocalBucket(1000, 1000)
		}))
	ctx := context.Background()

	flaky.setFail(true)
	const n = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			code, err := l.Take(ctx, "hot")
			if err != nil || code != Allowed {
				t.Errorf("concurrent take %d = (%d, %v), want (Allowed, nil)", i, code, err)
			}
			results[i] = code
		}(i)
	}
	close(start)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("factory called %d times under concurrent first degradation, want exactly 1 (#166)", got)
	}
	for i, code := range results {
		if code != Allowed {
			t.Fatalf("take %d = %d, want Allowed", i, code)
		}
	}
}

// TestResilientLimiter_FactoryConcurrentOnce 回归（#166，resilient 侧同形态）：
// 并发首次降级 shared 工厂只调一次。
func TestResilientLimiter_FactoryConcurrentOnce(t *testing.T) {
	store, mr := newTestStoreFastFail(t)
	var calls atomic.Int64
	l := NewResilientLimiter(NewPeriodLimit(time.Minute, 100, store, "pc5"),
		func(string) LocalLimiter {
			calls.Add(1)
			return newLocalBucket(1000, 1000)
		})
	ctx := context.Background()
	mr.Close() // miniredis 故障

	const n = 32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			d, _ := l.Allow(ctx, "hot") // err：底层 redis 故障按契约随 Decision 上抛
			if d.Source != SourceLocalFallback {
				t.Errorf("source = %v, want SourceLocalFallback", d.Source)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("factory called %d times under concurrent first degradation, want exactly 1 (#166)", got)
	}
}

// --- redisx#167：降级桶缓存回切清理 ---

// TestPeriodLimit_RescueBoundedLocal_BucketsClearedOnRecover 回归（#167）：
// 探活回切成功时降级桶缓存必须清空——高基数 key（per-user/per-token）在
// 一次长故障窗口后不再沉淀全量条目常驻内存；新故障周期重建桶（工厂再调）。
func TestPeriodLimit_RescueBoundedLocal_BucketsClearedOnRecover(t *testing.T) {
	store, _ := newTestStoreFastFail(t)
	flaky := newFlakyScripter(store)
	var calls atomic.Int64
	l := NewPeriodLimit(time.Minute, 100, flaky, "pc6",
		WithRescue(RescueBoundedLocal),
		WithRescueBucket(func(key string) LocalLimiter {
			calls.Add(1)
			return newLocalBucket(1000, 1000)
		}))
	ctx := context.Background()

	flaky.setFail(true)
	if code, err := l.Take(ctx, "user:1"); err != nil || code != Allowed {
		t.Fatalf("degraded take = (%d, %v), want (Allowed, nil)", code, err)
	}
	if _, ok := l.buckets.Load("user:1"); !ok {
		t.Fatal("expected bucket to be cached during outage")
	}

	// 恢复：探活成功自动回切，回切钩子清空桶缓存。
	flaky.setFail(false)
	deadline := time.Now().Add(2 * time.Second)
	for !l.redisAlive.Load() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !l.redisAlive.Load() {
		t.Fatal("expected redisAlive=true after recovery probe")
	}
	if n := countBuckets(&l.buckets); n != 0 {
		t.Fatalf("bucket cache must be cleared on recovery (#167), %d entries remain", n)
	}

	// 新故障周期：桶重建（工厂再调一次）。
	flaky.setFail(true)
	if code, err := l.Take(ctx, "user:1"); err != nil || code != Allowed {
		t.Fatalf("degraded take after new outage = (%d, %v), want (Allowed, nil)", code, err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("factory calls = %d, want 2 (one per degradation episode)", got)
	}
}

// TestResilientLimiter_LocalCacheClearedAfterRedisRecovery 回归（#167，
// resilient 侧同形态）：Redis 恢复后的首次判定清空本地兜底桶缓存；常态
// （无降级发生）不清扫、零开销。
func TestResilientLimiter_LocalCacheClearedAfterRedisRecovery(t *testing.T) {
	store, mr := newTestStoreFastFail(t)
	l := NewResilientLimiter(NewPeriodLimit(time.Minute, 100, store, "pc7"),
		func(string) LocalLimiter { return newLocalBucket(1000, 1000) })
	ctx := context.Background()

	// 常态：多次 Redis 判定不触发清扫（fallbackSeen 恒 false）。
	for i := 0; i < 3; i++ {
		if _, err := l.Allow(ctx, "k"); err != nil {
			t.Fatalf("allow %d: %v", i+1, err)
		}
	}
	if l.fallbackSeen.Load() {
		t.Fatal("fallbackSeen must stay false on healthy redis path")
	}

	mr.Close()
	// err：底层 redis 故障按契约随 Decision 上抛，判定口径看 Source。
	if d, _ := l.Allow(ctx, "user:1"); d.Source != SourceLocalFallback {
		t.Fatalf("degraded allow = %+v, want SourceLocalFallback", d)
	}
	if n := countBuckets(&l.localCache); n != 1 {
		t.Fatalf("expected 1 cached bucket during outage, got %d", n)
	}

	mr.Restart()
	d, err := l.Allow(ctx, "k")
	if err != nil || !d.Allowed || d.Source != SourceRedis {
		t.Fatalf("recovered allow = (%+v, %v), want {true SourceRedis}", d, err)
	}
	if n := countBuckets(&l.localCache); n != 0 {
		t.Fatalf("local bucket cache must be cleared after recovery (#167), %d entries remain", n)
	}
	if l.fallbackSeen.Load() {
		t.Fatal("fallbackSeen must be consumed by the recovery sweep")
	}

	// Close 幂等且清理彻底。
	if err := l.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := l.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// countBuckets 统计降级桶缓存条目数（测试断言用）。
func countBuckets(m *sync.Map) int {
	n := 0
	m.Range(func(any, any) bool {
		n++
		return true
	})
	return n
}

// --- redisx#168：构造期 store 非 nil 校验 ---

// TestNewPeriodLimit_NilStore_Panics 回归（#168）：store 为 nil 构造期
// fail-fast panic——历史实现不拦，故障延迟到首次 Take 的 nil interface
// panic 且堆栈落在 go-redis 内部帧。
func TestNewPeriodLimit_NilStore_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewPeriodLimit with nil store must panic at construction (#168)")
		}
	}()
	NewPeriodLimit(time.Minute, 10, nil, "pc8")
}

// TestNewTokenLimiter_NilStore_Panics 回归（#168）：同上（TokenLimiter 侧）。
func TestNewTokenLimiter_NilStore_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewTokenLimiter with nil store must panic at construction (#168)")
		}
	}()
	NewTokenLimiter(1, 1, nil, "pc9")
}
