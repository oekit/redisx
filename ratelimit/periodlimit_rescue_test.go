package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestPeriodLimit_RescueOffDefault 默认 RescueOff：Redis 故障时 Take 原样
// 返回 (Unknown, err)，不触发探活降级——历史行为回归钉（放行口径由调用方
// 决定，fail-open/fail-closed 自理）。
func TestPeriodLimit_RescueOffDefault(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewPeriodLimit(time.Minute, 10, flaky, "pr0")
	ctx := context.Background()

	flaky.setFail(true)
	code, err := l.Take(ctx, "k")
	if err == nil {
		t.Fatal("expected underlying redis error to surface under RescueOff")
	}
	if code != Unknown {
		t.Fatalf("code = %d, want Unknown", code)
	}
	if !l.redisAlive.Load() {
		t.Fatal("rescue must stay off by default (monitor must not be engaged)")
	}
}

// TestPeriodLimit_RescueFailOpen 降级放行：首个 Redis 故障返回 (Allowed, nil)
// 并拉起探活；故障期间后续判定直接走降级快路径（业务路径不再触碰 Redis）；
// Redis 恢复探活成功后自动回切。
func TestPeriodLimit_RescueFailOpen(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewPeriodLimit(time.Minute, 10, flaky, "pr1",
		WithRescue(RescueFailOpen), WithRescuePingInterval(5*time.Millisecond))
	ctx := context.Background()

	// Redis 正常：精确计数路径（配额拒绝仍是 OverQuota）
	for i := 0; i < 3; i++ {
		code, err := l.Take(ctx, "k")
		if err != nil || code != Allowed {
			t.Fatalf("redis take %d = (%d, %v), want (Allowed, nil)", i+1, code, err)
		}
	}

	flaky.setFail(true)
	code, err := l.Take(ctx, "k")
	if err != nil {
		t.Fatalf("rescued take must not surface redis error: %v", err)
	}
	if code != Allowed {
		t.Fatalf("rescued take = %d, want Allowed (fail-open)", code)
	}
	callsAfterFirst := flaky.bizCalls()

	// 等探活把 redisAlive 翻为 false（startMonitor 同步置位 + goroutine 拉起，
	// 这里等的是首次业务判定之后的探活循环首探）。
	deadline := time.Now().Add(2 * time.Second)
	for l.redisAlive.Load() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if l.redisAlive.Load() {
		t.Fatal("expected redisAlive=false while redis is down")
	}

	// 后续判定走降级快路径，业务路径不再触碰 Redis。
	for i := 0; i < 5; i++ {
		code, err := l.Take(ctx, "k")
		if err != nil || code != Allowed {
			t.Fatalf("fast-path take %d = (%d, %v), want (Allowed, nil)", i+1, code, err)
		}
	}
	if got := flaky.bizCalls(); got != callsAfterFirst {
		t.Fatalf("expected no redis traffic on business path during outage, biz calls %d -> %d", callsAfterFirst, got)
	}

	// 恢复：探活成功自动回切，下一次判定重新经过 Redis（配额路径继续计数）。
	flaky.setFail(false)
	deadline = time.Now().Add(2 * time.Second)
	for !l.redisAlive.Load() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !l.redisAlive.Load() {
		t.Fatal("expected redisAlive=true after recovery probe")
	}
	if _, err := l.Take(ctx, "k"); err != nil {
		t.Fatalf("take after recovery: %v", err)
	}
}

// TestPeriodLimit_RescueBoundedLocal_DefaultBucket 缺省兜底桶由 quota/period
// 推导（速率 quota/period 每秒、容量 quota）：故障期间单 key 有界——quota=3
// 内放行、第 4 发拒绝，全部 (code, nil) 无 error；不是 fail-open 无限放行。
func TestPeriodLimit_RescueBoundedLocal_DefaultBucket(t *testing.T) {
	store, _ := newTestStoreFastFail(t)
	flaky := newFlakyScripter(store)
	l := NewPeriodLimit(time.Second, 3, flaky, "pr2", WithRescue(RescueBoundedLocal))
	ctx := context.Background()

	flaky.setFail(true)
	for i := 1; i <= 3; i++ {
		code, err := l.Take(ctx, "k")
		if err != nil {
			t.Fatalf("rescued take %d: %v", i, err)
		}
		if code != Allowed {
			t.Fatalf("rescued take %d = %d, want Allowed (within derived bucket burst)", i, code)
		}
	}
	code, err := l.Take(ctx, "k")
	if err != nil {
		t.Fatalf("rescued take 4: %v", err)
	}
	if code != OverQuota {
		t.Fatalf("rescued take 4 = %d, want OverQuota (bounded local, not fail-open)", code)
	}
}

// TestPeriodLimit_RescueBoundedLocal_FactoryOncePerKey 自定义兜底桶工厂：
// 每 key 只调一次、桶实例跨请求共享——这里工厂**故意每次返回新实例**，若
// 库层真按 key 缓存，第二发仍被同一只 burst=1 的桶拒绝；随用随建则新桶恒满、
// 第二发被误放行（#125 点名的 zeus facade 坑，与 #127 同族契约）。
func TestPeriodLimit_RescueBoundedLocal_FactoryOncePerKey(t *testing.T) {
	store, _ := newTestStoreFastFail(t)
	flaky := newFlakyScripter(store)
	var (
		mu    sync.Mutex
		calls = map[string]int{}
	)
	l := NewPeriodLimit(time.Minute, 100, flaky, "pr3",
		WithRescue(RescueBoundedLocal),
		WithRescueBucket(func(key string) LocalLimiter {
			mu.Lock()
			calls[key]++
			mu.Unlock()
			return newLocalBucket(1, 1) // 每 key 每秒 1 个、容量 1
		}))
	ctx := context.Background()

	flaky.setFail(true)
	d1, err := l.Take(ctx, "k1")
	if err != nil || d1 != Allowed {
		t.Fatalf("first rescued take = (%d, %v), want (Allowed, nil)", d1, err)
	}
	if d2, _ := l.Take(ctx, "k1"); d2 != OverQuota {
		t.Fatalf("second rescued take = %d, want OverQuota (bucket must be shared across requests, not rebuilt)", d2)
	}
	if d3, _ := l.Take(ctx, "k2"); d3 != Allowed {
		t.Fatalf("other key rescued take = %d, want Allowed (per-key bucket)", d3)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls["k1"] != 1 || calls["k2"] != 1 {
		t.Fatalf("factory must be called exactly once per key (#125), got %v", calls)
	}
}

// TestPeriodLimit_RescueBoundedLocal_FactoryNil_FailOpen 工厂返回 nil = 未提供
// 该 key 的桶：该 key 降级退化为 fail-open，且 nil 结果不缓存（每次降级会再调
// 工厂，开始提供桶后即按桶限速——可动态提供/收回语义）。
func TestPeriodLimit_RescueBoundedLocal_FactoryNil_FailOpen(t *testing.T) {
	store, _ := newTestStoreFastFail(t)
	flaky := newFlakyScripter(store)
	provide := false
	var mu sync.Mutex
	var factoryCalls int
	l := NewPeriodLimit(time.Minute, 100, flaky, "pr4",
		WithRescue(RescueBoundedLocal),
		WithRescueBucket(func(string) LocalLimiter {
			mu.Lock()
			defer mu.Unlock()
			factoryCalls++
			if !provide {
				return nil
			}
			return newLocalBucket(1, 1)
		}))
	ctx := context.Background()

	flaky.setFail(true)
	if code, err := l.Take(ctx, "k"); err != nil || code != Allowed {
		t.Fatalf("nil-bucket rescue = (%d, %v), want (Allowed, nil)", code, err)
	}

	// 开始提供桶：nil 未缓存，本次判定即按新桶走。
	mu.Lock()
	provide = true
	callsAfterNilPhase := factoryCalls
	mu.Unlock()
	if code, err := l.Take(ctx, "k"); err != nil || code != Allowed {
		t.Fatalf("after factory provides = (%d, %v), want (Allowed, nil)", code, err)
	}
	if code, _ := l.Take(ctx, "k"); code != OverQuota {
		t.Fatalf("next rescued take = %d, want OverQuota (provided bucket must rate-limit)", code)
	}

	mu.Lock()
	defer mu.Unlock()
	if factoryCalls == callsAfterNilPhase {
		t.Fatal("nil result must not be cached: factory should be re-asked on later fallbacks")
	}
}

// TestPeriodLimit_RescueCtxCancelNotAFailure 调用方 ctx 取消/超时不是 Redis
// 故障：不触发降级（原样上抛 Unknown + ctx err），limiter 不进降级态。
func TestPeriodLimit_RescueCtxCancelNotAFailure(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewPeriodLimit(time.Minute, 10, flaky, "pr5", WithRescue(RescueFailOpen))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code, err := l.Take(ctx, "k")
	if code != Unknown {
		t.Fatalf("code = %d, want Unknown", code)
	}
	if err == nil {
		t.Fatal("expected ctx error to surface")
	}
	if l.redisAlive.Load() == false {
		t.Fatal("canceled ctx must not engage rescue")
	}
	// 健康 Redis 上照常判定
	if code, err := l.Take(context.Background(), "k"); err != nil || code != Allowed {
		t.Fatalf("take after canceled ctx = (%d, %v), want (Allowed, nil)", code, err)
	}
}

// TestPeriodLimit_RescueUnknownResponse 应答无法识别视同故障：rescue 开启时
// 降级给出最终口径（fail-open → (Allowed, nil)），不再返回 (Unknown, ErrUnknownCode)。
func TestPeriodLimit_RescueUnknownResponse(t *testing.T) {
	l := NewPeriodLimit(time.Minute, 10, stubScripter{val: "not-an-int"}, "pr6",
		WithRescue(RescueFailOpen))
	code, err := l.Take(context.Background(), "k")
	if err != nil {
		t.Fatalf("rescued unknown-response take: %v", err)
	}
	if code != Allowed {
		t.Fatalf("code = %d, want Allowed (fail-open on unknown response)", code)
	}
	if l.redisAlive.Load() {
		t.Fatal("unknown response must engage rescue (redisAlive=false)")
	}
}

// TestPeriodLimit_RescueConcurrent 判定与降级切换并发：多 goroutine 在故障
// 切换期间持续 Take，返回值只允许 (Allowed|OverQuota, nil)（降级口径）或
// (Allowed|HitQuota|OverQuota, nil)（Redis 口径）——绝不出现 (Unknown, nil)
// 或 (非Unknown, err) 的矛盾组合，且 -race 下无数据竞争。
func TestPeriodLimit_RescueConcurrent(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewPeriodLimit(time.Minute, 1000, flaky, "pr7",
		WithRescue(RescueBoundedLocal), WithRescuePingInterval(2*time.Millisecond))
	ctx := context.Background()

	stop := make(chan struct{})
	var toggleWG sync.WaitGroup
	toggleWG.Add(1)
	go func() {
		defer toggleWG.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		fail := false
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				fail = !fail
				flaky.setFail(fail)
			}
		}
	}()

	var loadWG sync.WaitGroup
	for g := 0; g < 8; g++ {
		loadWG.Add(1)
		go func() {
			defer loadWG.Done()
			for i := 0; i < 50; i++ {
				code, err := l.Take(ctx, "k")
				switch {
				case err != nil && code != Unknown:
					t.Errorf("incoherent verdict (%d, %v)", code, err)
					return
				case err == nil && code == Unknown:
					t.Errorf("incoherent verdict (Unknown, nil) — rescue must always finalize")
					return
				}
			}
		}()
	}
	loadWG.Wait()
	close(stop)
	toggleWG.Wait()
}
