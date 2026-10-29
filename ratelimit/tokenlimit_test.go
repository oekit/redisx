package ratelimit

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// flakyScripter 可控故障的 Scripter 包装：fail=true 时所有脚本调用返回
// 非 NOSCRIPT 的通信错误（模拟 Redis 故障）；fail=false 且 delay>0 时先
// 睡 delay 再透传（模拟高 RT 但存活的 Redis）。记录脚本调用次数，用于
// 判定"这次 Allow 走的是 Redis 还是本地兜底"。
type flakyScripter struct {
	inner redis.Scripter

	mu        sync.Mutex
	fail      bool
	delay     time.Duration
	callCount int
	// bizCallCount 只数**业务路径**的脚本调用（排除后台探活）。
	//
	// 为什么要分开（2026-10-30 修）：探活与业务共用同一个 Scripter，
	// 而探活脚本**本来就会打 Redis**（设计如此：探活就是探 Redis 通不通）。
	// 「fallback 期间不再触碰 Redis」这条断言真正要管的是**业务路径**
	// 不再发 EVAL——但混在一个计数器里时，后台探活那一次 EVAL 会把计数
	// 顶上去 ⇒ 断言偶发红（实测 100 轮红 14 次，CI 上 2026-10-29
	// 录到 calls 1 -> 2）。
	bizCallCount int
}

func newFlakyScripter(inner redis.Scripter) *flakyScripter {
	return &flakyScripter{inner: inner}
}

func (f *flakyScripter) setFail(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = v
}

// setSlow 设置成功调用的模拟 RT（sleep 后再透传）；0 = 不延迟.
func (f *flakyScripter) setSlow(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delay = d
}

// calls 返回累计脚本调用次数.
func (f *flakyScripter) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount
}

// bizCalls 返回累计的**业务路径**脚本调用次数（排除后台探活）。
func (f *flakyScripter) bizCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bizCallCount
}

// isProbeCall 判定该次脚本调用是否来自**后台探活**。
//
// 判据是**是否带 key**：探活脚本不带 key（`probeScript.Run(ctx, store, nil)`），
// 业务路径带 2 个 key（tokenKey + timestampKey）。
//
// 为什么不用脚本文本判：go-redis 的 `Script.Run` 走 EVALSHA（miss 才回退 EVAL），
// EVALSHA 只拿到摘要、拿不到脚本文本 ⇒ 文本判据在 EVALSHA 路径上失效。
// key 数量在两条路径上都稳定 ⇒ 这是唯一可靠的来源判据。
func isProbeCall(keys []string) bool { return len(keys) == 0 }

// recordCall 记一次脚本调用并返回当前故障开关与模拟 RT.
func (f *flakyScripter) recordCall(keys []string) (fail bool, delay time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount++
	if !isProbeCall(keys) {
		f.bizCallCount++
	}
	return f.fail, f.delay
}

// boom 构造与 go-redis 应答形状一致的故障命令（非 NOSCRIPT、非 redis.Nil，
// Script.Run 不会误判回退路径）.
func boom() *redis.Cmd {
	return redis.NewCmdResult(nil, errors.New("flaky: redis is down"))
}

// 各 Eval 变体逐一透传（EvalSha 路径收到的是脚本摘要，必须走 inner.EvalSha，
// 不能混用 Eval，否则摘要会被当成脚本文本编译失败）.
func (f *flakyScripter) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	fail, delay := f.recordCall(keys)
	if fail {
		return boom()
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return f.inner.Eval(ctx, script, keys, args...)
}

func (f *flakyScripter) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	fail, delay := f.recordCall(keys)
	if fail {
		return boom()
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return f.inner.EvalSha(ctx, sha1, keys, args...)
}

func (f *flakyScripter) EvalRO(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	fail, delay := f.recordCall(keys)
	if fail {
		return boom()
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return f.inner.EvalRO(ctx, script, keys, args...)
}

func (f *flakyScripter) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	fail, delay := f.recordCall(keys)
	if fail {
		return boom()
	}
	if delay > 0 {
		time.Sleep(delay)
	}
	return f.inner.EvalShaRO(ctx, sha1, keys, args...)
}

func (f *flakyScripter) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	return f.inner.ScriptExists(ctx, hashes...)
}

func (f *flakyScripter) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return f.inner.ScriptLoad(ctx, script)
}

func TestTokenLimiter_BurstConsume(t *testing.T) {
	store, mr := newTestStore(t)
	l := NewTokenLimiter(2, 3, store, "b1")
	ctx := context.Background()

	// burst 内全部放行
	for i := 0; i < 3; i++ {
		if !l.Allow(ctx) {
			t.Fatalf("allow %d: expected true within burst", i+1)
		}
	}
	// 超出 burst 拒绝（Lua false → redis.Nil）
	if l.Allow(ctx) {
		t.Fatal("expected false beyond burst")
	}

	// 桶标识 → hash-tag 双 key
	if !mr.Exists("{b1}.tokens") || !mr.Exists("{b1}.ts") {
		t.Fatalf("expected bucket keys to exist, got keys %v", mr.Keys())
	}
}

func TestTokenLimiter_Refill_BackdatedTimestamp(t *testing.T) {
	store, mr := newTestStore(t)
	l := NewTokenLimiter(2, 2, store, "b2")
	ctx := context.Background()

	// 耗尽（两次 Allow 各消耗一个令牌；拆变量规避 staticcheck SA4000 的
	// "identical expressions" 误报——两次调用语义不同但有副作用）
	firstAllow := l.Allow(ctx)
	secondAllow := l.Allow(ctx)
	if !firstAllow || !secondAllow {
		t.Fatal("expected initial burst to be allowed")
	}
	if l.Allow(ctx) {
		t.Fatal("expected false when bucket drained")
	}

	// 不 sleep：直接回写 ts key 模拟"距上次回补已过 10s"（确定性时间回补）。
	// 速率 2/s × 10s = 20 ≥ burst，桶应回满。
	if err := mr.Set("{b2}.ts", strconv.FormatInt(time.Now().Add(-10*time.Second).Unix(), 10)); err != nil {
		t.Fatal(err)
	}
	if !l.Allow(ctx) {
		t.Fatal("expected true after refill")
	}
	if !l.Allow(ctx) {
		t.Fatal("expected burst to be full after long idle")
	}
	if l.Allow(ctx) {
		t.Fatal("expected false after refilled burst consumed")
	}
}

func TestTokenLimiter_AllowN(t *testing.T) {
	store, _ := newTestStore(t)
	l := NewTokenLimiter(1, 5, store, "b3")
	ctx := context.Background()

	if !l.AllowN(ctx, 3) {
		t.Fatal("expected AllowN(3) true")
	}
	if !l.AllowN(ctx, 2) {
		t.Fatal("expected AllowN(2) true (exactly burst)")
	}
	if l.AllowN(ctx, 1) {
		t.Fatal("expected AllowN(1) false when drained")
	}
	// n <= 0 恒放行且不触碰 Redis
	if !l.AllowN(ctx, 0) || !l.AllowN(ctx, -1) {
		t.Fatal("expected AllowN with n<=0 to be trivially true")
	}
}

func TestTokenLimiter_NoFallback_FailClosed(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewTokenLimiter(100, 10, flaky, "b4")
	ctx := context.Background()

	// 未开启兜底：Redis 故障一律拒绝
	flaky.setFail(true)
	if l.Allow(ctx) {
		t.Fatal("expected fail-closed without WithTokenFallback")
	}
}

func TestTokenLimiter_Fallback_Engage(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewTokenLimiter(1000, 10, flaky, "b5",
		WithTokenFallback(), WithTokenPingInterval(5*time.Millisecond))
	ctx := context.Background()

	flaky.setFail(true)

	// 首次：尝试 Redis（失败）→ 降级本地桶放行
	if !l.Allow(ctx) {
		t.Fatal("expected fallback to allow first request")
	}
	callsAfterFirst := flaky.bizCalls()
	if callsAfterFirst == 0 {
		t.Fatal("expected at least one redis script attempt")
	}

	// 后续：redisAlive 已翻转，判定直接走本地桶，不再触碰 Redis。
	//
	// ⚠️ 断言口径必须用**业务路径**计数（bizCalls）而非全部调用（calls）：
	// 后台探活（waitForRedis → probe）**设计上就会打 Redis**（探活就是
	// 探 Redis 通不通），它与业务共用同一个 Scripter ⇒ 混在一个计数器里时，
	// 探活那一次 EVAL 会把计数顶上去 ⇒ 断言偶发红（实测 100 轮红 14 次；
	// CI 上 2026-10-29 录到 calls 1 -> 2）。
	//
	// 注：2026-10-27 那次修复把断言改成"等 redisAlive 翻转"是**无效修复**——
	// redisAlive=false 是在 startMonitor 里**同步**置位的（不是 ticker 驱动），
	// 真正的竞态是「取基线」与「探活首次 EVAL」的先后次序。
	deadline := time.Now().Add(2 * time.Second)
	for l.redisAlive.Load() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if l.redisAlive.Load() {
		t.Fatal("expected redisAlive=false while redis is down")
	}
	for i := 0; i < 5; i++ {
		if !l.Allow(ctx) {
			t.Fatalf("allow %d: expected local fallback to allow", i+1)
		}
	}
	if got := flaky.bizCalls(); got != callsAfterFirst {
		t.Fatalf("expected no redis traffic in business path while in fallback, biz calls %d -> %d", callsAfterFirst, got)
	}
}

func TestTokenLimiter_Fallback_Recover(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewTokenLimiter(1000, 10, flaky, "b6",
		WithTokenFallback(), WithTokenPingInterval(5*time.Millisecond))
	ctx := context.Background()

	// 触发降级
	flaky.setFail(true)
	if !l.Allow(ctx) {
		t.Fatal("expected fallback allow")
	}

	// Redis 恢复：探活成功 → 自动回切
	flaky.setFail(false)
	deadline := time.Now().Add(2 * time.Second)
	for !l.redisAlive.Load() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if !l.redisAlive.Load() {
		t.Fatal("expected redisAlive=true after recovery probe")
	}

	// 回切后判定重新经过 Redis（脚本调用计数继续增长）
	callsBefore := flaky.calls()
	if !l.Allow(ctx) {
		t.Fatal("expected allow via redis after recovery")
	}
	if got := flaky.calls(); got <= callsBefore {
		t.Fatalf("expected redis round-trip after recovery, calls %d -> %d", callsBefore, got)
	}
}

func TestTokenLimiter_Fallback_Recover_SlowRedisAbovePingInterval(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewTokenLimiter(1000, 10, flaky, "b12",
		WithTokenFallback(),
		WithTokenPingInterval(20*time.Millisecond),  // 间隔 << 探测 RT
		WithTokenProbeTimeout(500*time.Millisecond), // 超时 > 探测 RT：单次慢探测须能成功
	)
	ctx := context.Background()

	// 触发降级
	flaky.setFail(true)
	if !l.Allow(ctx) {
		t.Fatal("expected fallback allow")
	}

	// Redis 恢复但 RT 80ms > ping 间隔 20ms：探活超时与间隔耦合的实现里
	// 每次探活都会超时误判 → 永久降级；解耦后必须正常回切
	flaky.setFail(false)
	flaky.setSlow(80 * time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	for !l.redisAlive.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !l.redisAlive.Load() {
		t.Fatal("expected recovery despite probe RT > ping interval")
	}

	// 回切后判定重新经过 Redis（脚本调用计数继续增长）
	callsBefore := flaky.calls()
	if !l.Allow(ctx) {
		t.Fatal("expected allow via redis after recovery")
	}
	if got := flaky.calls(); got <= callsBefore {
		t.Fatalf("expected redis round-trip after recovery, calls %d -> %d", callsBefore, got)
	}
}

func TestTokenLimiter_Fallback_MiniRedisClosed(t *testing.T) {
	store, mr := newTestStoreFastFail(t)
	l := NewTokenLimiter(1000, 10, store, "b7",
		WithTokenFallback(), WithTokenPingInterval(5*time.Millisecond))
	ctx := context.Background()

	// 真实故障路径：Close miniredis → 连接失败 → 走本地兜底仍然放行
	mr.Close()
	for i := 0; i < 3; i++ {
		if !l.Allow(ctx) {
			t.Fatalf("allow %d: expected local fallback allow after redis closed", i+1)
		}
	}
}

func TestTokenLimiter_CtxCanceled_NoFallbackTrigger(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewTokenLimiter(1000, 10, flaky, "b8",
		WithTokenFallback(), WithTokenPingInterval(5*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// 调用方 ctx 取消不是 Redis 故障：拒绝但不触发兜底
	if l.Allow(ctx) {
		t.Fatal("expected false on canceled ctx")
	}
	if !l.redisAlive.Load() {
		t.Fatal("canceled ctx must not engage fallback")
	}
	if !l.Allow(context.Background()) {
		t.Fatal("expected healthy redis allow after canceled ctx")
	}
}

func TestTokenLimiter_UnknownResponse_FailClosed(t *testing.T) {
	l := NewTokenLimiter(1000, 10, stubScripter{val: "not-an-int"}, "b9")
	if l.Allow(context.Background()) {
		t.Fatal("expected fail-closed on unknown response")
	}
}

func TestTokenLimiter_UnknownResponse_FallbackEngages(t *testing.T) {
	l := NewTokenLimiter(1000, 10, stubScripter{val: "not-an-int"}, "b10",
		WithTokenFallback(), WithTokenPingInterval(5*time.Millisecond))
	if !l.Allow(context.Background()) {
		t.Fatal("expected fallback allow on unknown response")
	}
	if l.redisAlive.Load() {
		t.Fatal("expected redisAlive=false after unknown response")
	}
}

func TestTokenLimiter_PanicOnInvalidParams(t *testing.T) {
	store, _ := newTestStore(t)

	for name, fn := range map[string]func(){
		"zero rate":     func() { NewTokenLimiter(0, 10, store, "x") },
		"negative rate": func() { NewTokenLimiter(-1, 10, store, "x") },
		"zero burst":    func() { NewTokenLimiter(10, 0, store, "x") },
		"empty prefix":  func() { NewTokenLimiter(10, 10, store, "") },
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

func TestTokenLimiter_Concurrent_AllowAndFallbackToggle(t *testing.T) {
	store, _ := newTestStore(t)
	flaky := newFlakyScripter(store)
	l := NewTokenLimiter(10000, 1000, flaky, "b11",
		WithTokenFallback(), WithTokenPingInterval(2*time.Millisecond))
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
	var allowed atomic.Int64
	for g := 0; g < 8; g++ {
		loadWG.Add(1)
		go func() {
			defer loadWG.Done()
			for i := 0; i < 50; i++ {
				if l.AllowN(ctx, 1) {
					allowed.Add(1)
				}
			}
		}()
	}
	loadWG.Wait()
	close(stop)
	toggleWG.Wait()

	if allowed.Load() == 0 {
		t.Fatal("expected at least one allowed request")
	}
}
