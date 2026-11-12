package distlock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestLocker 启动 miniredis 并返回带真实 redis 客户端的 RedisLocker.
func newTestLocker(t *testing.T) (*RedisLocker, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return NewRedisLocker(c), mr
}

func TestTryLock_Success(t *testing.T) {
	l, mr := newTestLocker(t)
	ctx := context.Background()

	ownerID, err := l.TryLock(ctx, "k1", time.Minute)
	if err != nil {
		t.Fatalf("expected success, got err=%v", err)
	}
	if ownerID == "" {
		t.Error("expected non-empty ownerID")
	}
	if !mr.Exists("lock:k1") {
		t.Error("expected lock:k1 to exist in miniredis")
	}
}

func TestTryLock_NotAcquired(t *testing.T) {
	l, mr := newTestLocker(t)
	ctx := context.Background()

	// 预占 key
	if err := mr.Set("lock:k1", "other-owner"); err != nil {
		t.Fatal(err)
	}

	_, err := l.TryLock(ctx, "k1", time.Minute)
	if !errors.Is(err, ErrLockNotAcquired) {
		t.Errorf("expected ErrLockNotAcquired, got %v", err)
	}
}

func TestTryLock_OnLockCallback(t *testing.T) {
	l, _ := newTestLocker(t)

	var gotAcquired bool
	var gotKey string
	l.onLock = func(key string, acquired bool, err error) {
		gotKey = key
		gotAcquired = acquired
	}

	_, err := l.TryLock(context.Background(), "k1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !gotAcquired || gotKey != "k1" {
		t.Errorf("expected onLock(k1, true, nil), got key=%q acquired=%v", gotKey, gotAcquired)
	}
}

func TestTryLock_OnLockCallback_NotAcquired(t *testing.T) {
	l, mr := newTestLocker(t)
	if err := mr.Set("lock:k1", "other"); err != nil {
		t.Fatal(err)
	}

	var gotAcquired bool
	l.onLock = func(key string, acquired bool, err error) {
		gotAcquired = acquired
	}

	_, _ = l.TryLock(context.Background(), "k1", time.Minute)
	if gotAcquired {
		t.Error("expected onLock called with acquired=false")
	}
}

func TestUnlock_Success(t *testing.T) {
	l, mr := newTestLocker(t)
	ctx := context.Background()

	ownerID, err := l.TryLock(ctx, "k1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !mr.Exists("lock:k1") {
		t.Fatal("expected lock to exist")
	}

	if err := l.Unlock(ctx, "k1", ownerID); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if mr.Exists("lock:k1") {
		t.Error("expected lock to be deleted after Unlock")
	}
}

func TestUnlock_WrongOwner(t *testing.T) {
	l, mr := newTestLocker(t)
	ctx := context.Background()

	_, err := l.TryLock(ctx, "k1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// 用错 ownerID 解锁：Lua 脚本比较 ARGV[1] != current value，
	// 不调用 DEL，返 0 ⇒ 显式 ErrLockNotHeld（#115，修复前脚本返回值被
	// 丢弃、err==nil 的「假成功」）。key 仍存在。
	err = l.Unlock(ctx, "k1", "wrong-owner")
	if !errors.Is(err, ErrLockNotHeld) {
		t.Errorf("expected ErrLockNotHeld, got %v", err)
	}
	if !mr.Exists("lock:k1") {
		t.Error("expected lock:k1 to still exist after wrong-owner Unlock")
	}
}

// TestUnlock_NotHeldKeepsNewHolderLock 是 #115 的核心回归：锁 TTL 过期易主
// 后，陈旧持有者的 Unlock 必须得到 ErrLockNotHeld（修复前：脚本返回 0 被
// 丢弃 ⇒ err==nil「假成功」，调用方在不知情下继续假装持有，与新持有者并发），
// 且新持有者的锁完好、仍能正常解锁。
func TestUnlock_NotHeldKeepsNewHolderLock(t *testing.T) {
	l, mr := newTestLocker(t)
	mr.ClockTTL(true) // TTL 随真实时钟递减（短 TTL 过期场景必需）
	ctx := context.Background()

	staleOwner, err := l.TryLock(ctx, "k1", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// 等 TTL 过期（watchdog 未启用，无续期），新持有者获锁。
	time.Sleep(120 * time.Millisecond)
	newOwner, err := l.TryLock(ctx, "k1", time.Minute)
	if err != nil {
		t.Fatalf("new owner failed to acquire after TTL expiry: %v", err)
	}
	if newOwner == staleOwner {
		t.Fatal("new ownerID must differ from stale one")
	}

	// 陈旧持有者解锁：显式 ErrLockNotHeld，且新持有者的锁不受影响。
	if uerr := l.Unlock(ctx, "k1", staleOwner); !errors.Is(uerr, ErrLockNotHeld) {
		t.Fatalf("stale holder unlock must return ErrLockNotHeld, got %v", uerr)
	}
	if !mr.Exists("lock:k1") {
		t.Fatal("new holder's lock must survive stale unlock")
	}
	got, err := mr.Get("lock:k1")
	if err != nil {
		t.Fatalf("miniredis get: %v", err)
	}
	if got != newOwner {
		t.Fatalf("lock value changed after stale unlock: got %q, want %q", got, newOwner)
	}
	if err := l.Unlock(ctx, "k1", newOwner); err != nil {
		t.Fatalf("new owner unlock failed: %v", err)
	}
	if mr.Exists("lock:k1") {
		t.Error("lock should be deleted by new owner unlock")
	}
}

// TestUnlock_CommErrorKeepsAutoRenew 是 #115 第二半的回归：Unlock 的删锁
// 请求通信故障时，autoRenew watchdog 必须保持运行、续期状态原样保留——
// 修复前 watchdog 在网络请求之前就被杀掉，删锁失败 ⇒ 续期已停而锁还挂着
// ⇒ 锁在 TTL 后静默失效，调用方却以为「还没解开所以还持有」。
func TestUnlock_CommErrorKeepsAutoRenew(t *testing.T) {
	_, mr := newTestLocker(t)
	// 显式续期间隔远大于测试窗口 ⇒ 断言期间不发生续期写动作，只看状态。
	rl := NewRedisLocker(func() redis.UniversalClient {
		c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = c.Close() })
		return c
	}(), WithAutoRenew(), WithRenewInterval(time.Hour))

	ctx := context.Background()
	ownerID, err := rl.TryLock(ctx, "k1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// 杀掉 miniredis ⇒ 删锁请求通信故障。
	mr.Close()
	err = rl.Unlock(ctx, "k1", ownerID)
	if err == nil || errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected comm error (not ErrLockNotHeld/nil), got %v", err)
	}

	// watchdog 状态必须原样保留（未停、未删）。
	v, ok := rl.locks.Load("k1")
	if !ok {
		t.Fatal("renew state was dropped on comm failure (#115): lock will silently expire after TTL")
	}
	st := v.(*lockState)
	select {
	case <-st.stopChan:
		t.Fatal("watchdog was stopped before/at failed unlock (#115): renewal stopped while lock may still be held")
	default:
	}

	// 清理：显式停掉 watchdog（本测试的续期间隔 1h，miniredis 已关，
	// 不清理会泄漏 goroutine 并污染后续 goroutine 计数类用例的基线）。
	rl.stopRenewState("k1", ownerID)
	waitForNoAutoRenewGoroutines(t)
}

// waitForNoAutoRenewGoroutines 等待 startAutoRenew 派生的 goroutine 全部退出。
func waitForNoAutoRenewGoroutines(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if countAutoRenewGoroutines() == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestUnlock_OnUnlockCallback_ReportsNotHeld onUnlock 回调的错误语义与
// Unlock 返回值一致（#115）：非本人解锁回调收到 ErrLockNotHeld。
func TestUnlock_OnUnlockCallback_ReportsNotHeld(t *testing.T) {
	l, _ := newTestLocker(t)
	ctx := context.Background()

	var gotErr error
	l.onUnlock = func(key string, err error) {
		gotErr = err
	}

	_, _ = l.TryLock(ctx, "k1", time.Minute)
	if err := l.Unlock(ctx, "k1", "wrong-owner"); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("unlock: %v", err)
	}
	if !errors.Is(gotErr, ErrLockNotHeld) {
		t.Errorf("onUnlock must receive ErrLockNotHeld, got %v", gotErr)
	}
}

func TestUnlock_OnUnlockCallback(t *testing.T) {
	l, _ := newTestLocker(t)
	ctx := context.Background()

	var gotErr error
	l.onUnlock = func(key string, err error) {
		gotErr = err
	}

	ownerID, _ := l.TryLock(ctx, "k1", time.Minute)
	if err := l.Unlock(ctx, "k1", ownerID); err != nil {
		t.Fatal(err)
	}
	if gotErr != nil {
		t.Errorf("expected onUnlock with nil err, got %v", gotErr)
	}
}

func TestLock_Success_AfterContention(t *testing.T) {
	l, mr := newTestLocker(t)
	ctx := context.Background()

	// 第一次: 占用
	firstOwner, err := l.TryLock(ctx, "k1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// 第二次: 在另一个 goroutine 解锁后, 我们的 Lock 应当能拿到
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = l.Unlock(context.Background(), "k1", firstOwner)
	}()

	ownerID, err := l.Lock(ctx, "k1", time.Minute, 2*time.Second)
	if err != nil {
		t.Fatalf("expected Lock to succeed after contention, got %v", err)
	}
	if ownerID == firstOwner {
		t.Error("expected new ownerID, got the released one")
	}
	_ = mr
}

func TestLock_Timeout(t *testing.T) {
	l, mr := newTestLocker(t)
	if err := mr.Set("lock:k1", "other"); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	_, err := l.Lock(context.Background(), "k1", time.Minute, 200*time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("expected ErrLockTimeout, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("Lock took %v, expected < 500ms", elapsed)
	}
}

func TestLock_ContextCancel(t *testing.T) {
	l, mr := newTestLocker(t)
	if err := mr.Set("lock:k1", "other"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := l.Lock(ctx, "k1", time.Minute, 5*time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestLock_WithAutoRenew(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer c.Close()
	l := NewRedisLocker(c,
		WithAutoRenew(),
		WithRenewInterval(50*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ownerID, err := l.Lock(ctx, "auto-renew-key", 200*time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// 验证 lockState 已注册
	val, ok := l.locks.Load("auto-renew-key")
	if !ok {
		t.Fatal("expected lockState to be registered")
	}
	st := val.(*lockState)

	// Unlock 应当停止 auto-renew
	if err := l.Unlock(ctx, "auto-renew-key", ownerID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-st.stopChan:
		// expected
	case <-time.After(time.Second):
		t.Error("expected stopChan closed within 1s after Unlock")
	}
}

func TestTryLock_Prefix(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer c.Close()
	l := NewRedisLocker(c, WithPrefix("fuxi"))

	_, err := l.TryLock(context.Background(), "k1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// lockKey() returns "<prefix>:<key>" so prefix="fuxi" → "fuxi:k1"
	if !mr.Exists("fuxi:k1") {
		t.Errorf("expected fuxi:k1, got keys: %v", mr.Keys())
	}
}

func TestUnlock_StopsAutoRenew(t *testing.T) {
	// 验证 Lock 之前先 unlock 一个 key (虽然不存在 lockState), 应当安全 no-op
	l, _ := newTestLocker(t)
	if err := l.Unlock(context.Background(), "never-locked", "x"); err != nil {
		// 实际会调用 Lua 失败 (key 不存在, owner 不匹配), 但不会 panic
		t.Logf("expected error from Lua, got %v (acceptable)", err)
	}
}

func TestTryLock_RedisError(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer c.Close()
	l := NewRedisLocker(c, WithRetryInterval(10*time.Millisecond))

	mr.Close() // 关闭 miniredis, 让 SetNX 失败

	_, err := l.TryLock(context.Background(), "k1", time.Second)
	if err == nil {
		t.Error("expected error from closed miniredis")
	}
}

func TestWithLock_Fail(t *testing.T) {
	l, mr := newTestLocker(t)
	if err := mr.Set("lock:k1", "other"); err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("user fn")
	err := WithLock(context.Background(), l, "k1", time.Minute, 50*time.Millisecond, func() error {
		return sentinel
	})
	if !errors.Is(err, ErrLockTimeout) {
		t.Errorf("expected ErrLockTimeout wrapping, got %v", err)
	}
}

// TestLock_ConcurrentContention 验证多个 goroutine 同时竞争同一把锁时
// 互斥性严格成立：同一时刻只有一个持有者，且最终所有 goroutine 都能完成。
func TestLock_ConcurrentContention(t *testing.T) {
	l, _ := newTestLocker(t)
	ctx := context.Background()

	const goroutines = 10
	var (
		wg           sync.WaitGroup
		holders      atomic.Int32 // 当前持有者数，应始终 <= 1
		maxHolders   atomic.Int32 // 历史最大并发持有者数
		successCount atomic.Int32
	)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ownerID, err := l.Lock(ctx, "contest-key", time.Minute, 3*time.Second)
			if err != nil {
				// Lock 超时是可接受的（其他 goroutine 持有太久）
				return
			}
			// 进入临界区
			cur := holders.Add(1)
			// 更新最大持有者数
			for {
				old := maxHolders.Load()
				if cur <= old || maxHolders.CompareAndSwap(old, cur) {
					break
				}
			}
			if cur > 1 {
				t.Errorf("concurrent lock violation: %d holders at once", cur)
			}
			time.Sleep(10 * time.Millisecond) // 模拟临界区操作
			holders.Add(-1)
			successCount.Add(1)
			_ = l.Unlock(ctx, "contest-key", ownerID)
		}()
	}
	wg.Wait()

	if successCount.Load() == 0 {
		t.Error("no goroutine acquired the lock")
	}
	if maxHolders.Load() > 1 {
		t.Errorf("max concurrent holders was %d, expected 1", maxHolders.Load())
	}
}

// TestWithLock_UnlockWithCancelledCtx 回归：fn 执行期间 ctx 取消时，
// 用原 ctx 调 Unlock 会在发命令前就返回 context.Canceled，DEL 永不执行
// ——锁变孤儿，其他实例阻塞到 TTL 过期。钉住：ctx 取消后锁仍被释放。
func TestWithLock_UnlockWithCancelledCtx(t *testing.T) {
	l, mr := newTestLocker(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := WithLock(ctx, l, "orphan-k", time.Minute, time.Second, func() error {
		cancel() // 模拟 fn 执行期间超时/取消
		return nil
	})
	if err != nil {
		t.Fatalf("WithLock returned err: %v", err)
	}
	if mr.Exists("distlock:orphan-k") {
		t.Error("lock must be released even when ctx was cancelled inside fn (orphan lock)")
	}
}

// TestTryLock_ZeroTTL_Rejected 回归：go-redis SetNX 的 ttl=0 表示永不过期，
// 未初始化的零值 Duration 会产生打不破的锁。钉住：ttl<=0 显式报错。
func TestTryLock_ZeroTTL_Rejected(t *testing.T) {
	l, _ := newTestLocker(t)
	if _, err := l.TryLock(context.Background(), "k", 0); err == nil {
		t.Error("ttl=0 must be rejected (would create a never-expiring lock)")
	}
}

// TestTryLock_NilClientWithoutDegradation 回归：nil client 且未开优雅降级
// 时曾把故障推迟到第一次加锁的运行时 nil panic。钉住：构造组合非法即报错。
func TestTryLock_NilClientWithoutDegradation(t *testing.T) {
	l := NewRedisLocker(nil)
	if _, err := l.TryLock(context.Background(), "k", time.Minute); err == nil {
		t.Error("nil client without graceful degradation must fail fast, not panic")
	}
}

// TestUnlock_DoubleUnlock_SecondReportsNotHeld 是 #115 的补充回归钉
// （#115 已修复于 main，PR #117；本测试钉住 ErrLockNotHeld 文档里显式列出
// 的「对同一 (key, ownerID) 重复 Unlock」子情形）：第二次 Unlock 不得
// 假成功——脚本返回 0 必须转显式哨兵。
func TestUnlock_DoubleUnlock_SecondReportsNotHeld(t *testing.T) {
	l, _ := newTestLocker(t)
	ctx := context.Background()

	ownerID, err := l.TryLock(ctx, "k1", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Unlock(ctx, "k1", ownerID); err != nil {
		t.Fatalf("first unlock: %v", err)
	}
	if err := l.Unlock(ctx, "k1", ownerID); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("second unlock must report ErrLockNotHeld (#115), got %v", err)
	}
}

// TestLock_TimeoutIsStrictUpperBound 回归（redisx#178）：timeout 是严格上界，
// 到点后不再投递新的抢锁尝试——锁在超时之后才释放的场景必须返回
// ErrLockTimeout，不得「超时后抢到并返回成功」。实测形态：timeout=50ms、
// retryInterval=300ms、他方持锁并于 120ms 释放 ⇒ 旧实现（deadline 只在失败
// 之后检查）在 302ms 抢到并返回 nil，与 etcdx 侧 context.WithTimeout 的
// 封顶语义漂移；修复后 300ms 重试醒来即封口返回 ErrLockTimeout。
func TestLock_TimeoutIsStrictUpperBound(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	// 他方持锁（默认不开自动续期），120ms 后释放——timeout=50ms 早已过期 70ms。
	other := NewRedisLocker(c)
	ownerID, err := other.TryLock(ctx, "strict-deadline", time.Second)
	if err != nil {
		t.Fatalf("seed trylock: %v", err)
	}
	release := time.AfterFunc(120*time.Millisecond, func() {
		_ = other.Unlock(ctx, "strict-deadline", ownerID)
	})
	defer release.Stop()

	l := NewRedisLocker(c, WithRetryInterval(300*time.Millisecond))
	start := time.Now()
	got, err := l.Lock(ctx, "strict-deadline", time.Second, 50*time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("Lock(timeout=50ms) with late release at 120ms = (%q, %v), want ErrLockTimeout (redisx#178: 超时后不得成功)", got, err)
	}
	if got != "" {
		t.Fatalf("timeout path must return empty ownerID, got %q", got)
	}
	// 醒来封口发生在首个重试间隔（300ms）后：既不是超时前抢先成功，也不是
	// 越界后的成功路径。
	if elapsed < 250*time.Millisecond {
		t.Fatalf("Lock returned at %v, want >= one retry interval (~300ms)", elapsed)
	}
}

// TestLock_TimeoutStillAllowsFirstAttempt 钉住 #178 裁决的对齐面：timeout=0
// 仍保留一次非阻塞尝试——锁空闲时 Lock(timeout=0) 成功，与 etcdx 侧注释
// 声明的对齐口径一致；锁被占且 deadline 已过则首轮失败后立即 ErrLockTimeout。
func TestLock_TimeoutStillAllowsFirstAttempt(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	l := NewRedisLocker(c)
	ownerID, err := l.Lock(ctx, "zero-timeout-free", time.Minute, 0)
	if err != nil {
		t.Fatalf("Lock(timeout=0) on free lock must make one attempt and succeed, got %v", err)
	}
	if ownerID == "" {
		t.Fatal("expected non-empty ownerID")
	}

	// 锁被占：timeout=0 首轮失败即超时返回，不等待重试间隔。
	other := NewRedisLocker(c)
	held, err := other.TryLock(ctx, "zero-timeout-held", time.Minute)
	if err != nil {
		t.Fatalf("held trylock: %v", err)
	}
	start := time.Now()
	if _, err := l.Lock(ctx, "zero-timeout-held", time.Minute, 0); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("Lock(timeout=0) on held lock = %v, want ErrLockTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("timeout=0 must fail fast after the single attempt, took %v", elapsed)
	}
	_ = held
}
