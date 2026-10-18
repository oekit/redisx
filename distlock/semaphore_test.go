package distlock

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestSemaphore 启动 miniredis 并返回信号量与可注入时钟的句柄。
func newTestSemaphore(t *testing.T, key string, max int, slotTTL time.Duration) (*Semaphore, *miniredis.Miniredis, *[]time.Time) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	s := NewSemaphore(c, key, max, slotTTL)
	// 注入时钟：测试自由推进，验证过期回收不真等 slotTTL。
	clock := &[]time.Time{time.Unix(1_000_000, 0)}
	s.now = func() time.Time { return (*clock)[0] }
	return s, mr, clock
}

func advance(clock *[]time.Time, d time.Duration) {
	(*clock)[0] = (*clock)[0].Add(d)
}

// TestSemaphore_Acquire_RespectsMax 验证容量上界严格成立：max 个之后拒绝。
func TestSemaphore_Acquire_RespectsMax(t *testing.T) {
	s, _, _ := newTestSemaphore(t, "sem:max", 2, time.Minute)
	ctx := context.Background()

	for _, id := range []string{"a", "b"} {
		ok, err := s.Acquire(ctx, id)
		if err != nil || !ok {
			t.Fatalf("acquire %q: ok=%v err=%v", id, ok, err)
		}
	}
	ok, err := s.Acquire(ctx, "c")
	if err != nil {
		t.Fatalf("acquire c: %v", err)
	}
	if ok {
		t.Error("expected slot-full rejection beyond max=2")
	}
}

// TestSemaphore_Acquire_IdempotentRefresh 同一 identifier 重复 Acquire
// 幂等刷新，不占第二个槽。
func TestSemaphore_Acquire_IdempotentRefresh(t *testing.T) {
	s, mr, clock := newTestSemaphore(t, "sem:idem", 2, time.Minute)
	ctx := context.Background()

	if ok, _ := s.Acquire(ctx, "a"); !ok {
		t.Fatal("acquire a")
	}
	advance(clock, 10*time.Second)
	if ok, _ := s.Acquire(ctx, "a"); !ok {
		t.Fatal("re-acquire a must be idempotent-refresh")
	}
	// b 仍能进：a 只占一个槽
	if ok, _ := s.Acquire(ctx, "b"); !ok {
		t.Error("expected b to acquire the remaining slot")
	}
	// c 拒绝：槽满
	if ok, _ := s.Acquire(ctx, "c"); ok {
		t.Error("expected slot-full rejection")
	}
	members, err := mr.ZMembers("sem:idem")
	if err != nil {
		t.Fatalf("ZMembers: %v", err)
	}
	if len(members) != 2 {
		t.Errorf("expected 2 members, got %d: %v", len(members), members)
	}
}

// TestSemaphore_CrashRecovery 崩溃自愈：持有者不再续期，槽过期后被
// Acquire 内部的原子清理回收，无需 watchdog。
func TestSemaphore_CrashRecovery(t *testing.T) {
	s, _, clock := newTestSemaphore(t, "sem:crash", 2, 100*time.Millisecond)
	ctx := context.Background()

	_, _ = s.Acquire(ctx, "a")
	_, _ = s.Acquire(ctx, "b")
	advance(clock, 150*time.Millisecond) // 越过 slotTTL，模拟持有者全部崩溃

	ok, err := s.Acquire(ctx, "c")
	if err != nil {
		t.Fatalf("acquire after crash: %v", err)
	}
	if !ok {
		t.Error("expired slots must be recycled by Acquire (crash self-healing)")
	}
}

// TestSemaphore_Renew_KeepsSlot 续期保槽：临近过期续期后，槽在新窗口内
// 仍然有效，新竞争者拿不到。
func TestSemaphore_Renew_KeepsSlot(t *testing.T) {
	s, _, clock := newTestSemaphore(t, "sem:renew", 1, 100*time.Millisecond)
	ctx := context.Background()

	_, _ = s.Acquire(ctx, "a")
	advance(clock, 90*time.Millisecond) // 剩 10ms
	if ok, err := s.Renew(ctx, "a"); err != nil || !ok {
		t.Fatalf("renew a: ok=%v err=%v", ok, err)
	}
	advance(clock, 50*time.Millisecond) // 越过原过期点，但在续期窗口内

	if ok, _ := s.Acquire(ctx, "b"); ok {
		t.Error("renewed slot must stay held past the original expiry")
	}
}

// TestSemaphore_Renew_RejectsLost 已过期/未持有的槽不得借 Renew 复活：
// 配额可能已被他人接管。
func TestSemaphore_Renew_RejectsLost(t *testing.T) {
	s, _, clock := newTestSemaphore(t, "sem:renewlost", 1, 100*time.Millisecond)
	ctx := context.Background()

	_, _ = s.Acquire(ctx, "a")
	advance(clock, 150*time.Millisecond) // a 的槽已过期

	ok, err := s.Renew(ctx, "a")
	if err != nil {
		t.Fatalf("renew must not error on lost slot: %v", err)
	}
	if ok {
		t.Error("expired slot must not be resurrected via Renew")
	}
	if ok, err := s.Renew(ctx, "never-acquired"); err != nil || ok {
		t.Errorf("unknown identifier must renew-false, got ok=%v err=%v", ok, err)
	}
}

// TestSemaphore_Release_FreesSlotAndIdempotent 释放立即让出槽位，
// 重复释放幂等不报错。
func TestSemaphore_Release_FreesSlotAndIdempotent(t *testing.T) {
	s, _, _ := newTestSemaphore(t, "sem:release", 1, time.Minute)
	ctx := context.Background()

	_, _ = s.Acquire(ctx, "a")
	if ok, _ := s.Acquire(ctx, "b"); ok {
		t.Fatal("expected slot-full before release")
	}
	if err := s.Release(ctx, "a"); err != nil {
		t.Fatalf("release a: %v", err)
	}
	if ok, err := s.Acquire(ctx, "b"); err != nil || !ok {
		t.Errorf("expected b to acquire after release, ok=%v err=%v", ok, err)
	}
	if err := s.Release(ctx, "a"); err != nil {
		t.Errorf("double release must be idempotent, got %v", err)
	}
}

// TestSemaphore_Cleanup_OnlyExpired Cleanup 只清「已过期超过 olderThan」
// 的槽，未过期槽与宽限期内刚过期的槽不受影响。
func TestSemaphore_Cleanup_OnlyExpired(t *testing.T) {
	s, _, clock := newTestSemaphore(t, "sem:cleanup", 2, 100*time.Millisecond)
	ctx := context.Background()

	_, _ = s.Acquire(ctx, "a")          // 过期点 t+100ms
	advance(clock, 50*time.Millisecond) // t+50ms
	_, _ = s.Acquire(ctx, "b")          // 过期点 t+150ms
	advance(clock, 60*time.Millisecond) // t+110ms：a 已过期，b 未过期

	n, err := s.Cleanup(ctx, 0)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if n != 1 {
		t.Errorf("expected exactly expired slot a cleaned, got %d", n)
	}
	// b 仍持有（Renew 返回 true 即未过期），收尾释放避免干扰下一段
	if ok, _ := s.Renew(ctx, "b"); !ok {
		t.Fatal("live slot b must survive Cleanup")
	}
	if err = s.Release(ctx, "b"); err != nil {
		t.Fatalf("release b: %v", err)
	}

	// 宽限：c 过期 15ms 时不清（olderThan=20ms），过期 45ms 后才清
	_, _ = s.Acquire(ctx, "c")                   // t+110ms，过期点 t+210ms
	advance(clock, 115*time.Millisecond)         // t+225ms：c 过期 15ms
	n, err = s.Cleanup(ctx, 20*time.Millisecond) // 阈值 t+205ms < score t+210ms
	if err != nil || n != 0 {
		t.Errorf("slot expired within grace must be kept, n=%d err=%v", n, err)
	}
	advance(clock, 30*time.Millisecond)          // t+255ms：c 过期 45ms
	n, err = s.Cleanup(ctx, 20*time.Millisecond) // 阈值 t+235ms > score t+210ms
	if err != nil || n != 1 {
		t.Errorf("slot expired beyond grace must be cleaned, n=%d err=%v", n, err)
	}
}

// TestSemaphore_ConcurrentUnderMax 并发压力：多个 goroutine 以各自唯一
// 身份同时竞争，同时持有者数不超过 max（Lua 原子 + 容量检查）。
func TestSemaphore_ConcurrentUnderMax(t *testing.T) {
	s, _, _ := newTestSemaphore(t, "sem:race", 3, time.Minute)
	ctx := context.Background()

	const goroutines = 12
	var (
		wg       sync.WaitGroup
		holders  atomic.Int32
		maxSeen  atomic.Int32
		granted  atomic.Int32
		overflow atomic.Bool
	)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("worker-%d", i) // 唯一身份：12 抢 3
			ok, err := s.Acquire(ctx, id)
			if err != nil || !ok {
				return // 槽满被拒属预期
			}
			granted.Add(1)
			cur := holders.Add(1)
			for {
				old := maxSeen.Load()
				if cur <= old || maxSeen.CompareAndSwap(old, cur) {
					break
				}
			}
			if cur > 3 {
				overflow.Store(true)
			}
			time.Sleep(5 * time.Millisecond)
			holders.Add(-1)
			_ = s.Release(ctx, id)
		}(g)
	}
	wg.Wait()

	if overflow.Load() {
		t.Errorf("max concurrent holders was %d, expected <= 3", maxSeen.Load())
	}
	if granted.Load() == 0 {
		t.Error("no goroutine acquired a slot")
	}
}

// TestSemaphore_ValidationFailFast 构造参数/运行条件非法时报错不 panic。
func TestSemaphore_ValidationFailFast(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	if _, err := NewSemaphore(nil, "k", 1, time.Minute).Acquire(ctx, "a"); err == nil {
		t.Error("nil client must fail fast (quota semantics, no degradation)")
	}
	if _, err := NewSemaphore(c, "k", 0, time.Minute).Acquire(ctx, "a"); err == nil {
		t.Error("max=0 must fail fast")
	}
	if _, err := NewSemaphore(c, "k", 1, 0).Acquire(ctx, "a"); err == nil {
		t.Error("slotTTL=0 must fail fast (score would be in the past)")
	}
	s := NewSemaphore(c, "k", 1, time.Minute)
	if _, err := s.Acquire(ctx, ""); err == nil {
		t.Error("empty identifier must fail fast")
	}
	if _, err := s.Renew(ctx, ""); err == nil {
		t.Error("empty identifier must fail fast on Renew")
	}
	if err := s.Release(ctx, ""); err == nil {
		t.Error("empty identifier must fail fast on Release")
	}
	if _, err := NewSemaphore(c, "k", 1, time.Minute).Cleanup(ctx, 0); err != nil {
		t.Errorf("valid Cleanup should pass validation, got %v", err)
	}
}
