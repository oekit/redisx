package distlock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// --- #257a：Unlock/Release 与调用方 ctx 取消状态解耦 ---
//
// 口径对齐 pkg/distlock.ReleaseUncancelable：调用方 ctx 已取消时仍必须把
// 解锁命令发出去。修复前 go-redis 在发包前返回 context.Canceled，DEL/ZREM
// 不执行、watchdog 不停——`defer cancel(); defer lock.Unlock(ctx)` 的常见
// 写法下锁孤儿到 TTL 过期（ttl=30s 时 watchdog 会续期到 10×ttl）。

// TestLocker_UnlockWithCanceledCtxReleases 回归 #257：已取消 ctx 调 Unlock
// 必须成功删锁（nil），且锁 key 真正从 Redis 消失。
func TestLocker_UnlockWithCanceledCtxReleases(t *testing.T) {
	l, mr := newTestLocker(t)
	ctx := context.Background()

	ownerID, err := l.TryLock(ctx, "canceled-unlock", time.Minute)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if !mr.Exists("lock:canceled-unlock") {
		t.Fatal("expected lock key to exist before Unlock")
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel() // 模拟 defer cancel() 先于 defer Unlock 执行

	if err := l.Unlock(canceled, "canceled-unlock", ownerID); err != nil {
		t.Fatalf("Unlock with canceled ctx must still release the lock (#257): %v", err)
	}
	if mr.Exists("lock:canceled-unlock") {
		t.Fatal("lock key must be deleted despite canceled caller context (#257)")
	}
}

// TestSemaphore_ReleaseWithCanceledCtxFreesSlot 回归 #257：已取消 ctx 调
// Release 必须成功释放槽位，后续 Acquire 能拿到配额。
func TestSemaphore_ReleaseWithCanceledCtxFreesSlot(t *testing.T) {
	s, _, _ := newTestSemaphore(t, "sem:canceled-release", 1, time.Minute)
	ctx := context.Background()

	if ok, err := s.Acquire(ctx, "a"); err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	if err := s.Release(canceled, "a"); err != nil {
		t.Fatalf("Release with canceled ctx must still free the slot (#257): %v", err)
	}
	if ok, err := s.Acquire(ctx, "b"); err != nil || !ok {
		t.Fatalf("slot must be free after Release with canceled ctx, ok=%v err=%v", ok, err)
	}
}

// hangScriptHook 让 EVAL/EVALSHA 永远等到 ctx 结束，模拟 Redis 半开连接/
// 慢盘：验证 Unlock 内部的 releaseTimeout 上界真实生效（不裸 WithoutCancel）。
type hangScriptHook struct{}

func (hangScriptHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (hangScriptHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "eval", "evalsha":
			<-ctx.Done()
			return ctx.Err()
		}
		return next(ctx, cmd)
	}
}

func (hangScriptHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestLocker_UnlockBoundedWhenBackendHangs 回归 #257：解锁动作必须有硬上界
// ——WithoutCancel 会同时剥掉 deadline，裸用它会让 Unlock 永久挂起。
func TestLocker_UnlockBoundedWhenBackendHangs(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	c.AddHook(hangScriptHook{})
	l := NewRedisLocker(c)

	ownerID, err := l.TryLock(context.Background(), "hang-unlock", time.Minute)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	old := releaseTimeout
	releaseTimeout = 50 * time.Millisecond
	t.Cleanup(func() { releaseTimeout = old })

	start := time.Now()
	err = l.Unlock(context.Background(), "hang-unlock", ownerID)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Unlock against a hanging backend must return an error after the bound")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Unlock bound must surface context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Unlock must be bounded by releaseTimeout, took %v", elapsed)
	}
	// 通信故障路径：watchdog 状态保留（本次未启用 autoRenew，无状态可查；
	// 这里只断言错误口径不误报 ErrLockNotHeld）。
	if errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("bounded timeout must be a communication error, not ErrLockNotHeld: %v", err)
	}
}

// TestWithOnLockLost_Set 选项注入的纯构造测试（不依赖 Redis）。
func TestWithOnLockLost_Set(t *testing.T) {
	called := false
	l := NewRedisLocker(nil, WithOnLockLost(func(key string) { called = true }))
	if l.onLockLost == nil {
		t.Fatal("expected onLockLost to be set")
	}
	l.onLockLost("k")
	if !called {
		t.Error("expected callback called")
	}
}

// TestWithSemaphoreOnLockLost_Set 选项注入的纯构造测试（不依赖 Redis）。
func TestWithSemaphoreOnLockLost_Set(t *testing.T) {
	var got string
	s := NewSemaphore(nil, "sem:opt", 1, time.Minute, WithSemaphoreOnLockLost(func(id string) { got = id }))
	if s.onSlotLost == nil {
		t.Fatal("expected onSlotLost to be set")
	}
	s.onSlotLost("a")
	if got != "a" {
		t.Errorf("expected identifier forwarded, got %q", got)
	}
}
