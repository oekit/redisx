package distlock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLocker_LockKey(t *testing.T) {
	l := NewRedisLocker(nil, WithPrefix("test"))
	if l.lockKey("mykey") != "test:mykey" {
		t.Errorf("expected 'test:mykey', got %q", l.lockKey("mykey"))
	}
}

func TestLocker_LockKey_DefaultPrefix(t *testing.T) {
	l := NewRedisLocker(nil)
	if l.lockKey("mykey") != "lock:mykey" {
		t.Errorf("expected 'lock:mykey', got %q", l.lockKey("mykey"))
	}
}

func TestWithGracefulDegradation_TryLock(t *testing.T) {
	l := NewRedisLocker(nil, WithGracefulDegradation())
	ownerID, err := l.TryLock(context.TODO(), "key", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ownerID != "no-redis" {
		t.Errorf("expected 'no-redis', got %q", ownerID)
	}
}

func TestWithGracefulDegradation_Unlock(t *testing.T) {
	l := NewRedisLocker(nil, WithGracefulDegradation())
	err := l.Unlock(context.TODO(), "key", "no-redis")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestWithGracefulDegradation_Lock(t *testing.T) {
	l := NewRedisLocker(nil, WithGracefulDegradation())
	ownerID, err := l.Lock(context.TODO(), "key", time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ownerID != "no-redis" {
		t.Errorf("expected 'no-redis', got %q", ownerID)
	}
}

func TestWithRetryInterval(t *testing.T) {
	l := NewRedisLocker(nil, WithRetryInterval(200*time.Millisecond))
	if l.retryInterval != 200*time.Millisecond {
		t.Errorf("expected 200ms, got %v", l.retryInterval)
	}
}

func TestWithOnLock_Set(t *testing.T) {
	var called bool
	l := NewRedisLocker(nil, WithOnLock(func(key string, acquired bool, err error) {
		called = true
	}))
	if l.onLock == nil {
		t.Error("expected onLock to be set")
	}
	// Trigger the callback
	l.onLock("test", true, nil)
	if !called {
		t.Error("expected callback called")
	}
}

func TestWithOnUnlock_Set(t *testing.T) {
	var called bool
	l := NewRedisLocker(nil, WithOnUnlock(func(key string, err error) {
		called = true
	}))
	if l.onUnlock == nil {
		t.Error("expected onUnlock to be set")
	}
	l.onUnlock("test", nil)
	if !called {
		t.Error("expected callback called")
	}
}

func TestErrLockNotAcquired(t *testing.T) {
	if ErrLockNotAcquired.Error() != "lock not acquired" {
		t.Errorf("unexpected message: %s", ErrLockNotAcquired.Error())
	}
}

func TestErrLockTimeout(t *testing.T) {
	if ErrLockTimeout.Error() != "lock acquire timeout" {
		t.Errorf("unexpected message: %s", ErrLockTimeout.Error())
	}
}

func TestWithAutoRenew(t *testing.T) {
	l := NewRedisLocker(nil, WithAutoRenew())
	if !l.autoRenew {
		t.Error("expected autoRenew to be true")
	}
}

func TestWithRenewInterval(t *testing.T) {
	l := NewRedisLocker(nil, WithRenewInterval(5*time.Second))
	if l.renewInterval != 5*time.Second {
		t.Errorf("expected 5s, got %v", l.renewInterval)
	}
}

func TestLocker_StopAutoRenew(t *testing.T) {
	l := NewRedisLocker(nil, WithAutoRenew(), WithGracefulDegradation())
	st := &lockState{ownerID: "owner", stopChan: make(chan struct{})}
	l.locks.Store("test-key", st)

	_ = l.Unlock(context.TODO(), "test-key", "owner")

	select {
	case <-st.stopChan:
		// expected — channel was closed
	default:
		t.Error("expected stopChan to be closed after Unlock")
	}
}

// TestLocker_StaleOwnerUnlockKeepsRenew 回归测试：锁超时易主后，陈旧持有者
// 的 Unlock 不得取消新持有者的续期（否则新锁静默失效，产生双持有）。
func TestLocker_StaleOwnerUnlockKeepsRenew(t *testing.T) {
	l := NewRedisLocker(nil, WithAutoRenew(), WithGracefulDegradation())
	newOwnerState := &lockState{ownerID: "owner-B", stopChan: make(chan struct{})}
	l.locks.Store("test-key", newOwnerState)

	// 陈旧持有者 A（已超时）尝试解锁
	_ = l.Unlock(context.TODO(), "test-key", "owner-A")

	// 新持有者 B 的续期状态必须完好，且条目未被误删
	if _, ok := l.locks.Load("test-key"); !ok {
		t.Fatal("expected new owner's lockState to remain stored")
	}
	select {
	case <-newOwnerState.stopChan:
		t.Error("stale owner's Unlock must not cancel current owner's renewal")
	default:
	}

	// 新持有者自己解锁正常清理
	_ = l.Unlock(context.TODO(), "test-key", "owner-B")
	select {
	case <-newOwnerState.stopChan:
	default:
		t.Error("expected stopChan to be closed after real owner's Unlock")
	}
}

func TestLocker_MultipleOptions(t *testing.T) {
	var lockCalled, unlockCalled bool

	l := NewRedisLocker(nil,
		WithPrefix("custom"),
		WithGracefulDegradation(),
		WithRetryInterval(200*time.Millisecond),
		WithAutoRenew(),
		WithRenewInterval(5*time.Second),
		WithOnLock(func(key string, acquired bool, err error) {
			lockCalled = true
		}),
		WithOnUnlock(func(key string, err error) {
			unlockCalled = true
		}),
	)

	if l.prefix != "custom" {
		t.Errorf("expected 'custom', got %q", l.prefix)
	}
	if !l.gracefulDegradation {
		t.Error("expected gracefulDegradation")
	}
	if l.retryInterval != 200*time.Millisecond {
		t.Errorf("expected 200ms, got %v", l.retryInterval)
	}
	if !l.autoRenew {
		t.Error("expected autoRenew")
	}
	if l.renewInterval != 5*time.Second {
		t.Errorf("expected 5s, got %v", l.renewInterval)
	}

	// 测试回调
	l.onLock("test", true, nil)
	if !lockCalled {
		t.Error("expected lockCalled")
	}
	l.onUnlock("test", nil)
	if !unlockCalled {
		t.Error("expected unlockCalled")
	}
}

func TestWithLock_Success(t *testing.T) {
	l := NewRedisLocker(nil, WithGracefulDegradation())
	called := false
	err := WithLock(context.TODO(), l, "test-key", time.Minute, time.Second, func() error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if !called {
		t.Error("expected fn to be called")
	}
}

func TestWithLock_FnReturnsError(t *testing.T) {
	l := NewRedisLocker(nil, WithGracefulDegradation())
	sentinel := errors.New("fn error")
	err := WithLock(context.TODO(), l, "test-key", time.Minute, time.Second, func() error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
}

func TestStartAutoRenew_StartAndStop(t *testing.T) {
	// 在 gracefulDegradation 模式下，Lock 永远成功，触发 startAutoRenew。
	// Unlock 应当 stop auto-renew goroutine 并 close stopChan。
	l := NewRedisLocker(nil,
		WithGracefulDegradation(),
		WithAutoRenew(),
		WithRenewInterval(50*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()

	ownerID, err := l.Lock(ctx, "renew-key", 200*time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if ownerID != "no-redis" {
		t.Fatalf("expected no-redis owner, got %q", ownerID)
	}

	// 验证 lockState 已被注册
	val, ok := l.locks.Load("renew-key")
	if !ok {
		t.Fatal("expected lockState to be registered")
	}
	st := val.(*lockState)

	// 触发 stop：Unlock 应当 close stopChan 并 cancel 续期 context
	if err := l.Unlock(ctx, "renew-key", ownerID); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	select {
	case <-st.stopChan:
		// expected
	case <-time.After(time.Second):
		t.Error("expected stopChan closed within 1s after Unlock")
	}
}

func TestStartAutoRenew_DefaultInterval(t *testing.T) {
	// 显式不传 WithRenewInterval，验证走 default 分支（ttl/2）。
	l := NewRedisLocker(nil,
		WithGracefulDegradation(),
		WithAutoRenew(),
	)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
	defer cancel()

	ownerID, err := l.Lock(ctx, "default-interval-key", 100*time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	defer func() { _ = l.Unlock(ctx, "default-interval-key", ownerID) }()

	if l.renewInterval != 0 {
		t.Errorf("expected renewInterval to remain 0 (default), got %v", l.renewInterval)
	}
}
