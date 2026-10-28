package distlock

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestWithRetryInterval_NonPositive_KeepsDefault 回归测试：非正的重试间隔
// 必须被忽略（保留默认 100ms）。
//
// 缺陷成因：WithRetryInterval 无条件写字段 ⇒ 传 0/负值后，
// Lock() 里的 time.NewTicker(l.retryInterval) 直接 panic
// （"non-positive interval for NewTicker"）——一个选项参数把进程带崩。
func TestWithRetryInterval_NonPositive_KeepsDefault(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		l := NewRedisLocker(nil, WithRetryInterval(d))
		if l.retryInterval != defaultRetryInterval {
			t.Fatalf("非正间隔 %v 应被忽略（保留默认 %v），实得 %v", d, defaultRetryInterval, l.retryInterval)
		}
	}
}

// TestLock_NonPositiveRetryInterval_DoesNotPanic 回归护栏：即使字段被绕过关
// 直接改成非正值（结构体字段同包可写），Lock() 也不得 panic。
//
// 缺陷复现路径：Lock() 在进入重试循环**之前**就 time.NewTicker(interval)
// ⇒ 非正间隔当场 panic，连「锁被占着」这个正常业务分支都走不到。
func TestLock_NonPositiveRetryInterval_DoesNotPanic(t *testing.T) {
	l, _ := newTestLocker(t)

	// 先占住锁，让第二个 locker 走重试分支。
	holder := NewRedisLocker(l.client)
	if _, err := holder.TryLock(context.Background(), "k", 2*time.Second); err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	waiter := NewRedisLocker(l.client)
	waiter.retryInterval = 0 // 直接改字段：绕过 WithRetryInterval 的过滤

	done := make(chan error, 1)
	go func() {
		_, err := waiter.Lock(context.Background(), "k", time.Second, 150*time.Millisecond)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrLockTimeout) {
			t.Fatalf("期望 ErrLockTimeout，实得 %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Lock 未返回（可能 panic 在调用方 goroutine 之外或卡死）")
	}
}

// TestUnlock_NilClient_ReturnsError 回归测试：nil client 且未开优雅降级时，
// Unlock 必须**返回错误**而不是 panic。
//
// 缺陷成因：TryLock / Renew 都有对称的 nil client fail-fast 检查，Unlock
// 漏了 ⇒ unlockScript.Run 在 nil interface 上调用 ⇒ 运行时 nil 指针 panic，
// 而调用方的 defer/recover 通常接不住（它发生在业务语义之外）。
func TestUnlock_NilClient_ReturnsError(t *testing.T) {
	l := NewRedisLocker(nil) // 未开 WithGracefulDegradation

	err := l.Unlock(context.Background(), "k", "owner")
	if err == nil {
		t.Fatal("nil client 且未开降级时 Unlock 应返回错误")
	}
}
