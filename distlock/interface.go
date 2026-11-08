package distlock

import (
	"context"
	"time"
)

// Locker 是分布式锁接口，面向接口编程。
// 支持多种后端实现（Redis、etcd、ZooKeeper 等）。
type Locker interface {
	// TryLock 非阻塞尝试获取锁。
	// 返回 (ownerID, nil) 表示成功，ownerID 用于后续 Unlock。
	// 返回 ("", ErrLockNotAcquired) 表示锁被占用。
	TryLock(ctx context.Context, key string, ttl time.Duration) (string, error)

	// Lock 阻塞获取锁，带超时重试。
	// 返回 (ownerID, nil) 表示成功。
	// 返回 ("", ErrLockTimeout) 表示超时未获取到。
	Lock(ctx context.Context, key string, ttl time.Duration, timeout time.Duration) (string, error)

	// Unlock 释放锁，ownerID 必须匹配。
	// 返回 nil 表示锁已删除；锁不存在或已易主（陈旧持有者解锁）返回
	// ErrLockNotHeld（调用方不得继续假装持有）；其它 error 为后端通信故障。
	Unlock(ctx context.Context, key string, ownerID string) error
}
