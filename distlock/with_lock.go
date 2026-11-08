package distlock

import (
	"context"
	"fmt"
	"time"

	"github.com/oekit/logx"
)

// instanceLogger 由持有 per-instance logger 的 Locker 实现提供（#120）：
// WithLock 等接口层入口经此取用实例自己的 logger，注入的告警只进该实例的
// logger 而非进程全局；未实现的实现回退包级缺省（行为不变）。
type instanceLogger interface {
	logr() logx.Logger
}

// WithLock 是便捷包装：加锁 → 执行 fn → 自动释放锁。
// 使用 Lock（带超时重试）获取锁，确保在并发场景下更可靠。
// timeout 为锁获取的超时时间，每 100ms 重试一次。
// 推荐使用此 API，避免忘记释放锁。
func WithLock(ctx context.Context, locker Locker, key string, ttl time.Duration, timeout time.Duration, fn func() error) error {
	ownerID, err := locker.Lock(ctx, key, ttl, timeout)
	if err != nil {
		return fmt.Errorf("lock %s: %w", key, err)
	}
	defer func() {
		// 用与 fn 的取消状态解耦的 ctx 释放锁：fn 执行期间 ctx 到期（timeout
		// 参数存在的意义）时，取消的 ctx 会让 unlockScript 在发命令前就返回
		// context.Canceled，DEL 永不执行——锁变孤儿，其他实例阻塞到 TTL 过期。
		// 分布式锁释放失败可能导致孤儿锁直到 TTL 过期，
		// 记录日志便于运维定位，不掩盖失败。
		if e := locker.Unlock(context.WithoutCancel(ctx), key, ownerID); e != nil {
			// per-instance logger 优先（#120）：RedisLocker 经 WithLogger
			// 注入的 logger 在这里同样生效；其它实现用包级缺省。
			lg := log
			if p, ok := locker.(instanceLogger); ok {
				lg = p.logr()
			}
			lg.Warn("distlock unlock failed", "key", key, "owner", ownerID, logx.ErrKey, e)
		}
	}()
	return fn()
}
