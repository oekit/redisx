package distlock

import (
	"context"
	"time"

	pkgdistlock "github.com/oekit/pkg/distlock"
)

// WithLock 是便捷包装：加锁 → 执行 fn → 自动释放锁。
// 使用 Lock（带超时重试）获取锁，确保在并发场景下更可靠。
// timeout 为锁获取的超时时间，每 100ms 重试一次。
// 推荐使用此 API，避免忘记释放锁。
//
// etcdx#123 契约单源后，本函数**只保留薄转发**（签名与语义兼容，调用方零
// 改动）：实现上提至 [pkgdistlock.WithLock]——持锁期 caller ctx 取消不吞释放
// 动作的纪律（context.WithoutCancel）由 pkg 单源，本包不再各持一份。
//
// 行为兼容性说明（v0.15.0 起）：释放失败的告警改由 pkg 侧记录（log/slog，
// component=distlock），不再经 Locker 实现的 per-instance logx logger
// （#120 的 WithLogger 只覆盖本包自身的告警面——watchdog 续期失败/锁丢失/
// 超最大生命周期，见 [WithLogger]；WithLock 释放失败这一条历史实现也路由
// 实例 logger，上提后统一走 slog 侧）。错误返回口径不变：Lock 失败包装为
// `lock <key>: %w`，fn 的结果不被释放失败覆盖。
func WithLock(ctx context.Context, locker Locker, key string, ttl time.Duration, timeout time.Duration, fn func() error) error {
	return pkgdistlock.WithLock(ctx, locker, key, ttl, timeout, fn)
}
