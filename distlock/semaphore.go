package distlock

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// semaphoreAcquireScript 原子「清过期 → 已持有幂等刷新 → 容量检查 → 占槽」。
// KEYS[1]=key ARGV[1]=identifier ARGV[2]=now_ms ARGV[3]=expiry_ms ARGV[4]=max。
// 已持有（ZSCORE 命中，含已过期但未及清理的陈旧条目——此时槽在容量口径上
// 已让出，刷新即重新占用自己的空位）直接刷新过期时间返回 1；
// 否则先 ZREMRANGEBYSCORE 清掉过期槽再检查容量，成员集合大小任何时刻
// 都不会超过 max，插入路径全部经过容量检查或幂等刷新。
var semaphoreAcquireScript = redis.NewScript(`
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score then
	redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
	return 1
end
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[2])
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[4]) then
	redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
	return 1
end
return 0
`)

// semaphoreRenewScript 仅未过期槽可续期。
// KEYS[1]=key ARGV[1]=identifier ARGV[2]=now_ms ARGV[3]=expiry_ms。
// score > now 才续：已过期（或已被清理）的槽不得借 Renew 复活——
// 它的配额可能已被其他 Acquire 接管，复活即超限。
var semaphoreRenewScript = redis.NewScript(`
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if score and tonumber(score) > tonumber(ARGV[2]) then
	redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
	return 1
end
return 0
`)

// semaphoreReleaseScript 幂等释放：ZREM 返回移除数，未持有返回 0 不报错。
var semaphoreReleaseScript = redis.NewScript(`
return redis.call('ZREM', KEYS[1], ARGV[1])
`)

// semaphoreCleanupScript 清理过期槽。
// KEYS[1]=key ARGV[1]=threshold_ms（score < threshold 视为过期）。
var semaphoreCleanupScript = redis.NewScript(`
return redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
`)

// Semaphore 是基于 Redis ZSET 的分布式信号量：同一时刻最多 max 个持有者，
// 配额跨实例/跨进程共享——distlock.Locker 表达不了的「N 个持有者」场景
// （多实例全局并发上限、配额共享资源槽位）。
//
// 槽模型：member = 调用方提供的 identifier，score = 槽过期时间的 Unix 毫秒
// 时间戳（阈值口径同为毫秒）。持有者崩溃后不再 Renew，槽在 slotTTL 后
// 过期，由任意后续 Acquire/Cleanup 原子回收——不永久占槽，配额自愈。
//
// 时间基准为构造后的服务器本地时钟：多实例间只需近似同步（槽过期判定
// 以 Redis 端 score 对比为准，脚本原子执行，时钟偏差只影响 slotTTL 的
// 实际有效期长度，不破坏容量上界）。
//
// 本类型不提供无 Redis 降级模式：信号量承载的是全局配额语义，降级放行
// 会静默突破上界，nil client 一律报错。
type Semaphore struct {
	client  redis.UniversalClient
	key     string
	max     int
	slotTTL time.Duration
	// now 可注入时钟：测试越过 slotTTL 验证过期回收，不真等。
	now func() time.Time
}

// NewSemaphore 创建分布式信号量。key 为完整 Redis key（ZSET），
// max 为最大并发持有者数，slotTTL 为单次占槽的有效期。
// max/slotTTL 非法时在使用时报错（与 distlock 其余入口的 fail-fast 口径一致）。
func NewSemaphore(client redis.UniversalClient, key string, max int, slotTTL time.Duration) *Semaphore {
	return &Semaphore{client: client, key: key, max: max, slotTTL: slotTTL, now: time.Now}
}

// validate 构造参数与运行条件的统一 fail-fast 校验。
func (s *Semaphore) validate() error {
	if s.client == nil {
		return fmt.Errorf("distlock: semaphore requires a non-nil redis client (no graceful degradation for quota semantics)")
	}
	if s.max <= 0 {
		return fmt.Errorf("distlock: semaphore max must be positive, got %d", s.max)
	}
	if s.slotTTL <= 0 {
		return fmt.Errorf("distlock: semaphore slotTTL must be positive, got %v", s.slotTTL)
	}
	return nil
}

// Acquire 尝试占一个槽（非阻塞）。
// identifier 为持有者身份（如「实例 ID:任务 ID」），同一 identifier 重复
// Acquire 是幂等刷新（不占两个槽）。返回 false 表示槽满。
// 内部原子清理过期槽：崩溃持有者的槽在此自愈回收，无需外部 watchdog。
func (s *Semaphore) Acquire(ctx context.Context, identifier string) (bool, error) {
	if identifier == "" {
		return false, fmt.Errorf("distlock: semaphore identifier must not be empty")
	}
	if err := s.validate(); err != nil {
		return false, err
	}
	now := s.now()
	ok, err := semaphoreAcquireScript.Run(ctx, s.client, []string{s.key},
		identifier, now.UnixMilli(), now.Add(s.slotTTL).UnixMilli(), s.max).Int()
	if err != nil {
		return false, fmt.Errorf("redis semaphore acquire failed: %w", err)
	}
	return ok == 1, nil
}

// Renew 续期：仅当 identifier 当前持有未过期槽时，把过期时间顺延
// slotTTL。返回 false 表示槽已丢失（过期回收/已被 Release）——调用方
// watchdog 收到 false 应停止工作并重新 Acquire，继续执行会超出配额；
// error 表示 Redis 通信故障，可重试。
func (s *Semaphore) Renew(ctx context.Context, identifier string) (bool, error) {
	if identifier == "" {
		return false, fmt.Errorf("distlock: semaphore identifier must not be empty")
	}
	if err := s.validate(); err != nil {
		return false, err
	}
	now := s.now()
	ok, err := semaphoreRenewScript.Run(ctx, s.client, []string{s.key},
		identifier, now.UnixMilli(), now.Add(s.slotTTL).UnixMilli()).Int()
	if err != nil {
		return false, fmt.Errorf("redis semaphore renew failed: %w", err)
	}
	return ok == 1, nil
}

// Release 释放 identifier 持有的槽，幂等：未持有（已过期回收/重复释放）
// 也返回 nil，与 Locker.Unlock 的宽松口径一致。
func (s *Semaphore) Release(ctx context.Context, identifier string) error {
	if identifier == "" {
		return fmt.Errorf("distlock: semaphore identifier must not be empty")
	}
	if err := s.validate(); err != nil {
		return err
	}
	_, err := semaphoreReleaseScript.Run(ctx, s.client, []string{s.key}, identifier).Result()
	if err != nil {
		return fmt.Errorf("redis semaphore release failed: %w", err)
	}
	return nil
}

// Cleanup 清理「已过期超过 olderThan」的槽，返回清理条数。
// olderThan=0 清理所有已过期槽；olderThan>0 留出宽限，只清过期时间
// 早于 now-olderThan 的（巡检时避免干扰刚到期的边界槽）。
// 日常路径无需显式调用——Acquire 内部已原子清理；本方法面向运维巡检
// 与配额占用指标观测。
func (s *Semaphore) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	if err := s.validate(); err != nil {
		return 0, err
	}
	threshold := s.now().Add(-olderThan).UnixMilli()
	n, err := semaphoreCleanupScript.Run(ctx, s.client, []string{s.key}, threshold).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis semaphore cleanup failed: %w", err)
	}
	return n, nil
}
