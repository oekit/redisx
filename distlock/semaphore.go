package distlock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oekit/logx"
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
// 可选自动续期（[WithSemaphoreAutoRenew]，#126，与 RedisLocker 的
// WithAutoRenew 家族同构）：Acquire 成功后 watchdog 按 slotTTL/2（缺省推导）
// 周期续期保槽，长任务不再需要下游手拼 watchdog；Release 后即停。手动
// Renew 与 watchdog 解耦，两种口径可并存（各自独立调用）。
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

	// 自动续期配置（#126，仅构造期经选项写入）
	autoRenew     bool
	renewInterval time.Duration
	// maxAutoRenew 自动续期最大生命周期：0 = 默认 10×slotTTL；
	// 负值 = 不限制（兼容旧行为）；正值 = 显式上限。
	maxAutoRenew time.Duration
	// log 是 per-instance 日志（与 RedisLocker 的 WithLogger 同形）：watchdog
	// 告警只进该 logger，缺省为实例私有的 logx.Delegating("distlock")。
	log logx.Logger
	// sessions 是自动续期运行态：identifier → *lockState（复用 RedisLocker
	// 的续期状态机形态，identifier 对位 ownerID）。
	sessions sync.Map
}

// SemaphoreOption 配置 Semaphore 的可选参数。
type SemaphoreOption func(*Semaphore)

// WithSemaphoreAutoRenew 启用自动续期 watchdog（#126，与 RedisLocker 的
// WithAutoRenew 同构）：Acquire 成功后自动续期槽位直到 Release。
// interval 缺省 slotTTL/2（与 Lock 同推导，[WithSemaphoreRenewInterval] 可
// 自定义）；Renew 失败经 [WithSemaphoreLogger] 上报告警；
// [WithSemaphoreMaxAutoRenew] 安全网同 Lock 语义（忘 Release 时续期到期
// 自行退出，槽位 slotTTL 后自然回收）。Renew 返回 (false, nil)（槽位已被
// 回收重分配）时告警并退出 watchdog——「以为自己还占着配额」是静默脑裂
// 形态，调用方应重新 Acquire。
func WithSemaphoreAutoRenew() SemaphoreOption {
	return func(s *Semaphore) { s.autoRenew = true }
}

// WithSemaphoreRenewInterval 设置自动续期间隔，默认 slotTTL/2。
// 仅在启用 [WithSemaphoreAutoRenew] 时生效；d <= 0 忽略（保留默认推导）。
func WithSemaphoreRenewInterval(d time.Duration) SemaphoreOption {
	return func(s *Semaphore) {
		if d > 0 {
			s.renewInterval = d
		}
	}
}

// WithSemaphoreMaxAutoRenew 设置自动续期的最大生命周期（安全网）。
// 忘记 Release 时，续期 goroutine 到期后自行退出并记 Warn，槽位在 slotTTL
// 后自然过期回收——不形成永久占槽 + goroutine 泄漏。
//   - 0（默认）：10×slotTTL；
//   - 负值：不限制（需调用方保证 Release 一定被调用）；
//   - 正值：显式上限。
//
// 仅在启用 [WithSemaphoreAutoRenew] 时生效。
func WithSemaphoreMaxAutoRenew(d time.Duration) SemaphoreOption {
	return func(s *Semaphore) { s.maxAutoRenew = d }
}

// WithSemaphoreLogger 注入 per-instance 日志（与 RedisLocker 的 WithLogger
// 同形，#120）：自动续期 watchdog 的告警（续期失败/槽位丢失/超过最大生命
// 周期）只进该 logger，宿主的 zap/slog/hlog 体系无需动进程级 logx.SetDefault
// 即可收编本组件的日志。传 nil 保留缺省（实例私有的 logx.Delegating("distlock")，
// 落点与历史行为一致）。
func WithSemaphoreLogger(lg logx.Logger) SemaphoreOption {
	return func(s *Semaphore) {
		if lg != nil {
			s.log = lg
		}
	}
}

// NewSemaphore 创建分布式信号量。key 为完整 Redis key（ZSET），
// max 为最大并发持有者数，slotTTL 为单次占槽的有效期。
// max/slotTTL 非法时在使用时报错（与 distlock 其余入口的 fail-fast 口径一致）。
func NewSemaphore(client redis.UniversalClient, key string, max int, slotTTL time.Duration, opts ...SemaphoreOption) *Semaphore {
	s := &Semaphore{
		client:  client,
		key:     key,
		max:     max,
		slotTTL: slotTTL,
		now:     time.Now,
		log:     logx.Delegating("distlock"),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// logr 返回本实例的 logger；零值实例（未经构造器）兜底回包级缺省，绝不 nil。
func (s *Semaphore) logr() logx.Logger {
	if s.log != nil {
		return s.log
	}
	return log
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
// 启用 [WithSemaphoreAutoRenew] 时，占槽成功即启动自动续期 watchdog
// （同一 identifier 重复 Acquire 会重启 watchdog，旧 goroutine 被安全停掉）。
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
	if ok == 1 && s.autoRenew {
		s.startAutoRenew(identifier)
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
// 也返回 nil（槽位语义天然幂等，与 Locker.Unlock 的显式 ErrLockNotHeld
// 契约有意不同，#115）。
//
// 与自动续期 watchdog 的时序（#126，对齐 Locker.Unlock 的 #115 口径）：
// 释放（网络）先行、watchdog 后停——释放结果明确（nil，含幂等未持有）才停；
// 通信故障时 watchdog 继续续期兜底（槽位可能仍在），调用方可重试 Release。
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
	s.stopAutoRenew(identifier)
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

// startAutoRenew 启动槽位自动续期 watchdog（#126）：每次 Acquire 成功创建
// 独立 lockState（identifier 对位 ownerID），per-identifier 隔离；独立
// derived context，续期生命周期与槽位绑定而非调用方 context。
//
// 状态机本体（Swap-停旧、interval 推导、maxLifetime 三态、goroutine 循环）
// 与 RedisLocker 单源同一份 [renewWatchdog]（#159）——#33/#39 两个历史修复
// 从此只有一份；本方法只注入 renew 闭包与告警文案。幂等刷新成功意味着槽位
// 仍是本人的，覆盖旧 state 前先停掉旧续期（newRenewWatchdog 内置的 #33 防御）。
//
// 续期受最大生命周期约束（WithSemaphoreMaxAutoRenew，默认 10×slotTTL）：到期
// 后 goroutine 退出并记 Warn，槽位在 slotTTL 后自然过期回收——防止忘记
// Release 导致永久占槽与 goroutine 泄漏（配额自愈语义重新生效）。
func (s *Semaphore) startAutoRenew(identifier string) {
	w := newRenewWatchdog(&s.sessions, identifier, identifier)
	w.interval = renewIntervalFrom(s.renewInterval, s.slotTTL)
	w.maxLifetime = renewMaxLifetime(s.maxAutoRenew, s.slotTTL)
	w.renew = func(ctx context.Context) (bool, error) {
		return s.Renew(ctx, identifier)
	}
	w.onRetryErr = func(err error) {
		s.logr().Warn("auto renew failed, will retry",
			"key", s.key, "identifier", identifier, logx.ErrKey, err)
	}
	w.onLost = func() {
		// Renew 返回 (false, nil)：槽位已丢失（过期回收/已被 Release/配额被
		// 他人接管）——「以为自己还占着配额」是静默脑裂形态（#126），调用方
		// 应重新 Acquire。
		s.logr().Warn("auto renew failed: semaphore slot lost or recycled",
			"key", s.key, "identifier", identifier)
	}
	w.onGaveUp = func(maxLifetime time.Duration) {
		s.logr().Warn("auto renew exceeded max lifetime, giving up slot",
			"key", s.key, "identifier", identifier, "max_lifetime", maxLifetime)
	}
	w.startAutoRenew()
}

// stopAutoRenew 停掉 identifier 对应的续期 watchdog。LoadAndDelete 原子取删：
// 只有拿到状态的这一方负责 stop（幂等，stopState once 保护），不存在「误杀
// 他人」的形态——key 即 identifier，状态与身份一一对应（区别于 Locker 的
// key/ownerID 二元比对）。
func (s *Semaphore) stopAutoRenew(identifier string) {
	if v, ok := s.sessions.LoadAndDelete(identifier); ok {
		stopState(v.(*lockState))
	}
}
