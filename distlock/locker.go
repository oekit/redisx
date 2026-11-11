package distlock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oekit/logx"
	pkgdistlock "github.com/oekit/pkg/distlock"
	"github.com/redis/go-redis/v9"
)

// log 是包级缺省 logger（懒解析全局 Default）。#120 后 RedisLocker 与
// Semaphore 的实例日志走 per-instance 的 l.log / s.log（缺省同样是
// Delegating("distlock")，行为不变）；本变量是零值实例（未经构造器）时
// logr() 的兜底。WithLock 释放失败的告警已随契约单源上提 pkg（etcdx#123，
// 走 log/slog），不再经本变量。
var log = logx.Delegating("distlock")

// 哨兵错误集（etcdx#123 契约单源后改为**别名**指向 [pkgdistlock] 的同一套
// 值——同进程只此一套哨兵，下游用本包或 pkg 侧任一写法 errors.Is 都命中，
// 两仓各自 errors.New 造成的「同文案不同值、跨包判定恒 false」从此不可能；
// 文案以本仓历史为基准，逐字不变）。变量仍保存在本包名下：既有调用方的
// `distlock.ErrLockTimeout` 等引用零改动。
var (
	// ErrLockNotAcquired 表示锁被其他实例持有，获取失败。
	ErrLockNotAcquired = pkgdistlock.ErrLockNotAcquired

	// ErrLockTimeout 表示在指定超时时间内未能获取锁。
	ErrLockTimeout = pkgdistlock.ErrLockTimeout

	// ErrLockNotHeld 表示解锁时锁不存在或已易主——本次 Unlock **没有删除任何锁**
	// （#115）。典型成因：锁 TTL 过期后已被他人重新获取、或对同一 (key, ownerID)
	// 重复 Unlock。与 [Renew] 返回 (false, nil) 的「锁已丢失」语义对齐：调用方
	// 收到本哨兵即说明自己已不再持有锁，**不得继续在临界区内工作**（继续执行会
	// 与新持有者并发）；errors.Is 判定，与其它 Redis 错误（可重试的通信故障）
	// 区分。
	ErrLockNotHeld = pkgdistlock.ErrLockNotHeld
)

var unlockScript = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
`)

// renewScript Lua 续期脚本（原子 check-and-renew）。
// 用 PEXPIRE 毫秒精度：SETEX秒级精度对 sub-second TTL 会截断为 0，
// EXPIRE 0 在 Redis 里等价于立即删除。
//
// **脚本自身也兜住「删锁」**（2026-10-28，#54）：即便调用方传入 ≤ 0 的
// 毫秒值，脚本也只返回 0（续期失败），**绝不执行 PEXPIRE 0** ⇒
// 「锁被删掉却报告续期成功」这种事不可能再发生。
var renewScript = redis.NewScript(`
	if redis.call('get', KEYS[1]) == ARGV[1] then
		local ms = tonumber(ARGV[2])
		if not ms or ms <= 0 then
			return 0
		end
		return redis.call('pexpire', KEYS[1], ARGV[2])
	else
		return 0
	end
`)

// RedisLocker 是基于 Redis 的分布式锁实现。
type RedisLocker struct {
	client              redis.UniversalClient
	prefix              string
	gracefulDegradation bool
	retryInterval       time.Duration
	onLock              func(key string, acquired bool, err error)
	onUnlock            func(key string, err error)
	// log 是 per-instance 日志（#120）：构造期经 WithLogger 注入，缺省为
	// 本实例私有的 logx.Delegating("distlock")（与既有包级缺省行为一致——
	// 仍是懒解析全局 Default，宿主未 SetDefault 时落 stderr；区别是组件名
	// 随实例携带，且注入后告警只进该实例的 logger，不再与进程全局耦合）。
	log logx.Logger

	// 自动续期相关
	autoRenew     bool
	renewInterval time.Duration
	// maxAutoRenew 自动续期最大生命周期：0 = 默认 10×ttl；
	// 负值 = 不限制（兼容旧行为）；正值 = 显式上限。
	maxAutoRenew time.Duration

	// per-key 锁状态（key → *lockState）
	locks sync.Map
}

// RedisLockerOption 配置 RedisLocker 的可选参数。
type RedisLockerOption func(*RedisLocker)

// WithPrefix 设置锁 key 前缀，默认为 "lock"。
// 例如 WithPrefix("fuxi") 生成 key "fuxi:lock:<user-key>"。
func WithPrefix(prefix string) RedisLockerOption {
	return func(l *RedisLocker) { l.prefix = prefix }
}

// WithGracefulDegradation 启用无 Redis 时的降级模式。
// 当 Redis 不可用（client 为 nil）时，加锁操作直接成功（适合单实例部署）。
func WithGracefulDegradation() RedisLockerOption {
	return func(l *RedisLocker) { l.gracefulDegradation = true }
}

// WithRetryInterval 设置 Lock() 重试间隔，默认 100ms。
//
// **非正值被忽略**（保留默认 100ms）：time.NewTicker 对非正间隔会直接
// panic（"non-positive interval for NewTicker"），而这里是选项函数、
// 没有返回值可报错 ⇒ 传 0/负值不该把进程带崩。
func WithRetryInterval(d time.Duration) RedisLockerOption {
	return func(l *RedisLocker) {
		if d <= 0 {
			return
		}
		l.retryInterval = d
	}
}

// WithLogger 注入 per-instance 日志（#120，与 oagentkit sqllease 的
// WithLeaderLogger 同形）：自动续期 watchdog 的告警（续期失败/锁丢失/超过
// 最大生命周期）只进该 logger，宿主的 zap/slog/hlog 体系无需动进程级
// logx.SetDefault 即可收编本组件的日志。传 nil 保留缺省（实例私有的
// logx.Delegating("distlock")，落点与历史行为一致）。
func WithLogger(log logx.Logger) RedisLockerOption {
	return func(l *RedisLocker) {
		if log != nil {
			l.log = log
		}
	}
}

// WithOnLock 设置加锁事件回调。
// acquired=true 表示成功，acquired=false 表示锁被占用，err 非 nil 表示 Redis 错误。
// 用于日志记录和监控。
func WithOnLock(fn func(key string, acquired bool, err error)) RedisLockerOption {
	return func(l *RedisLocker) { l.onLock = fn }
}

// WithOnUnlock 设置解锁事件回调。
// err 语义与 Unlock 返回值一致（#115）：nil = 锁已删除；ErrLockNotHeld =
// 锁不存在或已易主（陈旧持有者，未删任何锁）；其它 = Redis 通信故障。
// 用于日志记录和监控。
func WithOnUnlock(fn func(key string, err error)) RedisLockerOption {
	return func(l *RedisLocker) { l.onUnlock = fn }
}

// WithAutoRenew 启用自动续期。
// 续期间隔默认为 TTL/2，可通过 WithRenewInterval 自定义。
// Lock 与 TryLock 成功后均启动续期（对称语义）：TryLock 场景的
// 「非阻塞获取 + 持有期续期」不再需要用 Lock(timeout=0) 模拟。
func WithAutoRenew() RedisLockerOption {
	return func(l *RedisLocker) { l.autoRenew = true }
}

// WithRenewInterval 设置自动续期间隔。
// 仅在启用 WithAutoRenew 时生效。
func WithRenewInterval(d time.Duration) RedisLockerOption {
	return func(l *RedisLocker) { l.renewInterval = d }
}

// WithMaxAutoRenew 设置自动续期的最大生命周期（安全网）。
// 忘记 Unlock 时，续期 goroutine 到期后自行退出，锁在 TTL 后自然过期，
// 不会形成永久持锁 + goroutine 泄漏。
//   - 0（默认）：10×ttl；
//   - 负值：不限制（兼容旧行为，需调用方保证 Unlock 一定被调用）；
//   - 正值：显式上限。
//
// 仅在启用 WithAutoRenew 时生效。
func WithMaxAutoRenew(d time.Duration) RedisLockerOption {
	return func(l *RedisLocker) { l.maxAutoRenew = d }
}

// defaultRetryInterval 是 Lock() 的默认重试间隔。
const defaultRetryInterval = 100 * time.Millisecond

// NewRedisLocker 创建基于 Redis 的分布式锁实例。
// client 为 nil 且启用了 WithGracefulDegradation 时，所有操作直接成功。
func NewRedisLocker(client redis.UniversalClient, opts ...RedisLockerOption) *RedisLocker {
	l := &RedisLocker{
		client:        client,
		prefix:        "lock",
		retryInterval: defaultRetryInterval,
		log:           logx.Delegating("distlock"),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// logr 返回本实例的 logger（#120，watchdog 告警取用）；零值实例（未经
// 构造器）兜底回包级缺省，绝不 nil。
func (l *RedisLocker) logr() logx.Logger {
	if l.log != nil {
		return l.log
	}
	return log
}

func (l *RedisLocker) lockKey(key string) string {
	return l.prefix + ":" + key
}

func (l *RedisLocker) generateValue() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand.Read failed: %w", err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// TryLock 尝试获取分布式锁（非阻塞，单次尝试）。
// 返回 (ownerID, nil) 表示成功，ownerID 用于后续 Unlock。
// 返回 ("", ErrLockNotAcquired) 表示锁被占用。
// 返回 ("", err) 表示 Redis 错误。
// 启用 WithAutoRenew 时，成功获取后与 Lock 对称地启动自动续期。
func (l *RedisLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	if l.client == nil && l.gracefulDegradation {
		// graceful 模式下同样注册 lockState 保持 Lock/Unlock 状态机对称。
		if l.autoRenew {
			l.startAutoRenew(ctx, key, "no-redis", ttl)
		}
		return "no-redis", nil
	}
	// go-redis SetNX 的 ttl=0 表示永不过期——零值 Duration（未初始化的
	// 常见笔误）会产生打不破的锁，持锁进程崩溃后所有实例永久阻塞。
	if ttl <= 0 {
		return "", fmt.Errorf("distlock: ttl must be positive, got %v", ttl)
	}
	// 非法组合 fail-fast：nil client 且未开优雅降级时，缺构造校验会把
	// 故障推迟到第一次加锁的运行时 nil interface panic。
	if l.client == nil {
		return "", fmt.Errorf("distlock: redis client is nil (construct with a client or enable WithGracefulDegradation)")
	}

	value, err := l.generateValue()
	if err != nil {
		return "", err
	}
	acquired, err := l.client.SetNX(ctx, l.lockKey(key), value, ttl).Result()
	if err != nil {
		if l.onLock != nil {
			l.onLock(key, false, err)
		}
		return "", fmt.Errorf("redis SetNX failed: %w", err)
	}
	if !acquired {
		if l.onLock != nil {
			l.onLock(key, false, nil)
		}
		return "", ErrLockNotAcquired
	}
	if l.onLock != nil {
		l.onLock(key, true, nil)
	}
	if l.autoRenew {
		l.startAutoRenew(ctx, key, value, ttl)
	}
	return value, nil
}

// Unlock 释放分布式锁（Lua 原子操作，只有锁的持有者才能释放）。
// ownerID 是 TryLock/Lock 返回的值。
//
// 返回值语义（#115，显式化）：
//   - nil：锁已被本人成功删除；
//   - [ErrLockNotHeld]：锁不存在或已易主，本次 Unlock 没有删除任何锁——
//     TTL 过期易主后陈旧持有者的解锁不再「假成功」（脚本返回 0 被丢弃、
//     err==nil 的旧口径与 Renew 的「锁已丢失」显式契约自相矛盾，也让调用方
//     在不知情下继续假装持有）；errors.Is 判定；
//   - 其它 error：Redis 通信故障（可重试，与 Renew 的 error 语义对齐）。
//
// 续期 watchdog 的时序（#115 第二半）：删锁（网络）先行、watchdog 后停——
// 修复前先 LoadAndDelete 杀 watchdog 再走网络删锁，删锁网络瞬断时
// 「续期已停而锁还挂着」⇒ 锁将在 TTL 后静默失效，调用方却拿到可重试 error
// 误以为还在持有。现在仅当删锁结果明确（成功或非本人）才清理状态；
// 通信故障时 watchdog 继续续期，锁不静默丢失。
//
// 状态清理仍用"LoadAndDelete 再比对 ownerID"消除竞态：ownerID 不匹配时
// 原样回填（LoadOrStore 保证不覆盖窗口期新存入的第三个状态），绝不误杀
// 新持有者的续期。
func (l *RedisLocker) Unlock(ctx context.Context, key string, ownerID string) error {
	// graceful 降级模式没有真锁可删：只做状态机对称清理（watchdog 注册了
	// 但无 goroutine），与旧行为一致。
	if l.client == nil && l.gracefulDegradation {
		l.stopRenewState(key, ownerID)
		return nil
	}
	// 非法组合 fail-fast：nil client 且未开优雅降级时，缺这一检查会让
	// unlockScript.Run 在 nil interface 上调用 ⇒ 运行时 nil 指针 panic
	// （TryLock/Renew 都有对称检查，Unlock 此前漏了）。
	if l.client == nil {
		return fmt.Errorf("distlock: redis client is nil (construct with a client or enable WithGracefulDegradation)")
	}

	// 删锁先行（#115）：拿到脚本结果前不碰 watchdog 状态。
	result, err := unlockScript.Run(ctx, l.client, []string{l.lockKey(key)}, ownerID).Int()
	if err != nil {
		// 通信故障：锁可能仍在，watchdog 不停（继续续期兜底），状态原样保留。
		werr := fmt.Errorf("redis unlock failed: %w", err)
		if l.onUnlock != nil {
			l.onUnlock(key, werr)
		}
		return werr
	}
	// 脚本返回 0：锁不存在或非本人（TTL 过期易主/重复解锁）——显式哨兵，
	// 不再「假成功」。
	if result == 0 {
		l.stopRenewState(key, ownerID)
		if l.onUnlock != nil {
			l.onUnlock(key, ErrLockNotHeld)
		}
		return ErrLockNotHeld
	}

	// 删锁成功，停掉本人的续期状态。
	l.stopRenewState(key, ownerID)
	if l.onUnlock != nil {
		l.onUnlock(key, nil)
	}
	return nil
}

// stopRenewState 清理 key 对应的续期状态：只停/只删属于 ownerID 的状态；
// 属于他人（锁易主后新持有者）的状态原样回填，绝不误杀。LoadOrStore 回填
// 保证不覆盖窗口期新存入的第三个状态。stop 收尾（once.Do cancel+close）单源
// 在 [stopRenewState]（#159，watchdog 状态机包内单源）。
func (l *RedisLocker) stopRenewState(key string, ownerID string) {
	if v, ok := l.locks.LoadAndDelete(key); ok {
		cur := v.(*lockState)
		if cur.ownerID == ownerID {
			stopState(cur)
		} else {
			l.locks.LoadOrStore(key, cur)
		}
	}
}

// Renew 手动续期：原子 check-and-renew（Lua CAS，GET==ownerID → PEXPIRE），
// 只有 ownerID 仍持有锁才续期，毫秒精度。
// 返回 true 表示续期成功；false 表示锁已丢失/易主/过期（调用方应停止
// 当前工作并重新获取锁，继续执行会与新的持有者并发）；error 表示 Redis
// 通信故障（网络抖动等，可重试）。
// 与 WithAutoRenew 的 watchdog 解耦：调用方自管续期节奏，不受
// WithMaxAutoRenew 安全网约束。方法在具体类型 *RedisLocker 上、
// 不进 Locker 接口（etcdx 等其他后端实现保持接口同形可互换）。
func (l *RedisLocker) Renew(ctx context.Context, key, ownerID string, ttl time.Duration) (bool, error) {
	if l.client == nil && l.gracefulDegradation {
		return true, nil
	}
	if l.client == nil {
		return false, fmt.Errorf("distlock: redis client is nil (construct with a client or enable WithGracefulDegradation)")
	}
	if ttl <= 0 {
		return false, fmt.Errorf("distlock: ttl must be positive, got %v", ttl)
	}
	result, err := renewScript.Run(ctx, l.client, []string{l.lockKey(key)}, ownerID, ttlMillis(ttl)).Int()
	if err != nil {
		return false, fmt.Errorf("redis renew failed: %w", err)
	}
	return result == 1, nil
}

// ttlMillis 把 TTL 量化成 Redis PEXPIRE 用的毫秒值，**下限 1ms**。
//
// 为什么要有下限（#54）：`PEXPIRE key 0` 在 Redis 语义里等于**立即删除该键**
// ⇒ 亚毫秒 TTL（`ttl.Milliseconds() == 0`）会让续期把锁**删掉**，而调用方
// 拿到的是「续期成功」⇒ 随后在「自己并不持有锁」的状态下写临界区 ⇒
// 另一实例可同时 SetNX 成功 ⇒ 静默脑裂。
//
// 量化下限与**写路径**一致：go-redis 的 formatMs 把亚毫秒的
// `SET NX PX` 抬到 1ms ⇒ 同一次加锁/续期的 TTL 语义一致
// （不会出现「写 1ms、续期 0ms」这种自相矛盾）。
func ttlMillis(ttl time.Duration) int64 {
	ms := ttl.Milliseconds()
	if ms < 1 {
		return 1
	}
	return ms
}

// Lock 带超时重试地获取分布式锁（阻塞直到成功或超时）。
// 重试间隔由 WithRetryInterval 配置（默认 100ms）。
// 如果启用了自动续期，成功获取锁后会启动续期 goroutine
// （由 TryLock 统一负责，避免同一把锁重复启动续期，#33）。
// 返回 (ownerID, nil) 表示成功。
// 返回 ("", ErrLockTimeout) 表示超时未获取到。
func (l *RedisLocker) Lock(ctx context.Context, key string, ttl time.Duration, timeout time.Duration) (string, error) {
	if l.client == nil && l.gracefulDegradation {
		// graceful 模式下也尊重 WithAutoRenew：注册 lockState 让 Unlock
		// 能找到并停止续期（虽然没真续期，状态机对称）。
		if l.autoRenew {
			l.startAutoRenew(ctx, key, "no-redis", ttl)
		}
		return "no-redis", nil
	}

	deadline := time.Now().Add(timeout)
	// 兜底：非正间隔会让 time.NewTicker 直接 panic。WithRetryInterval 已
	// 过滤非正值，这里再兜一层（结构体字段可被同包代码直接改写）。
	interval := l.retryInterval
	if interval <= 0 {
		interval = defaultRetryInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		ownerID, err := l.TryLock(ctx, key, ttl)
		if err == nil {
			// TryLock 成功时已按「成功即续期」的语义启动自动续期（见 TryLock），
			// 此处不得重复启动：重复启动会用新 state 覆盖旧 state，使先启动的
			// 续期 goroutine 成为孤儿 —— Unlock 的 LoadAndDelete 只停得到后启动
			// 的那个，孤儿要到下一个 tick 才发现锁已不属于自己，并打出误导性的
			// "auto renew failed: lock lost or not owned"（#33）。
			return ownerID, nil
		}
		if !errors.Is(err, ErrLockNotAcquired) {
			return "", err
		}

		if time.Now().After(deadline) {
			return "", ErrLockTimeout
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// startAutoRenew 启动自动续期 goroutine。
// 每次 Lock 成功时创建独立的 lockState，per-key 隔离。
// 使用独立的 derived context，续期生命周期与锁绑定而非调用方 context。
//
// 状态机本体（Swap-停旧、interval 推导、maxLifetime 三态、goroutine 循环）
// 单源在包内 [renewWatchdog]（#159）：#33/#39 两个历史修复从此只有一份，
// 本方法只注入 Redis 侧 renew 闭包与告警文案。
//
// 续期受最大生命周期约束（WithMaxAutoRenew，默认 10×ttl）：到期后
// goroutine 退出并记 Warn，锁在 TTL 后自然过期 —— 防止忘记 Unlock
// 导致永久持锁与 goroutine 泄漏（TTL 安全网重新生效）。
func (l *RedisLocker) startAutoRenew(_ context.Context, key string, ownerID string, ttl time.Duration) {
	w := newRenewWatchdog(&l.locks, key, ownerID)

	// graceful 降级模式没有真锁，也无从续期；只注册状态保持
	// Lock/Unlock 状态机对称，不启动 goroutine（否则每 tick nil client panic）。
	if l.client == nil {
		return
	}

	w.interval = renewIntervalFrom(l.renewInterval, ttl)
	w.maxLifetime = renewMaxLifetime(l.maxAutoRenew, ttl)
	w.renew = func(ctx context.Context) (bool, error) {
		result, err := renewScript.Run(ctx, l.client, []string{l.lockKey(key)}, ownerID, ttlMillis(ttl)).Int()
		if err != nil {
			return false, err
		}
		return result == 1, nil
	}
	w.onRetryErr = func(err error) {
		l.logr().Warn("auto renew failed, will retry", "key", key, logx.ErrKey, err)
	}
	w.onLost = func() {
		l.logr().Warn("auto renew failed: lock lost or not owned", "key", key)
	}
	w.onGaveUp = func(maxLifetime time.Duration) {
		l.logr().Warn("auto renew exceeded max lifetime, giving up lock", "key", key, "max_lifetime", maxLifetime)
	}
	w.startAutoRenew()
}
