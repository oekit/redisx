package distlock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
	"github.com/redis/go-redis/v9"
)

var log = logx.Delegating("distlock")

// ErrLockNotAcquired 表示锁被其他实例持有，获取失败。
var ErrLockNotAcquired = errors.New("lock not acquired")

// ErrLockTimeout 表示在指定超时时间内未能获取锁。
var ErrLockTimeout = errors.New("lock acquire timeout")

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
var renewScript = redis.NewScript(`
	if redis.call('get', KEYS[1]) == ARGV[1] then
		return redis.call('pexpire', KEYS[1], ARGV[2])
	else
		return 0
	end
`)

// lockState 管理单个锁的自动续期状态（per-key 隔离）。
// ownerID 记录本状态对应的锁持有者，Unlock 时校验，
// 防止超时后的陈旧持有者误杀新持有者的续期。
type lockState struct {
	ownerID  string
	stopChan chan struct{}
	cancel   context.CancelFunc
	once     sync.Once
}

// RedisLocker 是基于 Redis 的分布式锁实现。
type RedisLocker struct {
	client              redis.UniversalClient
	prefix              string
	gracefulDegradation bool
	retryInterval       time.Duration
	onLock              func(key string, acquired bool, err error)
	onUnlock            func(key string, err error)

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
func WithRetryInterval(d time.Duration) RedisLockerOption {
	return func(l *RedisLocker) { l.retryInterval = d }
}

// WithOnLock 设置加锁事件回调。
// acquired=true 表示成功，acquired=false 表示锁被占用，err 非 nil 表示 Redis 错误。
// 用于日志记录和监控。
func WithOnLock(fn func(key string, acquired bool, err error)) RedisLockerOption {
	return func(l *RedisLocker) { l.onLock = fn }
}

// WithOnUnlock 设置解锁事件回调。
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

// NewRedisLocker 创建基于 Redis 的分布式锁实例。
// client 为 nil 且启用了 WithGracefulDegradation 时，所有操作直接成功。
func NewRedisLocker(client redis.UniversalClient, opts ...RedisLockerOption) *RedisLocker {
	l := &RedisLocker{
		client:        client,
		prefix:        "lock",
		retryInterval: 100 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
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
// 续期状态的清理采用"先 LoadAndDelete 再比对 ownerID"的顺序，
// 消除 Load→比对→LoadAndDelete 窗口内的竞态：锁超时易主后，陈旧持有者的
// Unlock 不会误删/误杀新持有者的续期状态（ownerID 不匹配时原样回填，
// 回填用 LoadOrStore 保证不覆盖窗口期新存入的第三个状态）。
func (l *RedisLocker) Unlock(ctx context.Context, key string, ownerID string) error {
	if v, ok := l.locks.LoadAndDelete(key); ok {
		cur := v.(*lockState)
		if cur.ownerID == ownerID {
			cur.once.Do(func() {
				if cur.cancel != nil {
					cur.cancel()
				}
				close(cur.stopChan)
			})
		} else {
			// 陈旧持有者解锁新持有者的锁：回填新持有者的状态。
			// LoadOrStore：若窗口期已有第三个状态存入（他人重新获锁），
			// 保留第三个状态，丢弃我们删除的这份（其续期由自身生命周期
			// 或持有者后续 Unlock 兜底，绝不误杀在持续期）。
			l.locks.LoadOrStore(key, cur)
		}
	}

	if l.client == nil && l.gracefulDegradation {
		return nil
	}

	_, err := unlockScript.Run(ctx, l.client, []string{l.lockKey(key)}, ownerID).Result()
	if l.onUnlock != nil {
		l.onUnlock(key, err)
	}
	return err
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
	result, err := renewScript.Run(ctx, l.client, []string{l.lockKey(key)}, ownerID, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("redis renew failed: %w", err)
	}
	return result == 1, nil
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
	ticker := time.NewTicker(l.retryInterval)
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
// 续期受最大生命周期约束（WithMaxAutoRenew，默认 10×ttl）：到期后
// goroutine 退出并记 Warn，锁在 TTL 后自然过期 —— 防止忘记 Unlock
// 导致永久持锁与 goroutine 泄漏（TTL 安全网重新生效）。
func (l *RedisLocker) startAutoRenew(_ context.Context, key string, ownerID string, ttl time.Duration) {
	renewCtx, cancel := context.WithCancel(context.Background())
	st := &lockState{ownerID: ownerID, stopChan: make(chan struct{}), cancel: cancel}
	// 覆盖旧 state 前先停掉它（防御性，#33）：同一 key 的旧 state 若仍在续期，
	// 它的 goroutine 只能等下一个 tick 才发现锁已不属于自己 —— 既泄漏 goroutine，
	// 又打出误导性的 "auto renew failed: lock lost or not owned"。新获取锁成功
	// （SetNX 通过）意味着旧持有者的锁已不存在，停掉旧续期是安全的。
	if old, ok := l.locks.Swap(key, st); ok {
		if prev, ok := old.(*lockState); ok {
			prev.once.Do(func() {
				if prev.cancel != nil {
					prev.cancel()
				}
				close(prev.stopChan)
			})
		}
	}

	// graceful 降级模式没有真锁，也无从续期；只注册状态保持
	// Lock/Unlock 状态机对称，不启动 goroutine（否则每 tick nil client panic）。
	if l.client == nil {
		return
	}

	interval := l.renewInterval
	if interval <= 0 {
		interval = ttl / 2
	}
	if interval <= 0 {
		interval = 1 * time.Second
	}

	// 计算续期最大生命周期：0（未设置）= 10×ttl；负值 = 不限制（0 表示无 deadline）。
	maxLifetime := l.maxAutoRenew
	switch {
	case maxLifetime == 0:
		maxLifetime = 10 * ttl
	case maxLifetime < 0:
		maxLifetime = 0
	}
	var lifetimeTimer *time.Timer
	var lifetimeC <-chan time.Time
	if maxLifetime > 0 {
		lifetimeTimer = time.NewTimer(maxLifetime)
		defer lifetimeTimer.Stop()
		lifetimeC = lifetimeTimer.C
	}

	stopChan := st.stopChan

	async.GoSafe(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-renewCtx.Done():
				return
			case <-stopChan:
				return
			case <-lifetimeC:
				// 达到最大续期生命周期：放弃续期，锁将在 TTL 后自然过期。
				// 这是忘记 Unlock 时的安全网（默认 10×ttl）。
				log.Warn("auto renew exceeded max lifetime, giving up lock", "key", key, "max_lifetime", maxLifetime)
				return
			case <-ticker.C:
				result, err := renewScript.Run(renewCtx, l.client, []string{l.lockKey(key)}, ownerID, ttl.Milliseconds()).Int()
				if err != nil {
					// 瞬时错误（网络抖动、failover）不放弃：下一个 tick 重试。
					// 续期 goroutine 一旦退出，锁将在 TTL 后静默失效。
					log.Warn("auto renew failed, will retry", "key", key, "err", err)
					continue
				}

				if result == 0 {
					log.Warn("auto renew failed: lock lost or not owned", "key", key)
					return
				}
			}
		}
	})
}
