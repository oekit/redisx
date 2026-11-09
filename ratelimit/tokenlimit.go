package ratelimit

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	xrate "golang.org/x/time/rate"
)

// TokenLimiter 是基于 Redis 的分布式令牌桶限流器：以 keyPrefix 标识的
// 全局共享令牌桶（跨实例统一放行速率），Lua 脚本原子完成惰性回补 + 扣减。
//
// 可选进程内兜底（WithTokenFallback）：Redis 故障时降级到本地 golang.org/x/time/rate
// 桶（同 rate/burst），后台周期探活成功后自动回切；未开启兜底时故障一律拒绝。
// 探活/回切/幂等防重拉机制与 PeriodLimit 的 rescue（#125）同源（redisMonitor）。
//
// 面向"超出速率即丢弃"的场景（限流降级）；需要排队/等待请用 golang.org/x/time/rate
// 的 Reserve/Wait，本包不提供。
type TokenLimiter struct {
	rate  int // 每秒回补令牌数
	burst int // 桶容量（最大突发）

	store        redis.Scripter
	tokenKey     string // "{keyPrefix}.tokens"
	timestampKey string // "{keyPrefix}.ts"

	// 兜底开关 + 探活降级机制（pingInterval/probeTimeout/redisAlive/
	// startMonitor 经嵌入字段透出，WithTokenPingInterval/WithTokenProbeTimeout
	// 直写透出字段）。
	fallbackEnabled bool
	*redisMonitor

	rescueLimiter *xrate.Limiter
}

// TokenOption 配置 TokenLimiter 的可选参数。
type TokenOption func(*TokenLimiter)

// WithTokenFallback 开启 Redis 故障本地兜底：Eval 脚本失败（连接失败、超时、
// 应答异常等；调用方 ctx 取消不算）时，本次及后续判定降级到进程内
// golang.org/x/time/rate 桶（同 rate/burst），并启动后台探活，成功一次即
// 自动回切 Redis。未开启时 Redis 故障一律拒绝（fail-closed）。
func WithTokenFallback() TokenOption {
	return func(l *TokenLimiter) { l.fallbackEnabled = true }
}

// WithTokenPingInterval 设置兜底期间的 Redis 探活间隔，默认 100ms。
// d <= 0 时回退默认值。
func WithTokenPingInterval(d time.Duration) TokenOption {
	return func(l *TokenLimiter) {
		if d > 0 {
			l.pingInterval = d
		}
	}
}

// WithTokenProbeTimeout 设置单次探活的超时，默认 1s。与探活间隔
// （WithTokenPingInterval）解耦：间隔决定"多久探一次"，超时决定"单次探测
// 允许等多久"。超时须大于部署环境的正常 EVAL RT（如跨 region 常见几十到
// 上百毫秒）——若超时小于 RT，每次探活都会超时误判为故障，降级永不回切。
// d <= 0 时回退默认值。
func WithTokenProbeTimeout(d time.Duration) TokenOption {
	return func(l *TokenLimiter) {
		if d > 0 {
			l.probeTimeout = d
		}
	}
}

// NewTokenLimiter 创建分布式令牌桶限流器。
//
// rate 为每秒回补令牌数，burst 为桶容量（两者必须 > 0）；store 为任意
// go-redis v9 Scripter 实现；keyPrefix 即桶标识（必须非空）——同一标识的
// 所有实例共享同一个 Redis 令牌桶。实际 key 为 "{keyPrefix}.tokens" 与
// "{keyPrefix}.ts"，花括号为 Redis Cluster hash-tag，保证两 key 同 slot
// （沿用 go-zero 惯例）。
//
// rate/burst 非法或 keyPrefix 为空属于编程错误，直接 panic。
func NewTokenLimiter(rate, burst int, store redis.Scripter, keyPrefix string, opts ...TokenOption) *TokenLimiter {
	if rate <= 0 || burst <= 0 {
		panic("ratelimit: rate and burst must be positive")
	}
	if keyPrefix == "" {
		panic("ratelimit: keyPrefix must not be empty")
	}

	l := &TokenLimiter{
		rate:          rate,
		burst:         burst,
		store:         store,
		tokenKey:      "{" + keyPrefix + "}.tokens",
		timestampKey:  "{" + keyPrefix + "}.ts",
		redisMonitor:  newRedisMonitor(store),
		rescueLimiter: newRescueLimiter(rate, burst),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Allow 判定是否允许 1 个事件，等价 AllowN(ctx, 1)。
// 返回 false 表示超速丢弃；Redis 故障时行为见 WithTokenFallback。
func (l *TokenLimiter) Allow(ctx context.Context) bool {
	return l.AllowN(ctx, 1)
}

// AllowN 判定是否允许 n 个事件（n <= 0 视为 0，恒放行且不触碰 Redis）。
// 返回 false 表示超速丢弃；Redis 故障时行为见 WithTokenFallback。
// 本方法不阻塞（单次脚本往返）。
func (l *TokenLimiter) AllowN(ctx context.Context, n int) bool {
	if n <= 0 {
		return true
	}
	return l.reserveN(ctx, n)
}

// reserveN 单次判定：优先走 Redis 脚本；Redis 不可用且已开启兜底时走本地桶。
func (l *TokenLimiter) reserveN(ctx context.Context, n int) bool {
	if !l.redisAlive.Load() {
		return l.rescueLimiter.AllowN(time.Now(), n)
	}

	resp, err := tokenScript.Run(ctx, l.store, []string{l.tokenKey, l.timestampKey}, []string{
		strconv.Itoa(l.rate),
		strconv.Itoa(l.burst),
		strconv.FormatInt(time.Now().Unix(), 10),
		strconv.Itoa(n),
	}).Result()
	// Redis 拒绝 == false：Lua boolean false → nil 应答（go-redis 约定为 redis.Nil）
	if errors.Is(err, redis.Nil) {
		return false
	}
	// 调用方 ctx 取消/超时：不是 Redis 故障，不触发兜底，直接拒绝
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	if err != nil {
		return l.onRedisFailure(n)
	}

	code, ok := resp.(int64)
	if !ok {
		return l.onRedisFailure(n)
	}

	// Redis 允许 == true：Lua boolean true → 整数应答 1
	return code == 1
}

// onRedisFailure Redis 故障兜底入口：开启兜底则标记故障、启动探活并走本地桶；
// 否则 fail-closed 拒绝。
func (l *TokenLimiter) onRedisFailure(n int) bool {
	if !l.fallbackEnabled {
		return false
	}
	l.startMonitor()
	return l.rescueLimiter.AllowN(time.Now(), n)
}
