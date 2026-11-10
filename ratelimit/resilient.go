package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Source 表示一次限流判定的**依据**（#118）：Redis 分布式判定，或 Redis
// 故障后的降级路径。可观测是本原语的存在理由之一——「现在到底在 Redis 限
// 还是在本地限」必须能进 metrics/日志：Redis 故障被静默当成正常限流、或
// 攻击流量被算进故障降级，两者都无法察觉（这正是下游手搓版最容易漏的一环）。
type Source int

const (
	// SourceRedis Redis 分布式判定（PeriodLimit.Take 正常应答：Allowed /
	// HitQuota / OverQuota 三态收敛为 Decision.Allowed）。
	SourceRedis Source = iota
	// SourceLocalFallback Redis 故障（Take 返回 Unknown），fail-open 模式且
	// 提供了本地桶：本次判定由本地令牌桶给出（Allowed 按桶的余量）。
	SourceLocalFallback
	// SourceFailOpen Redis 故障且无本地桶：fail-open 直接放行（不消耗任何配额）。
	SourceFailOpen
	// SourceFailClosed Redis 故障：fail-closed 一律拒绝（不消耗任何配额）。
	SourceFailClosed
)

// String 返回 Source 的稳定标识（metrics/日志用，勿改动既有取值）。
func (s Source) String() string {
	switch s {
	case SourceRedis:
		return "redis"
	case SourceLocalFallback:
		return "local-fallback"
	case SourceFailOpen:
		return "fail-open"
	case SourceFailClosed:
		return "fail-closed"
	default:
		return "unknown"
	}
}

// FailMode 决定 Redis 故障（Take 返回 Unknown，含 ctx 取消/超时）时的放行
// 口径。安全策略显式化、单源化——不再随部署漂移（#118 的动机）。
type FailMode int

const (
	// FailModeOpen 降级放行（默认）：有本地桶走本地桶（[SourceLocalFallback]，
	// 限速语义仍在，只是失去跨实例全局精确性）；无本地桶直接放行
	// （[SourceFailOpen]）。
	FailModeOpen FailMode = iota
	// FailModeClosed 严格口径：Redis 故障一律拒绝（[SourceFailClosed]），
	// 本地桶不参与。适合「超配额后果不可逆」的业务（支付/风控）。
	FailModeClosed
)

// Decision 是一次限流判定的结果：是否放行 + 判定依据。三态 Unknown 与
// 「拒绝」必须可区分（Source=SourceRedis 且 Allowed=false 才是配额拒绝；
// SourceFail* 表示 Redis 故障下的降级口径）——否则监控会把 Redis 故障算成
// 攻击流量。
type Decision struct {
	// Allowed 是否放行本次请求。
	Allowed bool
	// Source 判定依据（redis / local-fallback / fail-open / fail-closed）。
	Source Source
}

// LocalLimiter 是本地兜底限流器的最小接口：与 golang.org/x/time/rate.Limiter
// 的 AllowN 同形，*rate.Limiter 天然满足，无需适配层；进程内既有其他形状的
// 限流器包一层即可接入（如 concurrency/ratelimit.KeyedLimiterOf 的每 key 桶）。
// 实现必须并发安全。
type LocalLimiter interface {
	AllowN(now time.Time, n int) bool
}

// ResilientOption 配置 ResilientLimiter 的可选参数。
type ResilientOption func(*ResilientLimiter)

// WithFailMode 设置 Redis 故障时的放行口径，默认 [FailModeOpen]。
// 仅 FailModeOpen / FailModeClosed 生效，其它值忽略（保留默认）。
func WithFailMode(m FailMode) ResilientOption {
	return func(l *ResilientLimiter) {
		switch m {
		case FailModeOpen:
			l.failMode = FailModeOpen
		case FailModeClosed:
			l.failMode = FailModeClosed
		}
	}
}

// WithFallbackMetrics 注册降级观测钩子：每次**非 Redis** 判定（Source !=
// SourceRedis）以最终 Source 回调一次，供 metrics/日志接入——「现在到底在
// Redis 限还是在本地限」从此可观测。fn 可为 nil（不观测）。fn 会在多
// goroutine 并发触发，须自行保证并发安全。
func WithFallbackMetrics(fn func(Source)) ResilientOption {
	return func(l *ResilientLimiter) { l.onFallback = fn }
}

// ResilientLimiter 是「Redis 分布式固定窗口 + 本地令牌桶兜底」的组合原语
// （#118）：Redis 正常时走 [PeriodLimit] 的跨实例精确配额；Redis 故障
// （Take 返回 Unknown）时按 [FailMode] 降级——fail-open 用本地令牌桶
// （每 key 独立，限速语义仍在）或直接放行，fail-closed 一律拒绝。
// 三态 Unknown 的降级口径由此单源，下游不再各写一遍。
//
// 与本包其余原语的关系：
//   - [PeriodLimit]：只有分布式窗口与三态返回，故障口径交给调用方——本类型
//     是它的「带兜底」组合，Redis 路径完全复用（同一脚本/同一 key 布局）；
//   - [TokenLimiter]：全局单桶 + 内置探活回切，粒度是「每秒速率」；本类型
//     粒度是「每 key 固定窗口配额」，且把降级判定显式返回给调用方观测。
//
// 判定顺序：每次 Allow 都先尝试 Redis（无探活后台 goroutine），故障才降级
// ⇒ Redis 恢复后下一次调用自动切回 SourceRedis，不会永久钉在本地兜底
// （故障期间每次请求多付一次失败的往返，由调用方 ctx 超时兜底）。
// localOnce 的缓存写入是判定路径上唯一可变状态（sync.Map 并发安全），其余
// 字段构造后不变。可并发使用。
type ResilientLimiter struct {
	dist  *PeriodLimit
	local func(key string) LocalLimiter
	// failMode 仅在构造期经 WithFailMode 写入，判定路径只读。
	failMode FailMode
	// onFallback 经 WithFallbackMetrics 写入，判定路径只读。
	onFallback func(Source)
	// localOnce 按 key 缓存工厂结果（#127）：兑现「每 key 调用一次」文档
	// 契约——降级判定在每次 Unknown Allow 都会发生，不缓存则读文档的消费者
	// 「return xrate.NewLimiter(...)」在故障期每请求拿全新满桶，限流静默死亡
	// （#125 点名的形态）。nil 结果不缓存（见 localBucket）。
	localOnce sync.Map
}

// NewResilientLimiter 创建组合限流器。
//
// dist 为 Redis 侧判定（必填；nil 属编程错误，直接 panic——静默 nil 会让
// 每次判定都走降级路径，限流形同虚设且无报错）；local 为本地兜底桶工厂
// （可 nil：故障时按 FailMode 直接放行/拒绝），对每个 key **至多调用一次**
// 并缓存其结果（#127）；返回 nil 的 key 视同未提供本地桶，该结果不缓存
// （每次降级判定都会重问工厂，保持动态语义）；需要共享单桶时闭包忽略 key
// 参数即可。
func NewResilientLimiter(dist *PeriodLimit, local func(key string) LocalLimiter, opts ...ResilientOption) *ResilientLimiter {
	if dist == nil {
		panic("ratelimit: dist must not be nil")
	}
	l := &ResilientLimiter{
		dist:     dist,
		local:    local,
		failMode: FailModeOpen,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Allow 请求一个配额，返回判定与依据。
//
// Redis 正常：(SourceRedis, nil)——Allowed/HitQuota 收敛为放行，OverQuota
// 为拒绝（配额拒绝与故障降级经 Source 区分）；err 为 nil。
// Redis 故障（含 ctx 取消/超时——对本次请求而言「太慢」等同「挂了」）：
//   - FailModeClosed：(SourceFailClosed, 底层 err)，一律拒绝；
//   - FailModeOpen 且本地桶可用：(SourceLocalFallback, 底层 err)，按本地桶
//     余量放行/拒绝；
//   - FailModeOpen 且无本地桶：(SourceFailOpen, 底层 err)，直接放行。
//
// err 非 nil 时 Decision 仍给出降级后的最终口径，调用方可只看 Decision
// 做放行、只看 err 做告警。无论降级路径如何，Redis 侧配额都不被消耗。
// 本方法不阻塞（单次脚本往返 + 故障时一次本地桶判定）。
func (l *ResilientLimiter) Allow(ctx context.Context, key string) (Decision, error) {
	code, err := l.dist.Take(ctx, key)
	switch code {
	case Allowed, HitQuota:
		return Decision{Allowed: true, Source: SourceRedis}, nil
	case OverQuota:
		return Decision{Allowed: false, Source: SourceRedis}, nil
	default: // Unknown：Redis 通信失败或应答无法识别 → 降级口径单源（#118）
		d := l.decideFallback(key)
		return d, err
	}
}

// decideFallback 未知态的降级判定（唯一口径）：fail-closed 拒绝；fail-open
// 优先本地桶，无桶放行。每次非 Redis 判定触发一次 onFallback。
func (l *ResilientLimiter) decideFallback(key string) Decision {
	if l.failMode == FailModeClosed {
		return l.fallback(Decision{Allowed: false, Source: SourceFailClosed})
	}
	if l.local != nil {
		if lim := l.localBucket(key); lim != nil {
			return l.fallback(Decision{
				Allowed: lim.AllowN(time.Now(), 1),
				Source:  SourceLocalFallback,
			})
		}
	}
	return l.fallback(Decision{Allowed: true, Source: SourceFailOpen})
}

// localBucket 取 key 的本地兜底桶（#127）：工厂结果按 key 缓存，兑现
// 「每 key 至多调用一次」契约——decideFallback 在每次 Unknown Allow 都会
// 进来，不缓存则读文档/消费惯性的「随用随建」每请求拿全新满桶，限流静默
// 死亡（#125 点名的形态）。工厂返回 nil 的 key 不缓存（每次降级判定重问
// 工厂，保持「返回 nil 视同未提供」的动态语义）。
func (l *ResilientLimiter) localBucket(key string) LocalLimiter {
	if v, ok := l.localOnce.Load(key); ok {
		lim, _ := v.(LocalLimiter)
		return lim
	}
	lim := l.local(key)
	if lim != nil {
		l.localOnce.Store(key, lim)
	}
	return lim
}

// fallback 收口降级决策：观测钩子在此统一触发。
func (l *ResilientLimiter) fallback(d Decision) Decision {
	if l.onFallback != nil {
		l.onFallback(d.Source)
	}
	return d
}
