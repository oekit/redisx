package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
// 降级桶由库层按 key 缓存（#127：工厂对每个 key 真正只调一次），可并发使用。
type ResilientLimiter struct {
	dist  *PeriodLimit
	local func(key string) LocalLimiter
	// failMode 仅在构造期经 WithFailMode 写入，判定路径只读。
	failMode FailMode
	// onFallback 经 WithFallbackMetrics 写入，判定路径只读。
	onFallback func(Source)
	// localCache 是 per-key 兜底桶缓存（#127）：key → *bucketEntry。判定路径
	// 并发读写，entry.once 把工厂调用收敛为 per-key singleflight（#166，
	// 与 PeriodLimit.bucketFor 同一形态）——「每个 key 只建一个桶、工厂只调
	// 一次」；回切/Close 时清空（#167，防高基数 key 长故障窗口后常驻内存）。
	localCache sync.Map
	// fallbackSeen 标记本次降级周期内建过桶（#167）：Redis 恢复的首次判定
	// 用它做一次性的缓存清扫，常态（无降级）零开销。
	fallbackSeen atomic.Bool
}

// NewResilientLimiter 创建组合限流器。
//
// dist 为 Redis 侧判定（必填；nil 属编程错误，直接 panic——静默 nil 会让
// 每次判定都走降级路径，限流形同虚设且无报错）。**dist 必须保持 RescueOff**
// （构造期断言，redisx#173）：dist 配了 [WithRescue]（rescue != RescueOff）
// 时故障降级在 PeriodLimit.Take 内部先行完成（恒返回非 Unknown），本类型的
// FailMode——尤其 FailModeClosed 的 fail-closed 语义——被静默旁路，
// Decision.Source 恒报 SourceRedis（#118 建立的「故障 vs 限流」观测面失真），
// dist 内部还会拉起本类型文档声明「无」的探活后台 goroutine；叠配属编程
// 错误，构造期响亮拒绝（与 nil dist 同一口径）。故障降级口径由本类型单源：
// 要弹性外壳就别给 dist 配 rescue，反之亦然。
//
// local 为本地兜底桶工厂（可 nil：故障时按 FailMode 直接放行/拒绝），
// **每 key 只调用一次**（#127：结果由库层按 key 缓存、跨请求共享同一桶
// 实例——随用随建会让故障期间的每个请求都拿到全新满桶，限流静默失效）；
// 工厂返回 nil 视同未提供该 key 的本地桶，nil 结果**不缓存**（下次降级会
// 再调工厂，保持「按 key 动态提供/收回」的语义）；需要共享单桶时闭包忽略
// key 参数即可。
func NewResilientLimiter(dist *PeriodLimit, local func(key string) LocalLimiter, opts ...ResilientOption) *ResilientLimiter {
	if dist == nil {
		panic("ratelimit: dist must not be nil")
	}
	// redisx#173：rescue != RescueOff 的 dist 与本类型叠配时，故障降级在
	// dist 内部先行完成（Take 恒返回非 Unknown），本类型的 FailMode（尤其
	// FailModeClosed 的 fail-closed 语义）被静默旁路，Decision.Source 恒报
	// SourceRedis（观测失真）。构造期拦截，不静默。
	if m := dist.Rescue(); m != RescueOff {
		panic(fmt.Sprintf("ratelimit: dist must be built with RescueOff (got rescue mode %d): WithRescue degrades inside PeriodLimit.Take and silently bypasses ResilientLimiter's FailMode while Decision.Source misreports redis (redisx#173)", int(m)))
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
// 为拒绝（配额拒绝与故障降级经 Source 区分）；err 为 nil。恢复后的首次
// 判定清空本类型的降级桶缓存（#167：Redis 健康后进程内桶失去存在意义，
// 高基数 key 不随一次长故障窗口常驻内存；常态无降级时零开销）。
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
	case Allowed, HitQuota, OverQuota:
		if l.fallbackSeen.CompareAndSwap(true, false) {
			l.clearLocalCache()
		}
		return Decision{Allowed: code != OverQuota, Source: SourceRedis}, nil
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
		l.fallbackSeen.Store(true) // #167：本降级周期建过桶，回切时清扫
		if lim := l.localFor(key); lim != nil {
			return l.fallback(Decision{
				Allowed: lim.AllowN(time.Now(), 1),
				Source:  SourceLocalFallback,
			})
		}
	}
	return l.fallback(Decision{Allowed: true, Source: SourceFailOpen})
}

// localFor 返回 key 对应的本地兜底桶，工厂结果按 key 缓存（#127）：每个 key
// 只调一次工厂、跨请求共享同一桶实例——工厂返回新实例≠新桶，随用随建会让
// Redis 故障期间的每个请求都拿到全新满桶，本地限流 100% 静默失效。
// 工厂调用经 entry.once 收敛为 per-key singleflight（#166，与
// PeriodLimit.bucketFor 同一形态）：并发首次降级不再重复调用工厂。
// 工厂返回 nil 不缓存（保持「未提供该 key 的桶」的可动态语义）。
func (l *ResilientLimiter) localFor(key string) LocalLimiter {
	if v, ok := l.localCache.Load(key); ok {
		return v.(*bucketEntry).get()
	}
	build := func() LocalLimiter { return l.local(key) }
	// Load miss 与 LoadOrStore 之间无同步也安全：落选者的 entry 因 once 未
	// 触发而从未调用工厂，零副作用、即弃。
	e := &bucketEntry{build: build}
	actual, _ := l.localCache.LoadOrStore(key, e)
	lim := actual.(*bucketEntry).get()
	if lim == nil {
		// 工厂返回 nil：该 key 的「无桶」结论不缓存。仅当缓存仍指向本条目
		// 时删除（并发窗口里可能已有更新条目，不可误删）。
		if cur, ok := l.localCache.Load(key); ok && cur == actual {
			l.localCache.Delete(key)
		}
	}
	return lim
}

// clearLocalCache 清空 per-key 兜底桶缓存（#167）。
func (l *ResilientLimiter) clearLocalCache() {
	l.localCache.Range(func(k, _ any) bool {
		l.localCache.Delete(k)
		return true
	})
}

// Close 清空本类型的降级桶缓存（#167）并级联停掉底层 [PeriodLimit] 的探活
// watchdog（#165），幂等。与 [PeriodLimit.Close] 同口径：Close 后 Redis 故障
// 不再自动回切，判定口径不变；宿主不应再使用已 Close 的实例。
func (l *ResilientLimiter) Close(ctx context.Context) error {
	if l.fallbackSeen.CompareAndSwap(true, false) {
		l.clearLocalCache()
	}
	return l.dist.Close(ctx)
}

// fallback 收口降级决策：观测钩子在此统一触发。
func (l *ResilientLimiter) fallback(d Decision) Decision {
	if l.onFallback != nil {
		l.onFallback(d.Source)
	}
	return d
}
