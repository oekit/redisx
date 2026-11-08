// Package ratelimit 提供基于 Redis 的分布式限流器。
//
// 包含三种限流器，Lua 脚本与三态返回语义忠实移植 go-zero core/limit
// （periodscript.lua / tokenscript.lua），可与其混部共用同一份 Redis 配额：
//
//   - [PeriodLimit]：固定窗口计数限流（INCRBY + EXPIRE 原子脚本）。
//     Take 返回三态（[Allowed] / [HitQuota] / [OverQuota]）。
//   - [TokenLimiter]：令牌桶限流（双 key 惰性回补）。
//     Allow/AllowN 返回 bool；可选进程内兜底（[WithTokenFallback]）。
//   - [ResilientLimiter]（#118）：[PeriodLimit] 的「带兜底」组合——Redis
//     分布式固定窗口 + Redis 故障时本地令牌桶降级（[LocalLimiter]）+
//     三态 Unknown 的 fail-open/fail-closed 口径单源（[FailMode]），
//     判定依据以 [Decision.Source] 显式上报（[WithFallbackMetrics]）。
//
// # 存储抽象
//
// 构造器只依赖 go-redis v9 的 redis.Scripter 接口（Client / ClusterClient /
// Ring 均已实现），不绑定具体客户端类型，便于单测注入 mock。
//
// key 布局：
//
//   - PeriodLimit：keyPrefix + ":" + key（keyPrefix 为空时直接用 key）
//   - TokenLimiter：桶标识即 keyPrefix，实际 key 为 "{keyPrefix}.tokens" /
//     "{keyPrefix}.ts"，花括号为 Redis Cluster hash-tag，保证同桶两 key
//     落在同一 slot（沿用 go-zero 惯例）
//
// # 固定窗口 PeriodLimit
//
//	l := ratelimit.NewPeriodLimit(time.Hour, 5, rdb, "sms:verify")
//	switch code, err := l.Take(ctx, "13800000000"); {
//	case err != nil:
//	    // Redis 故障：返回 Unknown + err，由调用方决定放行或拒绝
//	case code == ratelimit.OverQuota:
//	    // 拒绝
//	default:
//	    // 放行（HitQuota 表示恰好用完配额，本次仍放行）
//	}
//
// 可用 [WithPeriodAlign] 把窗口对齐到自然周期（本地时区的天/小时边界），
// 典型场景"每天 5 条短信"。
//
// 与 TokenLimiter 不同，PeriodLimit **不做**本地兜底：固定窗口计数是跨实例的
// 全局精确配额，故障窗口内进程无法在不破坏全局限额的前提下本地近似——本地
// 放行多少都会放大全局限额。因此 Redis 故障时 Take 直接返回 (Unknown, err)，
// fail-open 还是 fail-closed 由调用方业务决定。要把「降级到本地令牌桶 +
// fail 口径 + 降级可观测」这套组合口径单源化（不再每个下游各写一遍），直接
// 用 [ResilientLimiter]——Redis 路径完全复用 PeriodLimit（同一脚本、同一
// key 布局），其语义约束（本地桶不破坏全局限额的诚实说明）见下节。
//
// # 组合原语 ResilientLimiter（#118）
//
//	l := ratelimit.NewResilientLimiter(
//	    ratelimit.NewPeriodLimit(time.Minute, 100, rdb, "api"),
//	    func(key string) ratelimit.LocalLimiter { return perKeyBucket(key) },
//	    ratelimit.WithFailMode(ratelimit.FailModeOpen),          // 默认
//	    ratelimit.WithFallbackMetrics(func(s ratelimit.Source) { /* metrics */ }),
//	)
//	d, err := l.Allow(ctx, "user:42") // d.Source: redis / local-fallback / fail-open / fail-closed
//
// Redis 正常时即 PeriodLimit 语义；故障（Unknown，含 ctx 超时/取消）时按
// [FailMode] 降级：默认 fail-open——有本地桶按本地桶余量放行/拒绝
// （[SourceLocalFallback]，每 key 独立、限速语义仍在，仅失去跨实例全局
// 精确性——与 TokenLimiter 兜底同一诚实口径），无桶直接放行
// （[SourceFailOpen]）；[FailModeClosed] 一律拒绝（[SourceFailClosed]），
// 适合超配额后果不可逆的业务。每次 Allow 都先试 Redis（无探活后台
// goroutine），Redis 恢复后下一次判定自动切回 [SourceRedis]，不会永久钉在
// 本地兜底；故障期间每次请求多付一次失败的往返，由调用方 ctx 超时兜底。
// 三态 Unknown 与「配额拒绝」经 Source 严格区分——监控不会把 Redis 故障
// 算成攻击流量。
//
// # 令牌桶 TokenLimiter
//
//	l := ratelimit.NewTokenLimiter(100, 200, rdb, "api:ingress",
//	    ratelimit.WithTokenFallback())
//	if l.Allow(ctx) {
//	    // 放行
//	}
//
// 默认（未开启兜底）Redis 故障时一律拒绝（fail-closed）。开启
// [WithTokenFallback] 后，Redis 故障降级到进程内 golang.org/x/time/rate
// 兜底限流（同 rate/burst 参数），后台 goroutine 按 [WithTokenPingInterval]
// （默认 100ms）周期探活，成功一次即自动回切 Redis；探活期间兜底持续生效。
// 单次探活的超时与间隔解耦（[WithTokenProbeTimeout]，默认 1s）：须大于部署
// 环境的正常 EVAL RT（如跨 region），否则探测会因超时误判故障、降级永不回切。
// 兜底参数与 Redis 桶一致，故障窗口内的实际放行速率不变，仅失去跨实例全局
// 精确性（各进程各自持有一个等参本地桶）。
//
// # 设计边界
//
//   - ctx-only API：所有判定方法首参收 context.Context，无历史包袱
//   - 零可观测性注入：不内置 metrics/logx，故障切换对调用方透明；需要
//     观测时在业务层包装判定结果
//   - 单机限流请直接用 golang.org/x/time/rate，本包解决的是跨实例全局配额
package ratelimit
