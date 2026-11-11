// Package ratelimit 提供基于 Redis 的分布式限流器。
//
// 包含三种限流器，Lua 脚本与三态返回语义忠实移植 go-zero core/limit
// （periodscript.lua / tokenscript.lua），可与其混部共用同一份 Redis 配额：
//
//   - [PeriodLimit]：固定窗口计数限流（INCRBY + EXPIRE 原子脚本）。
//     Take 返回三态（[Allowed] / [HitQuota] / [OverQuota]）；可选 rescue
//     降级（[WithRescue]，#125）——Redis 故障按 [RescueMode] 给出最终口径
//     （fail-open / 进程内有界桶），探活回切与 TokenLimiter 同源。
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
// # 固定窗口降级 rescue（#125）
//
// 默认（RescueOff）Redis 故障时 Take 直接返回 (Unknown, err)——固定窗口计数
// 是跨实例的全局精确配额，故障窗口内进程无法在不破坏全局限额的前提下本地
// 近似，放行与否由调用方业务决定。需要内置降级时用 [WithRescue] 按档位
// 单源配置（不再每个下游手搓 facade）：
//
//	l := ratelimit.NewPeriodLimit(time.Hour, 5, rdb, "sms:verify",
//	    ratelimit.WithRescue(ratelimit.RescueBoundedLocal), // 故障降级到进程内有界桶
//	    // ratelimit.WithRescueBucket(func(key string) ratelimit.LocalLimiter { ... }), // 可选自定义兜底桶
//	)
//	code, err := l.Take(ctx, "13800000000") // 故障期间降级口径与 Redis 判定同形：(Allowed|OverQuota, nil)
//
// 三档语义：[RescueOff]（默认，不降级）；[RescueFailOpen]（故障一律放行，
// 限流失效）；[RescueBoundedLocal]（每 key 一只进程内有界桶，缺省由
// quota/period 推导等效令牌桶，[WithRescueBucket] 可自定义——桶在首次降级
// 创建并跨请求共享，规避「随用随建 = 每个请求都拿到刚初始化的满桶」的坑）。
// 有界降级的诚实口径：降级窗口内各进程各持一只等参桶，全局配额被放大为
// 「实例数 × 桶容量」；恢复后回到跨实例精确计数。降级期间由后台探活
// （[WithRescuePingInterval]/[WithRescueProbeTimeout]，与 TokenLimiter 同源
// 机制）监控 Redis，成功一次即自动回切。降级判定与 Redis 判定在返回码上
// 不可区分（与 TokenLimiter 兜底同口径）——需要**显式观测**「当前在 Redis
// 限还是在本地限」的用 [ResilientLimiter]（[Decision.Source]）。
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
// # 生命周期（#165/#167/#168）
//
//   - 构造期 fail-fast：[NewPeriodLimit]/[NewTokenLimiter] 对 quota/rate/burst/
//     keyPrefix 非 nil store 逐项校验，非法值直接 panic（nil store 历史上
//     不拦，故障延迟到首次判定的 nil interface panic，#168）；
//   - 探活 watchdog 停机口：开启降级/兜底的限流器提供 Close(ctx) error
//     （[PeriodLimit.Close]/[TokenLimiter.Close]/[ResilientLimiter.Close]，
//     sync.Once 幂等）——Redis 永久不可达且限流器被宿主丢弃时，探活
//     goroutine 随 Close 退出，不再永久轮询；Close 后故障不再自动回切；
//   - 降级桶缓存随回切/Close 清理：探活成功回切或 Close 时清空进程内
//     降级桶缓存（#167），高基数 key（per-user/per-token）不随一次长故障
//     窗口沉淀全量条目常驻内存；
//   - 降级桶工厂 per-key singleflight（#166）：并发首次降级只调一次工厂，
//     与「每 key 只建一个桶」的承诺由实现保证而非注释。
//
// # 设计边界
//
//   - ctx-only API：所有判定方法首参收 context.Context，无历史包袱
//   - 零可观测性注入：不内置 metrics/logx，故障切换对调用方透明；需要
//     观测时在业务层包装判定结果
//   - 单机限流请直接用 golang.org/x/time/rate，本包解决的是跨实例全局配额
package ratelimit
