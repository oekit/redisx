package ratelimit

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	xrate "golang.org/x/time/rate"
)

// Take 判定三态。与 go-zero core/limit 同名语义对齐，取值一致。
const (
	// Unknown 表示状态未知：Redis 通信失败，或返回码无法识别。
	Unknown int = iota
	// Allowed 表示本次请求允许通过（配额尚未用完）。
	Allowed
	// HitQuota 表示本次请求恰好用完配额（本次仍放行）。
	HitQuota
	// OverQuota 表示已超出配额，本次拒绝。
	OverQuota
)

// Lua 脚本内部返回码（periodLimitLua 约定）。
const (
	internalOverQuota int64 = 0
	internalAllowed   int64 = 1
	internalHitQuota  int64 = 2
)

// ErrUnknownCode 表示 Redis 返回了无法识别的状态码（非预期应答类型或非约定值）。
var ErrUnknownCode = errors.New("ratelimit: unknown status code")

// RescueMode 决定 Redis 故障（Take 判定失败）时的降级档位（#125）——固定窗
// +弹性消费方不再手搓 facade，降级语义取值（fail-open / bounded-local）由
// 库侧配置单源，且降级桶在**首次降级时创建并跨请求共享**（每 key 一只），
// 规避「随用随建 = 故障期间每个请求都拿到刚初始化的满桶 = 限流失效」的坑。
type RescueMode int

const (
	// RescueOff（默认）不降级：Take 原样返回 (Unknown, err)，放行与否由调用方
	// 决定（fail-open / fail-closed 自理，与历史行为一致——要显式观测降级
	// 口径的用 [ResilientLimiter]）。
	RescueOff RescueMode = iota
	// RescueFailOpen 降级放行：Redis 故障一律返回 (Allowed, nil)——降级窗口内
	// 限流完全失效（无限流）。Redis 恢复后自动回切（探活监控）。
	RescueFailOpen
	// RescueBoundedLocal 降级到进程内有界桶（每 key 一只，构造参数由 quota/
	// period 推导或 [WithRescueBucket] 自定义）：Redis 故障期间按本地桶余量
	// 放行/拒绝，单实例有界而非直接放行。诚实口径：固定窗口计数无法在本地
	// 精确近似，降级窗口内各进程各持一只等参桶，全局配额被放大为
	// 「实例数 × 桶容量」；恢复后回到跨实例精确计数。
	RescueBoundedLocal
)

// PeriodLimit 是固定窗口分布式限流器：keyPrefix + ":" + key 维度的跨实例精确计数，
// 窗口由该 key 的首个请求开启（INCRBY 结果为 1 时设置 EXPIRE），窗口内计数单调递增。
//
// 可选 rescue 降级（[WithRescue]，#125）：Redis 故障时按 [RescueMode] 降级——
// fail-open 直接放行，或降级到进程内有界桶（每 key 一只、跨请求共享，探活
// 监控成功后自动回切，机制与 [TokenLimiter] 兜底同源）；默认不降级，Take
// 原样返回 (Unknown, err) 由调用方定夺。降级判定与 Redis 判定在返回码上
// 不可区分（与 TokenLimiter 兜底同口径）——需要显式观测「当前在 Redis 限
// 还是在本地限」的用 [ResilientLimiter]（[Decision].Source）。
type PeriodLimit struct {
	period    int64 // 窗口秒数
	quota     int64 // 窗口内配额
	store     redis.Scripter
	keyPrefix string
	align     bool // 是否对齐自然周期

	// rescue 降级配置（仅构造期经选项写入，判定路径只读）
	rescue       RescueMode
	rescueBucket func(key string) LocalLimiter
	// buckets 是降级桶缓存：key → LocalLimiter。首次降级按需创建并跨请求
	// 共享（#125 的构造期生命周期语义按 key 落地），判定路径并发读写。
	buckets sync.Map
	// 探活降级机制（pingInterval/probeTimeout/redisAlive/startMonitor 经嵌入
	// 字段透出，与 TokenLimiter 同源，#125）。
	*redisMonitor
}

// PeriodOption 配置 PeriodLimit 的可选参数。
type PeriodOption func(*PeriodLimit)

// WithPeriodAlign 将窗口对齐到自然周期（本地时区）。
// 例如 period=24h + quota=5 表示"每个自然日 5 个配额"（而非滚动 24h 窗口）；
// key 的过期时间设为距下一个自然周期边界的剩余秒数。
// 上游 go-zero Align() 的同名语义，按本仓 XxxOption 硬规则改名。
func WithPeriodAlign() PeriodOption {
	return func(l *PeriodLimit) { l.align = true }
}

// WithRescue 设置 Redis 故障降级档位（#125），默认 [RescueOff]（不降级）。
// 仅 RescueOff / RescueFailOpen / RescueBoundedLocal 生效，其它值忽略（保留默认）。
func WithRescue(m RescueMode) PeriodOption {
	return func(l *PeriodLimit) {
		switch m {
		case RescueOff, RescueFailOpen, RescueBoundedLocal:
			l.rescue = m
		}
	}
}

// WithRescueBucket 自定义 [RescueBoundedLocal] 的兜底桶工厂：每 key **首次
// 降级**调用一次，结果缓存复用（跨请求共享同一桶实例——工厂返回新实例≠新桶，
// 随用随建 = 限流失效）；返回 nil 视同未提供该 key 的桶，该 key 降级退化为
// fail-open，且 nil 结果不缓存（下次降级会再调工厂，保持按 key 动态提供/
// 收回的语义）。缺省由 quota/period 推导等效令牌桶（速率 quota/period 每秒、
// 容量 quota）。仅 RescueBoundedLocal 生效。
func WithRescueBucket(fn func(key string) LocalLimiter) PeriodOption {
	return func(l *PeriodLimit) { l.rescueBucket = fn }
}

// WithRescuePingInterval 设置降级期间 Redis 探活间隔，默认 100ms。
// d <= 0 时忽略（保留默认值）。与 [WithRescue] 配套，探活机制与
// [WithTokenPingInterval] 同源。
func WithRescuePingInterval(d time.Duration) PeriodOption {
	return func(l *PeriodLimit) {
		if d > 0 {
			l.pingInterval = d
		}
	}
}

// WithRescueProbeTimeout 设置单次探活的超时，默认 1s。与探活间隔解耦：
// 须大于部署环境的正常 EVAL RT（如跨 region 常见几十到上百毫秒），否则
// 每次探活都会超时误判为故障，降级永不回切。d <= 0 时忽略（保留默认值）。
// 与 [WithRescue] 配套，探活机制与 [WithTokenProbeTimeout] 同源。
func WithRescueProbeTimeout(d time.Duration) PeriodOption {
	return func(l *PeriodLimit) {
		if d > 0 {
			l.probeTimeout = d
		}
	}
}

// NewPeriodLimit 创建固定窗口限流器。
//
// period 为窗口时长（按秒取整，不足 1s 按 1s）；quota 为窗口内配额（必须 > 0）；
// store 为任意 go-redis v9 Scripter 实现（Client/ClusterClient/Ring）；
// keyPrefix 为 key 命名空间，最终 Redis key 为 "keyPrefix:key"（keyPrefix
// 为空时直接用 key）。同一 (keyPrefix, key) 的所有实例共享同一份配额。
//
// quota <= 0 属于编程错误，直接 panic（fail-fast，与 NewTokenLimiter 对
// rate/burst 的校验口径一致）——静默接受会让 Lua 的 current < limit /
// current == limit 均不成立，每次 Take 都 OverQuota，流量 100% 被拒且无报错（#92）。
func NewPeriodLimit(period time.Duration, quota int64, store redis.Scripter, keyPrefix string, opts ...PeriodOption) *PeriodLimit {
	if quota <= 0 {
		panic("ratelimit: quota must be positive")
	}
	window := int64(period / time.Second)
	if window < 1 {
		window = 1
	}
	l := &PeriodLimit{
		period:       window,
		quota:        quota,
		store:        store,
		keyPrefix:    keyPrefix,
		redisMonitor: newRedisMonitor(store),
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Take 请求一个配额，返回判定结果。
//
// 返回 (Allowed|HitQuota|OverQuota, nil) 表示 Redis 判定成功（或已按
// [WithRescue] 降级给出最终口径，见下）；返回 (Unknown, err) 表示 Redis
// 通信失败（含 ctx 取消/超时）或应答无法识别，此时**不消耗配额**。
// 本方法不阻塞（单次脚本往返）。
//
// 降级口径（#125，rescue != RescueOff 时生效；判定结果与 Redis 判定在
// 返回码上不可区分，与 TokenLimiter 兜底同口径）：
//   - Redis 故障（通信失败/应答无法识别；调用方 ctx 取消/超时**不算**故障，
//     仍返回 (Unknown, ctx err)）→ 探活监控拉起，本次及后续判定按档位：
//     [RescueFailOpen] 返回 (Allowed, nil)；[RescueBoundedLocal] 按该 key 的
//     进程内兜底桶余量返回 (Allowed|OverQuota, nil)；
//   - 探活成功自动回切 Redis，无粘滞状态；故障期间后续判定直接走降级
//     快路径（不再每次请求白付一次失败的往返）；
//   - 需要**可观测**的降级判定（Source 显式区分）用 [ResilientLimiter]。
func (l *PeriodLimit) Take(ctx context.Context, key string) (int, error) {
	// 降级快路径：探活已判 Redis 故障时直接走本地降级口径（TokenLimiter
	// reserveN 同款），不再触碰 Redis。
	if l.rescue != RescueOff && !l.redisAlive.Load() {
		return l.takeRescued(key)
	}

	resp, err := periodScript.Run(ctx, l.store, []string{l.fullKey(key)}, []string{
		strconv.FormatInt(l.quota, 10),
		strconv.FormatInt(l.calcExpireSeconds(), 10),
	}).Result()
	if err != nil {
		// 调用方 ctx 取消/超时不是 Redis 故障：不触发降级，原样上抛。
		if l.rescue != RescueOff && !isCtxErr(err) {
			return l.takeRescued(key)
		}
		return Unknown, err
	}

	code, ok := resp.(int64)
	if !ok {
		// 应答无法识别：执行路径异常（代理损坏等），视同故障降级（与
		// TokenLimiter 对未知应答的口径一致）。
		if l.rescue != RescueOff {
			return l.takeRescued(key)
		}
		return Unknown, ErrUnknownCode
	}

	switch code {
	case internalOverQuota:
		return OverQuota, nil
	case internalAllowed:
		return Allowed, nil
	case internalHitQuota:
		return HitQuota, nil
	default:
		if l.rescue != RescueOff {
			return l.takeRescued(key)
		}
		return Unknown, ErrUnknownCode
	}
}

// takeRescued 降级判定（rescue != RescueOff 时 Redis 故障路径的唯一出口）：
// 先拉起探活监控（幂等），再按档位给出最终口径。返回的 (int, error) 恒为
// (非 Unknown, nil)——降级即「给出确定判定」，调用方不再需要处理 Unknown。
func (l *PeriodLimit) takeRescued(key string) (int, error) {
	l.startMonitor()
	switch l.rescue {
	case RescueFailOpen:
		return Allowed, nil
	case RescueBoundedLocal:
		if b := l.bucketFor(key); b != nil {
			if b.AllowN(time.Now(), 1) {
				return Allowed, nil
			}
			return OverQuota, nil
		}
		// 工厂未提供该 key 的桶：退化为 fail-open（可动态提供/收回）。
		return Allowed, nil
	default:
		// 不可达（Take 已保证 rescue != RescueOff），防御性返回 Unknown。
		return Unknown, ErrUnknownCode
	}
}

// bucketFor 返回 key 的进程内兜底桶：首次降级按需创建并缓存（sync.Map），
// 此后跨请求共享同一实例——Redis 故障期间每个请求都拿到全新满桶 = 限流失效
// （#125 点名的坑），缓存保证每个 key 只建一次。自定义工厂返回 nil 不缓存。
func (l *PeriodLimit) bucketFor(key string) LocalLimiter {
	if v, ok := l.buckets.Load(key); ok {
		return v.(LocalLimiter)
	}
	var b LocalLimiter
	if l.rescueBucket != nil {
		b = l.rescueBucket(key)
	} else {
		b = newRescueBucketFor(l.quota, l.period)
	}
	if b == nil {
		return nil
	}
	actual, _ := l.buckets.LoadOrStore(key, b)
	return actual.(LocalLimiter)
}

// newRescueBucketFor 由固定窗口参数推导等效令牌桶：平均速率 quota/period 每秒、
// 容量 quota（窗口配额全额可突发）。固定窗口计数无法在本地精确近似（见包
// 文档），这是「有界」而非「精确」：降级窗口内本地放行会放大全局配额，
// 恢复后回到跨实例精确计数。quota/period 恒为正（构造期已校验），无除零。
func newRescueBucketFor(quota, windowSec int64) *xrate.Limiter {
	return xrate.NewLimiter(xrate.Limit(quota)/xrate.Limit(windowSec), int(quota))
}

// calcExpireSeconds 计算本 key 的过期秒数。
// align 模式对齐自然周期边界（本地时区）：period - (unix+offset) % period；
// 普通模式即窗口本身（首个请求起算）。
func (l *PeriodLimit) calcExpireSeconds() int64 {
	if l.align {
		now := time.Now()
		_, offset := now.Zone()
		unix := now.Unix() + int64(offset)
		return l.period - unix%l.period
	}
	return l.period
}

// fullKey 拼接最终 Redis key：keyPrefix + ":" + key；keyPrefix 为空时用 key。
func (l *PeriodLimit) fullKey(key string) string {
	if l.keyPrefix == "" {
		return key
	}
	return l.keyPrefix + ":" + key
}
