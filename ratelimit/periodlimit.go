package ratelimit

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
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

// PeriodLimit 是固定窗口分布式限流器：keyPrefix + ":" + key 维度的跨实例精确计数，
// 窗口由该 key 的首个请求开启（INCRBY 结果为 1 时设置 EXPIRE），窗口内计数单调递增。
//
// 不做本地兜底：固定窗口计数无法在本地近似（见包文档），Redis 故障时
// Take 返回 (Unknown, err)，由调用方决定 fail-open 或 fail-closed。
type PeriodLimit struct {
	period    int64 // 窗口秒数
	quota     int64 // 窗口内配额
	store     redis.Scripter
	keyPrefix string
	align     bool // 是否对齐自然周期
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
		period:    window,
		quota:     quota,
		store:     store,
		keyPrefix: keyPrefix,
	}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// Take 请求一个配额，返回判定结果。
//
// 返回 (Allowed|HitQuota|OverQuota, nil) 表示 Redis 判定成功；
// 返回 (Unknown, err) 表示 Redis 通信失败（含 ctx 取消/超时）或应答无法识别，
// 此时**不消耗配额**，放行与否由调用方决定。本方法不阻塞（单次脚本往返）。
func (l *PeriodLimit) Take(ctx context.Context, key string) (int, error) {
	resp, err := periodScript.Run(ctx, l.store, []string{l.fullKey(key)}, []string{
		strconv.FormatInt(l.quota, 10),
		strconv.FormatInt(l.calcExpireSeconds(), 10),
	}).Result()
	if err != nil {
		return Unknown, err
	}

	code, ok := resp.(int64)
	if !ok {
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
		return Unknown, ErrUnknownCode
	}
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
