package ratelimit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/async"
	xrate "golang.org/x/time/rate"

	"github.com/redis/go-redis/v9"
)

// defaultPingInterval 兜底期间 Redis 探活默认间隔（沿用 go-zero 同名默认值）。
const defaultPingInterval = 100 * time.Millisecond

// defaultProbeTimeout 单次探活默认超时（与探活间隔解耦，见 WithTokenProbeTimeout）。
// 上游探活无 sub-RT 死线，这里取 1s 同量级：足以覆盖跨 region 等高 RT 部署的
// 正常 EVAL 往返；若超时小于 RT，探活将全部超时误判、降级永不回切。
const defaultProbeTimeout = time.Second

// isCtxErr 调用方 ctx 取消/超时判定：不是 Redis 故障，不触发降级（调用方
// 自己的主动放弃不该把 limiter 打进降级态）。go-redis 会把 ctx 错误原样或
// 包装后返回，errors.Is 均可识别。
func isCtxErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// redisMonitor 是 TokenLimiter / PeriodLimit 共用的 Redis 探活降级机制（#125
// 从 TokenLimiter.startMonitor/waitForRedis/probe 提升为包级单源，两个限流
// 器的弹性口径不再各写一遍）：首个 Redis 故障触发降级——redisAlive 置 false
// （后续判定直接走本地口径，不再每次请求白付一次失败的往返）→ 后台
// goroutine 按 pingInterval 周期探活，成功一次即回切（redisAlive=true）并
// 结束监控；探活期间再次故障不会重复拉起监控（monitorStarted 幂等）。
type redisMonitor struct {
	store        redis.Scripter
	pingInterval time.Duration // 探活间隔
	probeTimeout time.Duration // 单次探活超时（与间隔解耦）

	// 运行态：redisAlive 无锁读（热路径），monitorStarted 仅在锁内迁移
	rescueLock     sync.Mutex
	redisAlive     atomic.Bool
	monitorStarted bool
}

// newRedisMonitor 构建探活监控（默认间隔/超时，Redis 视为存活）。
func newRedisMonitor(store redis.Scripter) *redisMonitor {
	m := &redisMonitor{
		store:        store,
		pingInterval: defaultPingInterval,
		probeTimeout: defaultProbeTimeout,
	}
	m.redisAlive.Store(true)
	return m
}

// startMonitor 启动 Redis 探活监控（幂等）。
//
// 首个 Redis 故障触发：置 redisAlive=false（后续判定直接走本地口径）→
// 后台 goroutine 按 pingInterval 周期探活，成功一次即回切并结束监控；
// 探活期间再次故障不会重复拉起监控（monitorStarted 幂等）。
// goroutine 经 async.GoSafe 启动，panic 被 recover（本仓硬规则）。
func (m *redisMonitor) startMonitor() {
	m.rescueLock.Lock()
	defer m.rescueLock.Unlock()

	if m.monitorStarted {
		return
	}
	m.monitorStarted = true
	m.redisAlive.Store(false)

	async.GoSafe(m.waitForRedis)
}

// waitForRedis 探活循环：先立即探活一次，之后按 pingInterval 周期进行，
// 成功一次即回切 Redis（redisAlive=true）并结束。退出时无论何种路径都
// 复位 monitorStarted，保证下一次故障能重新拉起监控。
func (m *redisMonitor) waitForRedis() {
	defer func() {
		m.rescueLock.Lock()
		m.monitorStarted = false
		m.rescueLock.Unlock()
	}()

	ticker := time.NewTicker(m.pingInterval)
	defer ticker.Stop()

	if m.probe() {
		m.redisAlive.Store(true)
		return
	}
	for range ticker.C {
		if m.probe() {
			m.redisAlive.Store(true)
			return
		}
	}
}

// probe 探测 Redis 可用性：执行只含常量返回的最小 Lua 脚本（scripts.go probeLua）。
// redis.Scripter 接口无 Ping，EVAL 常量脚本等价于"连通 + 脚本执行能力"探测。
// 探活以独立 ctx 进行（不受单次请求 ctx 影响），超时取 probeTimeout
// （默认 1s，与 pingInterval 解耦）——高 RT 部署下单次探测慢但成功仍算
// 存活；超时仅防挂死的连接把探活 goroutine 永久卡住。
// 除无错误外还校验应答形态：探活脚本恒返回整数 1，应答残缺（类型或值不符）
// 说明执行路径异常（代理损坏、不兼容实现），视同失败继续兜底。
func (m *redisMonitor) probe() bool {
	ctx, cancel := context.WithTimeout(context.Background(), m.probeTimeout)
	defer cancel()
	resp, err := probeScript.Run(ctx, m.store, nil).Result()
	if err != nil {
		return false
	}
	v, ok := resp.(int64)
	return ok && v == 1
}

// newRescueLimiter 构建与 Redis 桶同参数的进程内兜底限流器。
// rate 恒为正（NewTokenLimiter 已校验），不存在除零。
func newRescueLimiter(rate, burst int) *xrate.Limiter {
	return xrate.NewLimiter(xrate.Every(time.Second/time.Duration(rate)), burst)
}
