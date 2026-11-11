package distlock

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	concrl "github.com/oekit/concurrency/ratelimit"

	"github.com/redis/go-redis/v9"
)

// isCtxErr 调用方 ctx 取消/超时判定：不是 Redis 故障，不触发降级（调用方
// 自己的主动放弃不该把判定路由进进程内降级实现）。与 ratelimit 包对
// TokenLimiter/PeriodLimit rescue 的口径一致。
func isCtxErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// GuardConfig 是 BruteForceGuard 的构造配置：分布式 Lockout 与进程内降级实现
// 从同一组字段装配——阈值/窗口/锁定时长在库内保证双实现同源（#128），下游
// 不再各自拼装、各自配置两份参数（推错即「降级后阈值变形」类静默偏差）。
type GuardConfig struct {
	// KeyPrefix 为分布式侧完整 Redis key 前缀（完整 key = KeyPrefix + 业务
	// key）；空串用缺省 "lockout:"（与 Lockout 同缺省）。进程内实现无 key。
	KeyPrefix string
	// MaxFails 为失败阈值：窗口内第 N 次失败触发锁定（双实现同源）。
	MaxFails int
	// Window 为失败计数窗口（滑动窗口，窗口外失败自然出窗；双实现同源）。
	Window time.Duration
	// LockTTL 为锁定时长：触发后 key 进入拒绝态持续 M 时长，到期自动解除，
	// 后续失败不延长（双实现同源，与两个 Lockout 的语义一致）。
	LockTTL time.Duration
	// MemFallback 打开进程内降级：Redis 故障（通信失败/应答异常；调用方
	// ctx 取消/超时不算）时 Fail/Check/Reset 自动路由到进程内 Lockout——
	// 单实例有界而非整体不可用。关闭时 Redis 故障原样上抛（防爆破语义
	// 降级放行会静默失效，调用方显式取舍；与 Lockout/Semaphore 的
	// fail-fast 口径一致）。
	MemFallback bool
	// MemMaxEntries 是进程内降级层的容量护栏上限（redisx#157：防用户名
	// 枚举撑爆内存——降级层以业务 key（如 username）为键，攻击者枚举 key
	// 时每次 RecordFail 都新增一条目，无上限即内存无界增长）。0 = 不限
	// （兼容既有调用方，对齐 concrl.LockoutConfig.MaxEntries 缺省）；
	// >0 时表满丢新样本 fail-open（已登记 key 不受影响）。
	// 防爆破场景**建议显式设置**（如 100_000）：护栏语义是「宁可漏记一个
	// 攻击者，不把进程内存交给攻击者的枚举速度」。
	MemMaxEntries int
}

// BruteForceGuard 是「防爆破锁对」的一等组合口（#128）：分布式主实现
// （[Lockout]，三键 hash-tag ZSET 滑窗）+ 进程内降级实现
// （concurrency/ratelimit.Lockout，fail2ban 形态）+ Redis 故障时的双实现
// 路由，Fail/Check/Reset 对外统一——[Lockout] 注释里的「典型装配」由本类型
// 单源，下游不再整包手搓（装配知识：降级触发条件、双实现阈值一致性、key
// 口径统一都收进库内）。
//
// 降级时机（MemFallback 打开时）由库层按「失败计数」路由：每次调用都先试
// 分布式（无探活后台 goroutine），Redis 故障才降级 ⇒ Redis 恢复后下一次
// 调用自动切回分布式口径，不会永久钉在进程内实现（故障期间每次请求多付
// 一次失败的往返，由调用方 ctx 超时兜底，与 ratelimit.ResilientLimiter
// 同口径）。降级判定与分布式判定在返回值上同形且**不可区分**——需要观测
// 降级与否的调用方保持 MemFallback 关闭（错误原样上抛）。
//
// 形状说明：Fail 返回**本次失败后的锁定态**（bool）而非 Lockout.Fail 的窗口
// 计数——进程内实现只暴露锁定态（RecordFailure），返回计数必须在降级路径上
// 造假；余量时长经 Check 查询（对外 429 文案）。
type BruteForceGuard struct {
	client redis.UniversalClient
	cfg    GuardConfig
	dist   *Lockout

	// mem 进程内降级实现：懒构建（validate 通过后 concrl.NewLockout 不会
	// panic——非法配置在使用时已被 fail-fast 拦截，不把构造期 panic 推给
	// 调用方，与 distlock 其余入口的「使用时报错」口径一致）。
	memOnce sync.Once
	mem     *concrl.Lockout
	// memNow 可注入时钟（进程内降级实现的窗口/锁定判定；nil = time.Now），
	// 测试越过多窗口验证出窗/解锁不真等。
	memNow func() time.Time
}

// NewBruteForceGuard 创建防爆破锁对组合口。Threshold>=1、Window/LockTTL>0
// 为必填语义参数，非法值在使用时报错（与 distlock 其余入口的 fail-fast
// 口径一致；GuardConfig 的字段错误不会在构造期 panic）。
func NewBruteForceGuard(client redis.UniversalClient, cfg GuardConfig) *BruteForceGuard {
	return &BruteForceGuard{
		client: client,
		cfg:    cfg,
		dist:   NewLockout(client, LockoutConfig{KeyPrefix: cfg.KeyPrefix, Threshold: cfg.MaxFails, Window: cfg.Window, Penalty: cfg.LockTTL}),
	}
}

// validate 构造参数与运行条件的统一 fail-fast 校验（与 Lockout.validate 同口径，
// 双实现的语义参数同源校验）。
func (g *BruteForceGuard) validate() error {
	if g.client == nil {
		return fmt.Errorf("distlock: brute force guard requires a non-nil redis client (set MemFallback for in-process degradation, nil client is still a config error)")
	}
	if g.cfg.MaxFails < 1 {
		return fmt.Errorf("distlock: brute force guard max fails must be positive, got %d", g.cfg.MaxFails)
	}
	if g.cfg.Window <= 0 {
		return fmt.Errorf("distlock: brute force guard window must be positive, got %v", g.cfg.Window)
	}
	if g.cfg.LockTTL <= 0 {
		return fmt.Errorf("distlock: brute force guard lock ttl must be positive, got %v", g.cfg.LockTTL)
	}
	return nil
}

// memFor 返回进程内降级实现（懒构建，一次）。
func (g *BruteForceGuard) memFor() *concrl.Lockout {
	g.memOnce.Do(func() {
		g.mem = concrl.NewLockout(concrl.LockoutConfig{
			Window:       g.cfg.Window,
			Threshold:    g.cfg.MaxFails,
			LockDuration: g.cfg.LockTTL,
			MaxEntries:   g.cfg.MemMaxEntries, // redisx#157：容量护栏透传（0=不限）
			Now:          g.memNow,
		})
	})
	return g.mem
}

// Fail 记一次失败事件，返回本次失败后的锁定态（locked=true 表示 key 已进入
// 拒绝态；余量时长经 Check 查询，供对外 429 文案）。
//
// 分布式路径：窗口计数达阈值即锁定（惩罚时长只按首次触发计算，与 Lockout
// 语义一致）。MemFallback 打开时 Redis 故障自动降级到进程内实现（本次失败
// 记在进程内窗口，单实例有界）；关闭时故障原样上抛。调用方 ctx 取消/超时
// 不是 Redis 故障，不降级、原样上抛。
func (g *BruteForceGuard) Fail(ctx context.Context, key string) (bool, error) {
	if key == "" {
		return false, fmt.Errorf("distlock: lockout key must not be empty")
	}
	if err := g.validate(); err != nil {
		return false, err
	}
	n, err := g.dist.Fail(ctx, key)
	if err == nil {
		// 分布式脚本在 count >= threshold 时原子置位锁定标记 ⇒ 计数达阈值
		// 等价于「现在处于锁定态」（含重复违规者的重新锁定）。
		return n >= int64(g.cfg.MaxFails), nil
	}
	if isCtxErr(err) || !g.cfg.MemFallback {
		return false, err
	}
	locked, _ := g.memFor().RecordFailure(key)
	return locked, nil
}

// Check 查询 key 是否处于锁定拒绝态：locked=true 时 remain 为剩余锁定时长
// （供对外 429 文案），未锁定返回 (false, 0, nil)。
//
// MemFallback 打开时 Redis 故障自动降级到进程内实现（单实例视角的锁定态）；
// 关闭时故障原样上抛。
func (g *BruteForceGuard) Check(ctx context.Context, key string) (bool, time.Duration, error) {
	if key == "" {
		return false, 0, fmt.Errorf("distlock: lockout key must not be empty")
	}
	if err := g.validate(); err != nil {
		return false, 0, err
	}
	locked, remain, err := g.dist.Check(ctx, key)
	if err == nil {
		return locked, remain, nil
	}
	if isCtxErr(err) || !g.cfg.MemFallback {
		return false, 0, err
	}
	locked, remain = g.memFor().CheckLocked(key)
	return locked, remain, nil
}

// Reset 清零 key 的失败计数并解除锁定（成功登录后调用；亦可用作管理员解封）。
// key 未登记为 no-op（幂等）。
//
// 降级边界（MemFallback 打开且 Redis 故障时）：进程内状态保证清零；分布式
// 侧本次**不可达**（其窗口/锁定标记未清）——恢复后分布式侧旧状态仍按其自身
// 的 Window/LockTTL 自然消亡。需要强一致解封的调用方应在 Redis 恢复后重试
// Reset（幂等）；关闭 MemFallback 时故障原样上抛，无此边界。
func (g *BruteForceGuard) Reset(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("distlock: lockout key must not be empty")
	}
	if err := g.validate(); err != nil {
		return err
	}
	err := g.dist.Reset(ctx, key)
	if err == nil {
		return nil
	}
	if isCtxErr(err) || !g.cfg.MemFallback {
		return err
	}
	// 降级：清进程内状态，分布式侧待恢复后自然消亡（见方法注释的降级边界）。
	g.memFor().Reset(key)
	return nil
}
