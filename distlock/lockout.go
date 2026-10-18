package distlock

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// lockoutFailScript 原子「窗口修剪 → 记失败 → 达阈值置锁定标记」。
// KEYS[1]=失败窗口 ZSET KEYS[2]=锁定标记 KEYS[3]=事件序号
// ARGV[1]=now_ms ARGV[2]=window_ms ARGV[3]=threshold ARGV[4]=penalty_ms。
//   - 窗口修剪：score < now-window 的旧失败出窗（滑动窗口）；
//   - 事件 member = now_ms:seq（INCR 序号防同毫秒覆盖），窗口 ZSET 与序号
//     计数器 PEXPIRE = window（毫秒——EXPIRE 是秒，错配会让窗口 key 长生不老；
//     空闲 key 自然消亡，与并发 Lockout 的条目回收对齐）；
//   - count >= threshold 时 SET NX PX 置锁定标记——NX 保证**惩罚时长只按首次
//     触发计算**，后续失败不延长（与 concurrency/ratelimit.Lockout 语义一致）；
//     锁定标记过期后窗口内旧失败仍在，下一次 Fail 即重新锁定（重复违规者）。
var lockoutFailScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', tonumber(ARGV[1]) - tonumber(ARGV[2]))
local seq = redis.call('INCR', KEYS[3])
redis.call('PEXPIRE', KEYS[3], tonumber(ARGV[2]))
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[1] .. ':' .. seq)
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
local count = redis.call('ZCARD', KEYS[1])
if count >= tonumber(ARGV[3]) then
	redis.call('SET', KEYS[2], '1', 'PX', ARGV[4], 'NX')
end
return count
`)

// lockoutCheckScript 锁定标记剩余毫秒数（>0 = 锁定中）。
var lockoutCheckScript = redis.NewScript(`
local ttl = redis.call('PTTL', KEYS[2])
if ttl > 0 then
	return {1, ttl}
end
return {0, 0}
`)

// lockoutResetScript 幂等清零：DEL 失败窗口 + 锁定标记 + 事件序号。
var lockoutResetScript = redis.NewScript(`
return redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
`)

// defaultLockoutKeyPrefix 是 LockoutConfig.KeyPrefix 未设置时的缺省前缀。
const defaultLockoutKeyPrefix = "lockout:"

// LockoutConfig 是分布式 Lockout 的构造配置（redisx#6）。
type LockoutConfig struct {
	// KeyPrefix 为完整 Redis key 前缀（完整 key = KeyPrefix + 业务 key，
	// 如 "zeus:lockout:" + "user:42"）；空串用缺省 "lockout:"。
	KeyPrefix string
	// Threshold 为失败阈值：窗口内第 N 次失败触发锁定。
	Threshold int
	// Window 为失败计数窗口（滑动窗口，窗口外失败自然出窗）。
	Window time.Duration
	// Penalty 为锁定时长：触发后 key 进入拒绝态持续 M 时长；
	// **惩罚时长只按首次触发计算**，锁定期间的后续失败不延长；
	// 到期自动解除，窗口内旧失败仍在时下一次 Fail 即重新锁定（重复违规者）。
	Penalty time.Duration
}

// Lockout 是基于 Redis 的分布式防爆破锁（redisx#6）：与 concurrency/ratelimit
// 的进程内 Lockout（fail2ban 形态）成对——登录防爆破锁定在多实例部署下必须
// 跨实例共享计数（攻击者把失败尝试打散到不同实例即可绕过进程内锁），
// 本类型与 distlock.Semaphore（多实例共享配额）同为共享惩罚/配额语义。
//
// 数据模型：失败窗口 ZSET（member=now_ms:seq，score=失败时刻 Unix 毫秒，
// TTL=Window 空闲消亡）+ 锁定标记（SET PX Penalty，NX 不延长）+ 事件序号
// 计数器；Fail/Check/Reset 全部 Lua 原子执行。
//
// 典型装配（防爆破语义单源化）：Redis 本类型为主，concurrency/ratelimit.Lockout
// 为进程内降级层；Redis 故障时的取舍由调用方决定（本类型不内置降级——
// 防爆破语义降级放行会静默失效，nil client 一律报错，与 Semaphore 口径一致）。
//
// 时间基准：窗口判定用注入时钟（now，缺省 time.Now）的 Go 侧毫秒与 Redis 端
// score 对比，脚本原子执行，多实例只需近似同步；锁定标记 TTL 走 Redis 原生
// EXPIRE/PITL，无跨实例时钟问题。
type Lockout struct {
	client redis.UniversalClient
	prefix string
	cfg    LockoutConfig
	// now 可注入时钟：测试越过窗口验证出窗重计数，不真等。
	now func() time.Time
}

// NewLockout 创建分布式防爆破锁。Threshold >= 1、Window/Penalty > 0 为必填
// 语义参数，非法值在使用时报错（与 distlock 其余入口的 fail-fast 口径一致）。
func NewLockout(client redis.UniversalClient, cfg LockoutConfig) *Lockout {
	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = defaultLockoutKeyPrefix
	}
	return &Lockout{client: client, prefix: prefix, cfg: cfg, now: time.Now}
}

func (l *Lockout) keys(key string) []string {
	return []string{l.prefix + key, l.prefix + key + ":lock", l.prefix + key + ":seq"}
}

// validate 构造参数与运行条件的统一 fail-fast 校验。
func (l *Lockout) validate() error {
	if l.client == nil {
		return fmt.Errorf("distlock: lockout requires a non-nil redis client (no graceful degradation for brute-force semantics)")
	}
	if l.cfg.Threshold < 1 {
		return fmt.Errorf("distlock: lockout threshold must be positive, got %d", l.cfg.Threshold)
	}
	if l.cfg.Window <= 0 {
		return fmt.Errorf("distlock: lockout window must be positive, got %v", l.cfg.Window)
	}
	if l.cfg.Penalty <= 0 {
		return fmt.Errorf("distlock: lockout penalty must be positive, got %v", l.cfg.Penalty)
	}
	return nil
}

// Fail 记一次失败事件，返回当前窗口内的失败计数（达阈值时已完成锁定置位，
// locked 状态经 Check 查询）。Redis 故障返回 error。
func (l *Lockout) Fail(ctx context.Context, key string) (int64, error) {
	if key == "" {
		return 0, fmt.Errorf("distlock: lockout key must not be empty")
	}
	if err := l.validate(); err != nil {
		return 0, err
	}
	n, err := lockoutFailScript.Run(ctx, l.client, l.keys(key),
		l.now().UnixMilli(), l.cfg.Window.Milliseconds(), l.cfg.Threshold,
		l.cfg.Penalty.Milliseconds()).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis lockout fail failed: %w", err)
	}
	return n, nil
}

// Check 查询 key 是否处于锁定拒绝态：locked=true 时 remain 为剩余锁定时长
// （供对外 429 文案），未锁定返回 (false, 0, nil)。
func (l *Lockout) Check(ctx context.Context, key string) (bool, time.Duration, error) {
	if key == "" {
		return false, 0, fmt.Errorf("distlock: lockout key must not be empty")
	}
	if err := l.validate(); err != nil {
		return false, 0, err
	}
	res, err := lockoutCheckScript.Run(ctx, l.client, l.keys(key)).Result()
	if err != nil {
		return false, 0, fmt.Errorf("redis lockout check failed: %w", err)
	}
	vals, ok := res.([]any)
	if !ok || len(vals) != 2 {
		return false, 0, fmt.Errorf("redis lockout check: unexpected reply %v", res)
	}
	if v, ok := vals[0].(int64); ok && v == 1 {
		ttl, _ := vals[1].(int64)
		return true, time.Duration(ttl) * time.Millisecond, nil
	}
	return false, 0, nil
}

// Reset 清零 key 的失败计数并解除锁定（成功登录后调用；亦可用作管理员解封）。
// key 未登记为 no-op（DEL 幂等）。
func (l *Lockout) Reset(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("distlock: lockout key must not be empty")
	}
	if err := l.validate(); err != nil {
		return err
	}
	_, err := lockoutResetScript.Run(ctx, l.client, l.keys(key)).Result()
	if err != nil {
		return fmt.Errorf("redis lockout reset failed: %w", err)
	}
	return nil
}
