package redisx

import (
	"context"
	"errors"
	"time"

	pkgerrors "github.com/oekit/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// Counter 是分布式原子计数器。
// 基于 Redis INCR/DECR 实现，支持原子增减和过期。
type Counter struct {
	client *Client
	key    string
}

// NewCounter 创建分布式计数器。
func (c *Client) NewCounter(key string) *Counter {
	return &Counter{
		client: c,
		key:    key,
	}
}

// Incr 原子增加并返回新值。
func (ct *Counter) Incr(ctx context.Context, delta int64) (int64, error) {
	if delta == 1 {
		return ct.client.Incr(ctx, ct.key).Result()
	}
	return ct.client.IncrBy(ctx, ct.key, delta).Result()
}

// Decr 原子减少并返回新值。
// 注意：Redis DECR 允许结果为负数。如需下界保护，使用 DecrFloor。
func (ct *Counter) Decr(ctx context.Context, delta int64) (int64, error) {
	if delta == 1 {
		return ct.client.Decr(ctx, ct.key).Result()
	}
	return ct.client.DecrBy(ctx, ct.key, delta).Result()
}

// decrFloorLua 原子减少，结果不低于 floor。
// 钳底回写使用 SET ... KEEPTTL：裸 SET 会清除 TTL，让经 IncrWithTTL
// 创建的配额键变成永不过期的永久计数器。
var decrFloorLua = redis.NewScript(`
local val = redis.call("DECRBY", KEYS[1], ARGV[1])
if val < tonumber(ARGV[2]) then
    redis.call("SET", KEYS[1], ARGV[2], "KEEPTTL")
    return tonumber(ARGV[2])
end
return val
`)

// DecrFloor 原子减少并返回新值，结果不低于 floor（默认 0）。
// 适用于配额、限流等不允许负数的场景。
func (ct *Counter) DecrFloor(ctx context.Context, delta int64, floor int64) (int64, error) {
	// Lua 脚本走裸 client，key 必须显式过 convertKey，否则与 Get/Incr 等
	// 前缀感知路径操作不同的物理 key（配额读写分裂）。
	val, err := decrFloorLua.Run(ctx, ct.client.client, []string{ct.client.convertKey(ct.key)}, delta, floor).Int64()
	if err != nil {
		return 0, pkgerrors.Wrapf(err, "decr floor")
	}
	return val, nil
}

// Get 获取当前值。key 不存在时返回 0。
func (ct *Counter) Get(ctx context.Context) (int64, error) {
	val, err := ct.client.Get(ctx, ct.key).Int64()
	if err != nil {
		if isRedisNil(err) {
			return 0, nil
		}
		return 0, err
	}
	return val, nil
}

// Reset 重置计数器（删除 key）。
func (ct *Counter) Reset(ctx context.Context) error {
	return ct.client.Del(ctx, ct.key).Err()
}

// incrWithTTLLua 原子增加并设置过期时间（仅 key 无 TTL 时设置）。
// 用 PEXPIRE 毫秒精度：EXPIRE 秒取整会把亚秒 TTL 截断为 0（Redis 立即删 key）。
var incrWithTTLLua = redis.NewScript(`
local val = redis.call("INCRBY", KEYS[1], ARGV[1])
local pttl = redis.call("PTTL", KEYS[1])
if pttl == -1 then
    redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return val
`)

// IncrWithTTL 原子增加并设置过期时间。
// 如果 key 已有 TTL，只增加不影响原有 TTL；如果 key 无 TTL，设置指定的 TTL。
func (ct *Counter) IncrWithTTL(ctx context.Context, delta int64, ttl time.Duration) (int64, error) {
	ms := ttl.Milliseconds()
	if ms <= 0 {
		return 0, pkgerrors.Newf("incr with ttl: ttl 必须为正（得到 %v）", ttl)
	}
	val, err := incrWithTTLLua.Run(ctx, ct.client.client, []string{ct.client.convertKey(ct.key)}, delta, ms).Int64()
	if err != nil {
		return 0, pkgerrors.Wrapf(err, "incr with ttl")
	}
	return val, nil
}

// isRedisNil 检查错误是否为 redis.Nil（key 不存在）。
func isRedisNil(err error) bool {
	return errors.Is(err, redis.Nil)
}
