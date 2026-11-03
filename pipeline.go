package redisx

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Pipeline 是 redis.Pipeliner 的键前缀代理：管道上排队的命令与单发命令同口径，
// 所有键经 convertKey 应用 key 前缀。
//
// 为什么需要（redisx#79）：此前 Pipeline()/TxPipeline() 直接返回裸
// redis.Pipeliner，管道命令不经 convertKey —— 配了 key 前缀的部署里，
// 同一逻辑键在「管道」与「单发」两条路径落到不同物理键（缓存击穿、计数分裂、
// 锁不住该锁的键），且两边各自自洽、静默无报错。键转换是 Client 的既有承诺，
// 管道不该是例外。
//
// 覆盖面：与 Client 单发封装一一对应的全部键命令（string/key/hash/list/set/
// zset/Lua/Publish）。未覆盖的命令（MGet、Geo、Stream…）不会静默漏前缀——
// 本类型不暴露它们（编译期即拦截）；需要时用 Raw() 拿底层裸 Pipeliner
// （不做键转换，键名请自行处理），或在本仓补一层封装。
//
// GetDelCompat 的 <6.2 回退不提供管道版：unknown command 只在 Exec 时才暴露，
// 无法在排队期选择回退路径；老 Redis 请用单发 Client.GetDelCompat。
type Pipeline struct {
	client *Client
	pipe   redis.Pipeliner
}

// Raw 返回底层裸 redis.Pipeliner——不做任何键转换。
// 仅当 Pipeline 未覆盖所需命令时使用；键名（含前缀）由调用方自行负责。
func (p *Pipeline) Raw() redis.Pipeliner {
	return p.pipe
}

// Exec 执行管道中排队的全部命令，返回各命令的结果。
func (p *Pipeline) Exec(ctx context.Context) ([]redis.Cmder, error) {
	return p.pipe.Exec(ctx)
}

// Discard 丢弃管道中尚未执行的命令。
func (p *Pipeline) Discard() {
	p.pipe.Discard()
}

// Len 返回管道中排队的命令数量。
func (p *Pipeline) Len() int {
	return p.pipe.Len()
}

// ==================== String 操作 ====================

// Get 获取 key 的值。
func (p *Pipeline) Get(ctx context.Context, key string) *redis.StringCmd {
	return p.pipe.Get(ctx, p.client.convertKey(key))
}

// Set 设置 key 的值。
func (p *Pipeline) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	return p.pipe.Set(ctx, p.client.convertKey(key), value, expiration)
}

// SetNX 仅当 key 不存在时设置。
func (p *Pipeline) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	return p.pipe.SetNX(ctx, p.client.convertKey(key), value, expiration)
}

// GetDel 原子返回 key 的值并删除该键（Redis GETDEL，要求 Redis >= 6.2）。
// 与单发 Client.GetDel 同口径：convertKey 应用 key 前缀，缺失键回 redis.Nil。
func (p *Pipeline) GetDel(ctx context.Context, key string) *redis.StringCmd {
	return p.pipe.GetDel(ctx, p.client.convertKey(key))
}

// ==================== Key 操作 ====================

// Del 删除一个或多个 key。
func (p *Pipeline) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return p.pipe.Del(ctx, p.client.convertKeys(keys)...)
}

// Exists 检查 key 是否存在。
func (p *Pipeline) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return p.pipe.Exists(ctx, p.client.convertKeys(keys)...)
}

// Expire 设置 key 的过期时间。
func (p *Pipeline) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return p.pipe.Expire(ctx, p.client.convertKey(key), expiration)
}

// ==================== 计数器操作 ====================

// Incr 将 key 的值加 1。
func (p *Pipeline) Incr(ctx context.Context, key string) *redis.IntCmd {
	return p.pipe.Incr(ctx, p.client.convertKey(key))
}

// IncrBy 将 key 的值增加指定增量。
func (p *Pipeline) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return p.pipe.IncrBy(ctx, p.client.convertKey(key), value)
}

// Decr 将 key 的值减 1。
func (p *Pipeline) Decr(ctx context.Context, key string) *redis.IntCmd {
	return p.pipe.Decr(ctx, p.client.convertKey(key))
}

// DecrBy 将 key 的值减少指定减量。
func (p *Pipeline) DecrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return p.pipe.DecrBy(ctx, p.client.convertKey(key), value)
}

// ==================== Hash 操作 ====================

// HGet 获取 hash 中指定字段的值。
func (p *Pipeline) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	return p.pipe.HGet(ctx, p.client.convertKey(key), field)
}

// HSet 设置 hash 中的一个或多个字段。
func (p *Pipeline) HSet(ctx context.Context, key string, values ...any) *redis.IntCmd {
	return p.pipe.HSet(ctx, p.client.convertKey(key), values...)
}

// HDel 删除 hash 中的一个或多个字段。
func (p *Pipeline) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	return p.pipe.HDel(ctx, p.client.convertKey(key), fields...)
}

// HGetAll 获取 hash 中所有字段和值。
func (p *Pipeline) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	return p.pipe.HGetAll(ctx, p.client.convertKey(key))
}

// ==================== List 操作 ====================

// LPush 将一个或多个值插入列表头部。
func (p *Pipeline) LPush(ctx context.Context, key string, values ...any) *redis.IntCmd {
	return p.pipe.LPush(ctx, p.client.convertKey(key), values...)
}

// RPush 将一个或多个值插入列表尾部。
func (p *Pipeline) RPush(ctx context.Context, key string, values ...any) *redis.IntCmd {
	return p.pipe.RPush(ctx, p.client.convertKey(key), values...)
}

// LPop 移除并返回列表头部元素。
func (p *Pipeline) LPop(ctx context.Context, key string) *redis.StringCmd {
	return p.pipe.LPop(ctx, p.client.convertKey(key))
}

// RPop 移除并返回列表尾部元素。
func (p *Pipeline) RPop(ctx context.Context, key string) *redis.StringCmd {
	return p.pipe.RPop(ctx, p.client.convertKey(key))
}

// LRange 获取列表指定范围的元素。
func (p *Pipeline) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return p.pipe.LRange(ctx, p.client.convertKey(key), start, stop)
}

// LLen 获取列表长度。
func (p *Pipeline) LLen(ctx context.Context, key string) *redis.IntCmd {
	return p.pipe.LLen(ctx, p.client.convertKey(key))
}

// ==================== Set 操作 ====================

// SAdd 向集合添加一个或多个成员。
func (p *Pipeline) SAdd(ctx context.Context, key string, members ...any) *redis.IntCmd {
	return p.pipe.SAdd(ctx, p.client.convertKey(key), members...)
}

// SMembers 获取集合所有成员。
func (p *Pipeline) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	return p.pipe.SMembers(ctx, p.client.convertKey(key))
}

// SIsMember 判断成员是否在集合中。
func (p *Pipeline) SIsMember(ctx context.Context, key string, member any) *redis.BoolCmd {
	return p.pipe.SIsMember(ctx, p.client.convertKey(key), member)
}

// SRem 从集合移除一个或多个成员。
func (p *Pipeline) SRem(ctx context.Context, key string, members ...any) *redis.IntCmd {
	return p.pipe.SRem(ctx, p.client.convertKey(key), members...)
}

// ==================== Sorted Set 操作 ====================

// ZAdd 向有序集合添加一个或多个成员。
func (p *Pipeline) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	return p.pipe.ZAdd(ctx, p.client.convertKey(key), members...)
}

// ZRange 获取有序集合指定范围的成员。
func (p *Pipeline) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return p.pipe.ZRange(ctx, p.client.convertKey(key), start, stop)
}

// ZRangeWithScores 获取有序集合指定范围的成员（含分数）。
func (p *Pipeline) ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	return p.pipe.ZRangeWithScores(ctx, p.client.convertKey(key), start, stop)
}

// ZRem 从有序集合移除一个或多个成员。
func (p *Pipeline) ZRem(ctx context.Context, key string, members ...any) *redis.IntCmd {
	return p.pipe.ZRem(ctx, p.client.convertKey(key), members...)
}

// ZCard 获取有序集合的成员数。
func (p *Pipeline) ZCard(ctx context.Context, key string) *redis.IntCmd {
	return p.pipe.ZCard(ctx, p.client.convertKey(key))
}

// ==================== Lua 脚本 ====================

// Eval 执行 Lua 脚本。
func (p *Pipeline) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return p.pipe.Eval(ctx, script, p.client.convertKeys(keys), args...)
}

// EvalSha 通过 SHA1 执行已缓存的 Lua 脚本。
func (p *Pipeline) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	return p.pipe.EvalSha(ctx, sha1, p.client.convertKeys(keys), args...)
}

// EvalRO 执行只读 Lua 脚本。
func (p *Pipeline) EvalRO(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return p.pipe.EvalRO(ctx, script, p.client.convertKeys(keys), args...)
}

// EvalShaRO 通过 SHA1 执行已缓存的只读 Lua 脚本。
func (p *Pipeline) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	return p.pipe.EvalShaRO(ctx, sha1, p.client.convertKeys(keys), args...)
}

// ==================== Pub/Sub ====================

// Publish 向频道发布消息（频道名经 convertKey 套前缀，与单发 Client.Publish 同口径）。
func (p *Pipeline) Publish(ctx context.Context, channel string, message any) *redis.IntCmd {
	return p.pipe.Publish(ctx, p.client.convertKey(channel), message)
}
