package redisx

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// ==================== Sorted Set 操作 ====================

// ZAdd 向有序集合添加一个或多个成员。
func (c *Client) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	return c.client.ZAdd(ctx, c.convertKey(key), members...)
}

// ZRange 获取有序集合指定范围的成员。
func (c *Client) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return c.client.ZRange(ctx, c.convertKey(key), start, stop)
}

// ZRangeWithScores 获取有序集合指定范围的成员（含分数）。
func (c *Client) ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	return c.client.ZRangeWithScores(ctx, c.convertKey(key), start, stop)
}

// ZRem 从有序集合移除一个或多个成员。
func (c *Client) ZRem(ctx context.Context, key string, members ...any) *redis.IntCmd {
	return c.client.ZRem(ctx, c.convertKey(key), members...)
}

// ZCard 获取有序集合的成员数。
func (c *Client) ZCard(ctx context.Context, key string) *redis.IntCmd {
	return c.client.ZCard(ctx, c.convertKey(key))
}

// ==================== Pipeline ====================

// Pipeline 创建管道代理（批量发送命令，减少网络往返）。
//
// 与单发命令同口径：管道上排队的命令同样经 convertKey 应用 key 前缀。
// 此前直接返回裸 redis.Pipeliner，管道命令绕过前缀——配了前缀的部署里
// 同一逻辑键在「管道」与「单发」落到不同物理键，静默分叉（redisx#79）。
// 覆盖面与裸管道逃生舱见 Pipeline 类型文档。
func (c *Client) Pipeline() *Pipeline {
	return &Pipeline{client: c, pipe: c.client.Pipeline()}
}

// TxPipeline 创建事务管道（MULTI/EXEC），键转换口径与 Pipeline 相同。
func (c *Client) TxPipeline() *Pipeline {
	return &Pipeline{client: c, pipe: c.client.TxPipeline()}
}

// ==================== Pub/Sub ====================

// Subscribe 订阅一个或多个频道。
func (c *Client) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return c.client.Subscribe(ctx, c.convertKeys(channels)...)
}

// Publish 向频道发布消息。
func (c *Client) Publish(ctx context.Context, channel string, message any) *redis.IntCmd {
	return c.client.Publish(ctx, c.convertKey(channel), message)
}

// ==================== Lua 脚本 ====================

// Eval 执行 Lua 脚本。
func (c *Client) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return c.client.Eval(ctx, script, c.convertKeys(keys), args...)
}

// EvalSha 通过 SHA1 执行已缓存的 Lua 脚本。
func (c *Client) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	return c.client.EvalSha(ctx, sha1, c.convertKeys(keys), args...)
}

// EvalRO 执行只读 Lua 脚本。
func (c *Client) EvalRO(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return c.client.EvalRO(ctx, script, c.convertKeys(keys), args...)
}

// EvalShaRO 通过 SHA1 执行已缓存的只读 Lua 脚本。
func (c *Client) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	return c.client.EvalShaRO(ctx, sha1, c.convertKeys(keys), args...)
}

// ScriptExists 检查脚本是否已缓存。
func (c *Client) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	return c.client.ScriptExists(ctx, hashes...)
}

// ScriptLoad 将脚本加载到脚本缓存。
func (c *Client) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return c.client.ScriptLoad(ctx, script)
}
