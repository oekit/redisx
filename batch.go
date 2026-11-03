package redisx

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Batch 封装 Redis Pipeline 批量操作。
// 将多个命令打包发送，减少网络往返次数。
// Batch 自动处理 key 前缀转换（与 Pipeline 代理同口径）。
type Batch struct {
	client *Client
	pipe   redis.Pipeliner
}

// NewBatch 创建一个批量操作实例。
func (c *Client) NewBatch() *Batch {
	return &Batch{
		client: c,
		pipe:   c.client.Pipeline(),
	}
}

// Set 批量 Set 操作。
func (b *Batch) Set(ctx context.Context, key string, value any, ttl time.Duration) {
	b.pipe.Set(ctx, b.client.convertKey(key), value, ttl)
}

// Get 批量 Get 操作。
func (b *Batch) Get(ctx context.Context, key string) *redis.StringCmd {
	return b.pipe.Get(ctx, b.client.convertKey(key))
}

// Del 批量删除。
func (b *Batch) Del(ctx context.Context, keys ...string) {
	b.pipe.Del(ctx, b.client.convertKeys(keys)...)
}

// Incr 批量自增。
func (b *Batch) Incr(ctx context.Context, key string) *redis.IntCmd {
	return b.pipe.Incr(ctx, b.client.convertKey(key))
}

// IncrBy 批量按增量自增。
func (b *Batch) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return b.pipe.IncrBy(ctx, b.client.convertKey(key), value)
}

// HSet 批量 Hash Set。
func (b *Batch) HSet(ctx context.Context, key string, values ...any) {
	b.pipe.HSet(ctx, b.client.convertKey(key), values...)
}

// HGet 批量 Hash Get。
func (b *Batch) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	return b.pipe.HGet(ctx, b.client.convertKey(key), field)
}

// Expire 批量设置过期时间。
func (b *Batch) Expire(ctx context.Context, key string, ttl time.Duration) {
	b.pipe.Expire(ctx, b.client.convertKey(key), ttl)
}

// Exists 批量检查 key 存在。
func (b *Batch) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return b.pipe.Exists(ctx, b.client.convertKeys(keys)...)
}

// Exec 执行所有批量操作，返回所有命令的结果。
func (b *Batch) Exec(ctx context.Context) ([]redis.Cmder, error) {
	return b.pipe.Exec(ctx)
}

// Len 返回批量操作中待执行的命令数量。
func (b *Batch) Len() int {
	return b.pipe.Len()
}

// Discard 丢弃批量操作中的所有命令。
func (b *Batch) Discard() {
	b.pipe.Discard()
}
