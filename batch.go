package redisx

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Batch 封装 Redis Pipeline 批量操作。
// 将多个命令打包发送，减少网络往返次数。
// Batch 自动处理 key 前缀转换（与 Pipeline 代理同口径）。
//
// Deprecated: Batch 是 [Pipeline] 的功能子集旧类型（redisx#161）——字段与
// Pipeline 完全同构，12 个方法全部被 Pipeline 覆盖且 convertKey 口径一致，
// Pipeline 额外覆盖 SetNX/GetDel/HDel/HGetAll/List/ZSet/Lua/Publish 等约
// 35 个命令；同一「前缀管道」抽象不再维护两套实现。本类型改为内部委托
// [Pipeline]（行为不变，void 返回签名保留兼容），新增能力只进 Pipeline；
// 新代码请直接使用 [Client.Pipeline] / [Client.TxPipeline]。
type Batch struct {
	p *Pipeline
}

// NewBatch 创建一个批量操作实例。
//
// Deprecated: 请改用 [Client.Pipeline]（见 [Batch] 的说明，redisx#161）。
func (c *Client) NewBatch() *Batch {
	return &Batch{p: c.Pipeline()}
}

// Set 批量 Set 操作。
func (b *Batch) Set(ctx context.Context, key string, value any, ttl time.Duration) {
	b.p.Set(ctx, key, value, ttl)
}

// Get 批量 Get 操作。
func (b *Batch) Get(ctx context.Context, key string) *redis.StringCmd {
	return b.p.Get(ctx, key)
}

// Del 批量删除。
func (b *Batch) Del(ctx context.Context, keys ...string) {
	b.p.Del(ctx, keys...)
}

// Incr 批量自增。
func (b *Batch) Incr(ctx context.Context, key string) *redis.IntCmd {
	return b.p.Incr(ctx, key)
}

// IncrBy 批量按增量自增。
func (b *Batch) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return b.p.IncrBy(ctx, key, value)
}

// HSet 批量 Hash Set。
func (b *Batch) HSet(ctx context.Context, key string, values ...any) {
	b.p.HSet(ctx, key, values...)
}

// HGet 批量 Hash Get。
func (b *Batch) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	return b.p.HGet(ctx, key, field)
}

// Expire 批量设置过期时间。
func (b *Batch) Expire(ctx context.Context, key string, ttl time.Duration) {
	b.p.Expire(ctx, key, ttl)
}

// Exists 批量检查 key 存在。
func (b *Batch) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return b.p.Exists(ctx, keys...)
}

// Exec 执行所有批量操作，返回所有命令的结果。
func (b *Batch) Exec(ctx context.Context) ([]redis.Cmder, error) {
	return b.p.Exec(ctx)
}

// Len 返回批量操作中待执行的命令数量。
func (b *Batch) Len() int {
	return b.p.Len()
}

// Discard 丢弃批量操作中的所有命令。
func (b *Batch) Discard() {
	b.p.Discard()
}
