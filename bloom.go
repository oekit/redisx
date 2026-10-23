package redisx

import (
	"context"
	"fmt"
	"strings"
)

// BloomFilter 是基于 RedisBloom 的布隆过滤器封装。
// 需要 Redis 服务器安装 RedisBloom 模块。
type BloomFilter struct {
	client *Client
	key    string
}

// NewBloomFilter 创建布隆过滤器。
// 如果 key 不存在，会自动创建。
//
// ⚠️ key 已存在时**复用既有过滤器**：errorRate 与 capacity 以既有实例为准，
// 本次传入的参数不生效（Redis 的 BF.RESERVE 对已存在的 key 不覆盖）。
// 需要按新参数重建，请先 DEL 该 key。
//
// 参数:
//   - ctx: 上下文（支持超时/取消）
//   - key: 布隆过滤器的 key 名
//   - errorRate: 误判率（如 0.01 表示 1%）；key 已存在时不生效
//   - capacity: 预计元素数量；key 已存在时不生效
func (c *Client) NewBloomFilter(ctx context.Context, key string, errorRate float64, capacity int64) (*BloomFilter, error) {
	bf := &BloomFilter{
		client: c,
		key:    key,
	}

	// 尝试创建布隆过滤器（已存在时会返回错误，可忽略）
	err := c.client.Do(ctx, "BF.RESERVE", c.convertKey(key), errorRate, capacity).Err()
	if err != nil {
		// 如果是已存在的错误，忽略（使用 Contains 兼容不同 Redis 版本的错误消息）
		if !strings.Contains(err.Error(), "exists") {
			return nil, fmt.Errorf("create bloom filter: %w", err)
		}
	}

	return bf, nil
}

// Add 向布隆过滤器添加元素。
// 返回 true 表示元素可能是新添加的，false 表示元素已存在。
func (bf *BloomFilter) Add(ctx context.Context, item string) (bool, error) {
	return bf.client.client.Do(ctx, "BF.ADD", bf.client.convertKey(bf.key), item).Bool()
}

// Exists 检查元素是否可能存在。
// 返回 true 表示元素可能存在（有误判率），false 表示元素一定不存在。
func (bf *BloomFilter) Exists(ctx context.Context, item string) (bool, error) {
	return bf.client.client.Do(ctx, "BF.EXISTS", bf.client.convertKey(bf.key), item).Bool()
}

// MAdd 批量添加元素。
// 返回每个元素的结果：true 表示新添加，false 表示已存在。
func (bf *BloomFilter) MAdd(ctx context.Context, items ...string) ([]bool, error) {
	args := make([]any, 0, len(items)+2)
	args = append(args, "BF.MADD", bf.client.convertKey(bf.key))
	for _, item := range items {
		args = append(args, item)
	}

	cmd := bf.client.client.Do(ctx, args...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	results, err := cmd.BoolSlice()
	if err != nil {
		return nil, err
	}
	return results, nil
}

// MExists 批量检查元素是否存在。
func (bf *BloomFilter) MExists(ctx context.Context, items ...string) ([]bool, error) {
	args := make([]any, 0, len(items)+2)
	args = append(args, "BF.MEXISTS", bf.client.convertKey(bf.key))
	for _, item := range items {
		args = append(args, item)
	}

	cmd := bf.client.client.Do(ctx, args...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	results, err := cmd.BoolSlice()
	if err != nil {
		return nil, err
	}
	return results, nil
}

// Info 获取布隆过滤器信息。
// BF.INFO 返回交替的 key-value 数组，本方法将其解析为 map。
func (bf *BloomFilter) Info(ctx context.Context) (map[string]any, error) {
	cmd := bf.client.client.Do(ctx, "BF.INFO", bf.client.convertKey(bf.key))
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	val := cmd.Val()
	arr, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected BF.INFO response type: %T", val)
	}

	info := make(map[string]any, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		key, ok := arr[i].(string)
		if !ok {
			continue
		}
		info[key] = arr[i+1]
	}
	return info, nil
}
