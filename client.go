package redisx

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client 是带 key 前缀的 Redis 客户端封装。
type Client struct {
	prefix string
	client redis.UniversalClient
}

// RedisClientOption 配置 Client 的可选参数。
type RedisClientOption struct {
	Prefix string
}

// RedisClientOptionFunc 配置函数类型。
type RedisClientOptionFunc func(*RedisClientOption)

// WithPrefix 设置 key 前缀。
// 所有操作的 key 会自动添加 "prefix:" 前缀。
func WithPrefix(prefix string) RedisClientOptionFunc {
	return func(o *RedisClientOption) { o.Prefix = prefix }
}

// NewClient 从 UniversalOptions 创建 Redis 客户端。
//
// opt 不会被改写（#116）：入口先 shallow-copy 再填缺省值——调用方复用同一
// opt 构建第二个实例（同配置双实例/测试矩阵常见）时，不会拿到被第一次调用
// 污染过的配置（否则「调用方显式设置的字段不覆盖」的判定基线已变，比如
// 调用方故意传 DialTimeout: 0 想用 go-redis 自身缺省，第一次调用后被固化
// 为 3s）。*redis.UniversalOptions 是纯值结构（字段全为值/切片），浅拷贝
// 安全。与 NewClientFromClient 的「一个参数都不动」口径对齐。
//
// 零可观测性副作用：不自动注入任何 tracing。需要 OpenTelemetry span 时
// 用装饰器显式接入（无 SDK 时 noop）：
//
//	c, _ := redisx.NewClient(opt)
//	_ = redisotel.InstrumentTracing(c.Client()) // github.com/oekit/tracing/redisotel
func NewClient(opt *redis.UniversalOptions, opts ...RedisClientOptionFunc) (*Client, error) {
	o := &RedisClientOption{}
	for _, fn := range opts {
		fn(o)
	}

	// shallow-copy 后再填默认值（#116）：改写只落在副本上，调用方的
	// 结构体保持原样。浅拷贝足够——默认值只写 Duration/int 标量字段，
	// 不触碰 Addrs 等切片字段。
	effective := *opt

	// 生产默认值（调用方显式设置的字段不覆盖）：
	// 拨号/读写超时收紧故障感知窗口；池超时防止慢 Redis 拖垮 worker；
	// 最小空闲连接削峰重拨。go-redis 对未设 PoolSize 默认 10*GOMAXPROCS，
	// 此处不越俎代庖。
	if effective.DialTimeout == 0 {
		effective.DialTimeout = 3 * time.Second
	}
	if effective.ReadTimeout == 0 {
		effective.ReadTimeout = 2 * time.Second
	}
	if effective.WriteTimeout == 0 {
		effective.WriteTimeout = 2 * time.Second
	}
	if effective.PoolTimeout == 0 {
		effective.PoolTimeout = 3 * time.Second
	}
	if effective.MinIdleConns == 0 {
		effective.MinIdleConns = 2
	}

	client := redis.NewUniversalClient(&effective)

	return &Client{
		prefix: o.Prefix,
		client: client,
	}, nil
}

// NewClientFromClient **包住既有** go-redis 客户端，不新建连接、不改连接参数。
//
// 为什么需要：消费方（pigeonbox/core）自己管连接池与超时（不想让 redisx 改写
// 它们的连接语义），但又想用 redisx 的原语（典型是 Counter.IncrWithTTL：
// 「计数 + 首次命中才设 TTL」的原子版）。
// 与 NewClient 的关键区别：
//   - NewClient 会在副本上（不改调用方 opt，#116）填连接默认参数
//     （DialTimeout/ReadTimeout/WriteTimeout/PoolTimeout/MinIdleConns）
//     ⇒ 连接语义会变；
//   - NewClientFromClient 一个参数都不动 ⇒ 纯包装，调用方的连接行为完全不变。
//
// 传入 nil 会得到一个不可用的 Client（调用其方法会 panic）——调用方应自行保证非 nil。
func NewClientFromClient(c redis.UniversalClient, opts ...RedisClientOptionFunc) *Client {
	o := &RedisClientOption{}
	for _, fn := range opts {
		fn(o)
	}
	return &Client{
		prefix: o.Prefix,
		client: c,
	}
}

// NewClientFromAddr 从地址字符串创建 Redis 客户端（便捷方法）。
func NewClientFromAddr(addr, password string, db int, opts ...RedisClientOptionFunc) (*Client, error) {
	opt := &redis.UniversalOptions{
		Addrs:    []string{addr},
		Password: password,
		DB:       db,
	}
	return NewClient(opt, opts...)
}

// Close 关闭底层 Redis 连接。
func (c *Client) Close() error {
	return c.client.Close()
}

// Ping 测试连接。
func (c *Client) Ping(ctx context.Context) *redis.StatusCmd {
	return c.client.Ping(ctx)
}

// Client 返回底层 redis.UniversalClient（用于 distlock 等需要原生客户端的场景）。
func (c *Client) Client() redis.UniversalClient {
	return c.client
}

func (c *Client) convertKey(key string) string {
	if c.prefix == "" {
		return key
	}
	return c.prefix + ":" + key
}

func (c *Client) convertKeys(keys []string) []string {
	if c.prefix == "" {
		return keys
	}
	result := make([]string, len(keys))
	for i, key := range keys {
		result[i] = c.convertKey(key)
	}
	return result
}
