package redisx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client 是带 key 前缀的 Redis 客户端封装。
type Client struct {
	prefix string
	client redis.UniversalClient

	// closeOnce/closeErr：Close 幂等（#148）——sync.Once 保证底层连接池只真正
	// 关一次，首次关闭的结果（含 nil）缓存在 closeErr，重复调用稳定返回同一结果。
	closeOnce sync.Once
	closeErr  error
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
// opt 为 nil 属调用方参数错误，构造期 fail-fast 返回 error（#129）——
// 函数签名预留了 error 返回通道，nil 入参不应升级成进程崩溃（构造期直接
// 解引用会 nil 指针 panic，调用方拿不到任何可处理的错误）。与 etcdx 的
// 同族构造口径一致（etcdx.NewClient(nil) 同样返回 error 而非 panic）。
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
	if opt == nil {
		return nil, fmt.Errorf("redisx: options are nil")
	}

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

// Close 幂等关闭底层 Redis 连接（#148）。
//
// 契约（本仓生命周期硬规则：组件 Close 统一 Close(ctx) error + sync.Once 幂等）：
//   - sync.Once 保证「显式 Close + defer Close」双路径、多持有者各自 Close 时
//     底层连接池只真正关一次，重复调用稳定返回**首次**关闭的结果（成功即恒 nil，
//     不再透传 go-redis 的 redis.ErrClosed——「二次关闭被误记为关闭失败」的口径
//     就此关闭）；
//   - 签名与 etcdx.Client.Close(ctx) 同形（矩阵统一口径）：底层 go-redis 连接池
//     关闭是同步有界操作，ctx 仅承接统一签名、不参与期限控制（无需 goroutine
//     包装），停机期限由调用方在外层关停流程约束。
func (c *Client) Close(_ context.Context) error {
	c.closeOnce.Do(func() {
		c.closeErr = c.client.Close()
	})
	return c.closeErr
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
