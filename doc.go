// Package redisx 提供 Redis 客户端封装。
//
// 所有 enjoydream 后端服务共享此包，统一封装：key 前缀管理。
// 零可观测性副作用：构造不注入任何 tracing；需要 OpenTelemetry span 时
// 用 github.com/oekit/tracing/redisotel 装饰器显式接入。
//
// # 核心特性
//
//   - key 前缀自动添加（prefix:key 格式）
//   - 支持 redis.UniversalClient（兼容单机、集群、哨兵模式）
//   - 提供 Client() 方法获取原生客户端（用于 distlock、tracing 接入等场景）
//   - Close(ctx) error 幂等关停（sync.Once；重复调用稳定返回同一结果）
//
// # 快速接入
//
// 从地址创建：
//
//	client, err := redisx.NewClientFromAddr("localhost:6379", "", 0,
//	    redisx.WithPrefix("myservice"),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close(context.Background())
//
// 从配置创建（支持集群）：
//
//	opt := &redis.UniversalOptions{
//	    Addrs:    []string{"redis-1:6379", "redis-2:6379"},
//	    Password: "password",
//	    DB:       0,
//	}
//	client, err := redisx.NewClient(opt, redisx.WithPrefix("myservice"))
//
// # Key 前缀
//
// 所有操作的 key 会自动添加 "prefix:" 前缀：
//
//	client.Set(ctx, "user:123", value, 0)
//	// 实际存储的 key 为 "myservice:user:123"
//
// # 原生客户端
//
// 需要原生 redis.UniversalClient 时（如 distlock、cache、tracing 接入等包）：
//
//	locker := distlock.NewRedisLocker(client.Client(), distlock.WithPrefix("myservice"))
//	_ = redisotel.InstrumentTracing(client.Client()) // github.com/oekit/tracing/redisotel
package redisx
