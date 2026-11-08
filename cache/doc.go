// Package cache 提供基于 go-redis 的 Cache[T] 分布式缓存实现。
//
// 与根包内存实现（LocalCache）的关系：根包 data/cache 只保留接口
// （Cache/CacheAside/SourceFunc）与内存实现，本子包是 Redis 后端——
// import 根包不再传递编译依赖 go-redis。
//
//	import (
//		"github.com/oekit/pkg/cache"
//		rediscache "github.com/oekit/redisx/cache"
//	)
//
//	c := rediscache.NewRedisCache[User](rdb, rediscache.RedisConfig[User]{
//	    Prefix: "app:user:",
//	    TTL:    10 * time.Minute,
//	    Source: loadUserFromDB, // Cache-Aside 未命中回源
//	})
//	// c 满足 cache.Cache[User] / cache.CacheAside[User]
//
// 接口契约：RedisCache 对 data/cache 门面接口的满足由编译期断言固化
// （redis.go 顶部 var 块，int/string 双类型实例化）；跨实现一致的通用
// 缓存语义（miss 非 error、null marker 防穿透、Del 幂等、Refresh、并发
// 混合等）由 github.com/oekit/pkg/cache/cachetest 契约套件固化，本包
// contract_suite_test.go 接入（8 场景 × string/结构体双值类型）。
//
// 空值缓存语义（防穿透）见 RedisConfig.NullTTL 字段注释，与 LocalCache
// 保持一致：0 = 默认开启（1 分钟），负值 = 显式禁用。
//
// 日志：内部告警（回填失败/序列化失败等）缺省走实例私有的
// logx.Delegating("cache-redis")（懒解析进程级 logx.Default）；需要单独
// 收编进宿主日志体系（zap/slog/hlog）时经 RedisConfig.Logger 注入
// per-instance logger，不影响其它 logx 使用方（#120）。
//
// null 标记保留前缀契约：null 标记哨兵以 \x00 开头（见 redis.go
// nullValue），合法 JSON 序列化输出不会自然撞上该前缀。SetRaw 等原始
// 写入入口不得写入以 \x00 开头的值——精确等于哨兵的条目按 null marker
// 处理，其余 \x00 开头值不认作 null（走反序列化，默认 JSON 下报错）。
// v0.4.0 前哨兵为 "__NULL__"，可被 SetRaw 精确碰撞（已废弃）。
package cache
