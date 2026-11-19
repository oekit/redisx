package cache

// RedisCache 契约套件接入点：跑 github.com/oekit/pkg/cache/cachetest 的 9 个
// 通用场景（miss (nil,nil)、Set/Get 往返覆盖、显式 TTL 过期、Del/DelBatch
// 幂等、GetOrLoad 回填计数、null marker 防穿透、Refresh、并发混合、
// nil_ctx_treated_as_background——nil ctx 按 Background 处理不 panic）。
// 通用缓存语义由套件固化，本包不再重复编写。
//
// 本包为内部测试包（package cache）——cachetest import 的是 data/cache
// 被测接口，与本包无 import cycle，可直接接入。

import (
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/oekit/pkg/cache"
	cachetest "github.com/oekit/pkg/cache/cachetest"
)

// newContractCache 契约套件工厂：每个场景独立 miniredis + go-redis client
// （均经 t.Cleanup 清理），注入场景给定的 source 构造全新 RedisCache。
//
// ClockTTL(true)：套件的 set_with_ttl_expiry 场景按真实时间睡眠等待过期，
// 而 miniredis 默认只在 FastForward() 手动拨钟时才让 TTL 递减（真实时间
// 流逝不触发过期），须显式开启随真实时钟递减的 TTL 语义。
func newContractCache[T any](t *testing.T, source cache.SourceFunc[T]) cache.CacheAside[T] {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.ClockTTL(true)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisCache[T](client, RedisConfig[T]{
		Prefix: "ct:",
		TTL:    time.Minute,
		Source: source,
	})
}

// TestContract_RedisCache_String 契约套件（string 值，Comparable 现成实现）。
func TestContract_RedisCache_String(t *testing.T) {
	cachetest.Run(t, cachetest.Comparable[string](), newContractCache[string])
}

// ctUser 契约套件结构体值类型（覆盖真实 JSON 结构体序列化路径；可比较，
// 配套自定义 Contract——Comparable 不支持 struct 枚举）。
type ctUser struct {
	ID   int64
	Name string
}

// TestContract_RedisCache_Struct 契约套件（结构体值，自定义 Contract）。
func TestContract_RedisCache_Struct(t *testing.T) {
	contract := cachetest.Contract[ctUser]{
		NewValue: func(i int) *ctUser {
			return &ctUser{ID: int64(i), Name: fmt.Sprintf("user-%d", i)}
		},
		Equal: func(a, b *ctUser) bool {
			if a == nil || b == nil {
				return a == b
			}
			return *a == *b
		},
	}
	cachetest.Run(t, contract, newContractCache[ctUser])
}
