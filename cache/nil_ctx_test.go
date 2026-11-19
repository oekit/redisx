package cache

// nil ctx 全入口归一测试（cachetest 契约第 9 场景 nil_ctx_treated_as_background，
// pkg#554，消费方 CI 地板 bump 到 pkg v0.55.1 后暴露）：
//
// 契约场景只覆盖 Get/GetOrLoad/Set/Exists/Del 五个入口；本文件补齐其余公开
// 入口（GetBytes/SetWithTTL/SetRaw/DelBatch/MGet/Refresh）的 nil ctx 路径——
// 此前 nil 接口值会一路透传进 go-redis v9 连接池（pool.go nil 解引用 panic，
// 堆栈离传错的调用点很远）。语义口径与 pkg/contextx.OrBackground 一致：
// nil == context.Background()（只做 nil 归一，不复活取消信号）。
//
// 与 #247 的 Set/SetWithTTL nil 值 fail-fast 无冲突：一个守 ctx、一个守 value。

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newNilCtxCache(t *testing.T) *RedisCache[string] {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.ClockTTL(true)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisCache[string](client, RedisConfig[string]{
		Prefix: "nilctx:",
		TTL:    time.Minute,
		Source: func(_ context.Context, _ string) (*string, error) {
			v := "from-source"
			return &v, nil
		},
	})
}

func TestRedisCache_NilCtxTreatedAsBackground(t *testing.T) {
	t.Run("写读删全入口", func(t *testing.T) {
		c := newNilCtxCache(t)

		if err := c.Set(nil, "k1", strPtr("v1")); err != nil { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx Set: %v", err)
		}
		if err := c.SetWithTTL(nil, "k2", strPtr("v2"), time.Minute); err != nil { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx SetWithTTL: %v", err)
		}
		if err := c.SetRaw(nil, "k3", []byte("raw"), time.Minute); err != nil { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx SetRaw: %v", err)
		}

		if got, err := c.Get(nil, "k1"); err != nil || got == nil || *got != "v1" { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx Get: got (%v, %v)", got, err)
		}
		if data, err := c.GetBytes(nil, "k1"); err != nil || string(data) != `"v1"` { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx GetBytes: got (%q, %v)", data, err)
		}
		if out, err := c.MGet(nil, "k1", "k2"); err != nil || len(out) != 2 || *out["k2"] != "v2" { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx MGet: got (%v, %v)", out, err)
		}
		if exists, err := c.Exists(nil, "k3"); err != nil || !exists { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx Exists: got (exists=%v, err=%v)", exists, err)
		}
		if err := c.DelBatch(nil, "k2", "k3"); err != nil { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx DelBatch: %v", err)
		}
		if err := c.Del(nil, "k1"); err != nil { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx Del: %v", err)
		}
	})

	t.Run("Refresh 与 GetOrLoad 回源", func(t *testing.T) {
		c := newNilCtxCache(t)

		if got, err := c.GetOrLoad(nil, "lk"); err != nil || got == nil || *got != "from-source" { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx GetOrLoad: got (%v, %v)", got, err)
		}
		if got, err := c.Refresh(nil, "lk"); err != nil || got == nil || *got != "from-source" { //nolint:staticcheck // SA1012：传 nil ctx 正是本测试点
			t.Fatalf("nil ctx Refresh: got (%v, %v)", got, err)
		}
	})
}

func strPtr(s string) *string { return &s }
