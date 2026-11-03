package redisx

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestPipeline_PrefixSameKeyAsSingleShot（redisx#79 回归）：配置 key 前缀后，
// 管道与单发命令必须落到同一物理键。此前 Pipeline() 返回裸 redis.Pipeliner，
// 管道命令不经 convertKey：单发 Set("k") 落 app:k、管道 Set("k") 落 k，
// 两边各自自洽、静默分叉（缓存击穿/计数分裂/锁不住键）。
func TestPipeline_PrefixSameKeyAsSingleShot(t *testing.T) {
	c, mr := newTestClient(t, "app")
	ctx := context.Background()

	// 单发路径：逻辑键 k → 物理键 app:k
	if err := c.Set(ctx, "k", "single", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	// 管道路径：同一逻辑键必须读写同一物理键
	p := c.Pipeline()
	get := p.Get(ctx, "k")
	p.Set(ctx, "k", "pipe", time.Minute)
	p.Incr(ctx, "cnt")
	if _, err := p.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := get.Result(); err != nil || v != "single" {
		t.Errorf("管道 Get 应读到单发写入的值，got %q err=%v", v, err)
	}
	// 单发读管道写的值——同键的最终证明
	if v, _ := c.Get(ctx, "k").Result(); v != "pipe" {
		t.Errorf("单发 Get 应读到管道写入的值，got %q", v)
	}

	// 物理上只允许带前缀的键
	for _, k := range mr.Keys() {
		if k == "k" || k == "cnt" {
			t.Errorf("裸键 %q 存在：管道命令漏了 convertKey（静默分叉）", k)
		}
	}
	if !mr.Exists("app:k") || !mr.Exists("app:cnt") {
		t.Errorf("期望 app:k / app:cnt 存在，实际 keys=%v", mr.Keys())
	}
}

// TestTxPipeline_PrefixSameKeyAsSingleShot（redisx#79 回归）：TxPipeline 同口径。
// 覆盖 zeus sso 的 SAdd+Expire 事务管道用法（issue 中点名踩雷的场景）。
func TestTxPipeline_PrefixSameKeyAsSingleShot(t *testing.T) {
	c, mr := newTestClient(t, "app")
	ctx := context.Background()

	tx := c.TxPipeline()
	tx.SAdd(ctx, "sess:u1", "sid1")
	tx.Expire(ctx, "sess:u1", time.Minute)
	if _, err := tx.Exec(ctx); err != nil {
		t.Fatal(err)
	}

	// 单发路径查同一逻辑键：必须命中管道写的物理键
	n, err := c.Exists(ctx, "sess:u1").Result()
	if err != nil || n != 1 {
		t.Fatalf("单发 Exists 应命中管道写的键，n=%d err=%v", n, err)
	}
	for _, k := range mr.Keys() {
		if k == "sess:u1" {
			t.Errorf("裸键 %q 存在：TxPipeline 漏了 convertKey（静默分叉）", k)
		}
	}
}

// TestPipeline_SurfaceCommandsUsePrefix：代理覆盖面与 Client 单发封装一一对应，
// 逐组冒烟——hash/list/set/zset/Lua 排队命令全部经 convertKey。
func TestPipeline_SurfaceCommandsUsePrefix(t *testing.T) {
	c, mr := newTestClient(t, "app")
	ctx := context.Background()

	p := c.Pipeline()
	p.HSet(ctx, "h", "f", "v")
	p.LPush(ctx, "l", "a")
	p.SAdd(ctx, "s", "m")
	p.ZAdd(ctx, "z", redis.Z{Score: 1, Member: "m"})
	p.SetNX(ctx, "nx", "v", time.Minute)
	p.Eval(ctx, "return redis.call('SET', KEYS[1], 'lua')", []string{"lk"})
	if _, err := p.Exec(ctx); err != nil {
		t.Fatal(err)
	}

	for _, k := range mr.Keys() {
		if k == "h" || k == "l" || k == "s" || k == "z" || k == "nx" || k == "lk" {
			t.Errorf("裸键 %q 存在：管道命令漏了 convertKey", k)
		}
	}
	for _, want := range []string{"app:h", "app:l", "app:s", "app:z", "app:nx", "app:lk"} {
		if !mr.Exists(want) {
			t.Errorf("期望物理键 %q 存在，实际 keys=%v", want, mr.Keys())
		}
	}
}

// TestPipeline_Raw_NoConversion 钉住逃生舱口径：Raw() 返回裸 Pipeliner，
// 不做键转换——代理未覆盖的命令显式走这条路，键名由调用方自行负责。
func TestPipeline_Raw_NoConversion(t *testing.T) {
	c, mr := newTestClient(t, "app")
	ctx := context.Background()

	p := c.Pipeline()
	p.Raw().Set(ctx, "raw-k", "v", time.Minute)
	if _, err := p.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if !mr.Exists("raw-k") {
		t.Errorf("Raw() 路径应不做键转换（裸键 raw-k），实际 keys=%v", mr.Keys())
	}
}
