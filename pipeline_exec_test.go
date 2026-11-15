package redisx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestPipeline_Exec_FirstErrorOnly 钉住 go-redis 语义（redisx#214 前提）：
// Exec 返回的 err 只是首条命令错误，后续命令的失败只在 []Cmder 里逐条带；
// 首条为 redis.Nil（Get 未命中）时会遮蔽其后的真实错误。本测试固化该
// 口径，防止 go-redis 升级改变行为时 ExecChecked 的对照失去意义。
func TestPipeline_Exec_FirstErrorOnly(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()
	if err := c.Set(ctx, "str", "notanint", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	p := c.Pipeline()
	p.Get(ctx, "missing") // 首条：redis.Nil（合法负回复）
	p.Incr(ctx, "str")    // 次条：ERR value is not an integer（真失败）
	cmds, err := p.Exec(ctx)
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("go-redis Exec 应返回首条命令错误（redis.Nil），got %v", err)
	}
	if cmds[1].Err() == nil || !strings.Contains(cmds[1].Err().Error(), "not an integer") {
		t.Fatalf("次条命令的真错误应逐条携带，got %v", cmds[1].Err())
	}
	// 首条 Nil 遮蔽了次条真错误——这正是 ExecChecked 要解决的问题。
	if errors.Is(err, cmds[1].Err()) {
		t.Fatal("对照失效：Exec 不应包含次条错误")
	}
}

// TestPipeline_ExecChecked_AggregatesAllErrors 回归（redisx#214）：
// 混合失败管道（真错误 ×2，中间夹成功命令）——Exec 只回首条，ExecChecked
// 必须聚合全部真错误（errors.Is 可逐条判定），且 redis.Nil 不计入。
func TestPipeline_ExecChecked_AggregatesAllErrors(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()
	if err := c.Set(ctx, "str", "notanint", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	// 首条故意用 Get 未命中的 Nil：Exec 会返回 Nil 遮蔽一切，
	// ExecChecked 必须越过 Nil 把两条真错误都聚合出来。
	p := c.Pipeline()
	p.Get(ctx, "missing")
	incr := p.Incr(ctx, "str")      // ERR value is not an integer
	p.Set(ctx, "ok", "v", 0)        // 成功命令夹在中间
	hget := p.HGet(ctx, "str", "f") // WRONGTYPE（str 是 string 而非 hash）

	err := p.ExecChecked(ctx)
	if err == nil {
		t.Fatal("混合失败管道 ExecChecked 必须返回 error（修复前按 Exec 首条口径会拿到 Nil 或漏报）")
	}
	if errors.Is(err, redis.Nil) {
		t.Errorf("redis.Nil 是合法负回复，不应计入整批失败，got %v", err)
	}
	if !errors.Is(err, incr.Err()) {
		t.Errorf("应聚合 Incr 的错误（errors.Is 可判定），got %v", err)
	}
	if hget.Err() == nil {
		t.Fatal("HGet 应报 WRONGTYPE")
	}
	if !errors.Is(err, hget.Err()) {
		t.Errorf("应聚合 HGet 的错误（errors.Is 可判定），got %v", err)
	}
}

// TestPipeline_ExecChecked_AllSuccessOrNil 回归（redisx#214）：整批成功
// （含仅 Get 未命中的 Nil）时 ExecChecked 返回 nil——「err == nil ⟺ 整批
// 成功」的另一半口径。
func TestPipeline_ExecChecked_AllSuccessOrNil(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	// 全成功
	p := c.Pipeline()
	p.Set(ctx, "k", "v", 0)
	p.Incr(ctx, "cnt")
	if err := p.ExecChecked(ctx); err != nil {
		t.Fatalf("整批成功应返回 nil，got %v", err)
	}

	// 仅 redis.Nil（Get 未命中）：合法负回复，不算失败
	p2 := c.Pipeline()
	p2.Get(ctx, "missing")
	p2.Get(ctx, "missing2")
	if err := p2.ExecChecked(ctx); err != nil {
		t.Fatalf("仅 Get 未命中应返回 nil（Nil 不计入），got %v", err)
	}
}

// TestPipeline_ExecChecked_DedupTransportError 回归（redisx#214）：传输级
// 失败（连接被拒）时 go-redis setCmdsErr 把同一错误实例铺到每条命令——
// 聚合必须去重，报错消息不重复 N 遍。
func TestPipeline_ExecChecked_DedupTransportError(t *testing.T) {
	// 127.0.0.1:1 无服务，dial 必拒；惰性连接构造不报错。
	c, err := NewClientFromAddr("127.0.0.1:1", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	ctx := context.Background()

	p := c.Pipeline()
	p.Set(ctx, "a", "1", 0)
	p.Set(ctx, "b", "2", 0)
	p.Set(ctx, "c", "3", 0)

	execErr := p.ExecChecked(ctx)
	if execErr == nil {
		t.Fatal("连接不可达时 ExecChecked 必须返回 error")
	}
	msg := execErr.Error()
	if n := strings.Count(msg, "connection refused"); n != 1 {
		t.Fatalf("同一传输错误应去重为 1 次出现，got %d 次（消息：%s）", n, msg)
	}
}
