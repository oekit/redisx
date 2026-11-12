package redisx

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

// fakeBloomClient 是只实现 Do 的 UniversalClient 测试桩（与 client_close_test
// 的 stubClient 同形态）：其余方法内嵌 nil 接口，被误调即 panic；应答由 doFn
// 注入——miniredis 不支持 BF.* 模块命令，MAdd/MExists/Info 的应答解析只能经
// 桩钉住（redisx#170）。
type fakeBloomClient struct {
	redis.UniversalClient
	doFn func(ctx context.Context, args ...any) *redis.Cmd
}

func (f *fakeBloomClient) Do(ctx context.Context, args ...any) *redis.Cmd {
	return f.doFn(ctx, args...)
}

// newFakeBloom 构造挂桩客户端的布隆过滤器（key 不经前缀，convertKey 透传）。
func newFakeBloom(doFn func(ctx context.Context, args ...any) *redis.Cmd) *BloomFilter {
	return &BloomFilter{client: NewClientFromClient(&fakeBloomClient{doFn: doFn}), key: "bf:stub"}
}

// TestBloomFilter_ErrorContainingExists_NotSwallowed 回归钉（#112）：RESERVE
// 报错不再按错误文本猜「已存在」——错误消息恰好含 "exists" 的连接/代理级
// 故障（如 "connection pool exhausted, no free conn exists"）此前会被 Contains
// 文本匹配误判成「已存在，复用」，NewBloomFilter 返回 nil error，调用方拿到
// 一个打不进去的空过滤器句柄，真实失败被推远到 BF.ADD 才爆且根因不可见。
// 修复后：BF.INFO 探测同样失败 ⇒ RESERVE 的失败另有原因，原始错误上抛不吞。
func TestBloomFilter_ErrorContainingExists_NotSwallowed(t *testing.T) {
	c, mr := newTestClient(t, "")
	ctx := context.Background()

	// miniredis 不支持 BF.* 模块命令，经 SetError 让每条命令稳定返回带
	// "exists" 字样的连接级错误消息——旧实现对该消息必吞（Contains 命中）。
	mr.SetError("connection pool exhausted, no free conn exists")

	bf, err := c.NewBloomFilter(ctx, "bf:trap", 0.01, 1000)
	if err == nil {
		t.Fatal("含 exists 的连接级错误不得被吞（#112）：NewBloomFilter 必须返回错误")
	}
	if bf != nil {
		t.Fatal("错误路径不得返回过滤器句柄")
	}
	if !strings.Contains(err.Error(), "connection pool exhausted") {
		t.Fatalf("返回错误必须保留 RESERVE 原始错误，got: %v", err)
	}
}

// TestBloomFilter_ModuleMissing_ErrorPropagates 表驱动补第二形态：错误不含
// "exists"（模块缺失的 unknown command）同样必须上抛——锁住「删除文本匹配」
// 不退化成「只吞特定文案」。miniredis 对 BF.RESERVE/BF.INFO 均回 unknown
// command ⇒ 探测失败 ⇒ 原始错误包装上抛（真 Redis 无 RedisBloom 模块同形态）。
func TestBloomFilter_ModuleMissing_ErrorPropagates(t *testing.T) {
	c, _ := newTestClient(t, "")
	ctx := context.Background()

	bf, err := c.NewBloomFilter(ctx, "bf:nomodule", 0.01, 1000)
	if err == nil {
		t.Fatal("BF.RESERVE 失败（模块缺失）必须返回错误")
	}
	if bf != nil {
		t.Fatal("错误路径不得返回过滤器句柄")
	}
	if !strings.Contains(err.Error(), "create bloom filter") {
		t.Fatalf("错误必须带 create bloom filter 包装，got: %v", err)
	}
}

// TestBloomFilter_MAdd_MExists_ParseAndArgs 批量路径补测（redisx#170）：
// miniredis 不支持 BF.*，经注入桩钉住 args 拼装（命令名 + convertKey 前缀键 +
// 逐个成员）与 BoolSlice 解析（int64 0/1 → false/true）。
func TestBloomFilter_MAdd_MExists_ParseAndArgs(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		cmd      string
		call     func(bf *BloomFilter) ([]bool, error)
		response []any
		want     []bool
	}{
		{
			cmd:      "BF.MADD",
			response: []any{int64(1), int64(0), int64(1)},
			want:     []bool{true, false, true},
		},
		{
			cmd:      "BF.MEXISTS",
			response: []any{int64(0), int64(1)},
			want:     []bool{false, true},
		},
	}
	for _, c := range cases {
		var gotArgs []any
		bf := newFakeBloom(func(_ context.Context, args ...any) *redis.Cmd {
			gotArgs = args
			cmd := redis.NewCmd(ctx)
			cmd.SetVal(c.response)
			return cmd
		})
		var res []bool
		var err error
		if c.cmd == "BF.MADD" {
			res, err = bf.MAdd(ctx, "a", "b", "c")
		} else {
			res, err = bf.MExists(ctx, "a", "b")
		}
		if err != nil {
			t.Fatalf("%s: %v", c.cmd, err)
		}
		if len(res) != len(c.want) {
			t.Fatalf("%s: got %v, want %v", c.cmd, res, c.want)
		}
		for i := range c.want {
			if res[i] != c.want[i] {
				t.Fatalf("%s: got %v, want %v", c.cmd, res, c.want)
			}
		}
		// args 拼装口径：args[0]=命令名、args[1]=convertKey 后的键、随后逐个成员。
		if len(gotArgs) != 2+len(c.want) {
			t.Fatalf("%s args = %v, want cmd+key+%d members", c.cmd, gotArgs, len(c.want))
		}
		if gotArgs[0] != c.cmd || gotArgs[1] != "bf:stub" {
			t.Fatalf("%s args[0..1] = (%v, %v), want (%s, bf:stub)", c.cmd, gotArgs[0], gotArgs[1], c.cmd)
		}
	}
}

// TestBloomFilter_MAdd_ErrorPropagates 通信失败原样上抛不吞、结果置 nil。
func TestBloomFilter_MAdd_ErrorPropagates(t *testing.T) {
	ctx := context.Background()
	bf := newFakeBloom(func(ctx context.Context, _ ...any) *redis.Cmd {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(context.DeadlineExceeded)
		return cmd
	})
	res, err := bf.MAdd(ctx, "a")
	if err == nil {
		t.Fatal("MAdd 通信失败必须上抛 error")
	}
	if res != nil {
		t.Fatalf("错误路径不得返回部分结果, got %v", res)
	}
}

// TestBloomFilter_MAdd_EmptyItems 空入参边界（redisx#170）：args 只含命令名与
// 键，服务器应答按空切片解析——真实 Redis 对缺成员的 BF.MADD 回 wrong number
// of arguments 并经上条用例的同一路径上抛，桩态下钉 args 形状与空应答解析。
func TestBloomFilter_MAdd_EmptyItems(t *testing.T) {
	ctx := context.Background()
	var gotArgs []any
	bf := newFakeBloom(func(_ context.Context, args ...any) *redis.Cmd {
		gotArgs = args
		cmd := redis.NewCmd(ctx)
		cmd.SetVal([]any{})
		return cmd
	})
	res, err := bf.MAdd(ctx)
	if err != nil {
		t.Fatalf("empty MAdd with empty reply: %v", err)
	}
	if len(res) != 0 {
		t.Fatalf("empty MAdd must return empty slice, got %v", res)
	}
	if len(gotArgs) != 2 || gotArgs[0] != "BF.MADD" || gotArgs[1] != "bf:stub" {
		t.Fatalf("empty MAdd args = %v, want [BF.MADD bf:stub]", gotArgs)
	}
}

// TestBloomFilter_Info_ParseMapAndSkipNonStringKeys BF.INFO 应答解析（redisx#170）：
// 交替 k-v 数组 → map；非字符串 key 的元素走 continue 分支不进 map。
func TestBloomFilter_Info_ParseMapAndSkipNonStringKeys(t *testing.T) {
	ctx := context.Background()
	bf := newFakeBloom(func(ctx context.Context, _ ...any) *redis.Cmd {
		cmd := redis.NewCmd(ctx)
		cmd.SetVal([]any{
			"Capacity", int64(1000),
			int64(42), "dropped-non-string-key", // 非字符串 key：跳过
			"Size", int64(128),
			"Filter", "bloom", // 值保持原始类型透传
		})
		return cmd
	})
	info, err := bf.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if v, ok := info["Capacity"].(int64); !ok || v != 1000 {
		t.Fatalf("Capacity = %v, want int64(1000)", info["Capacity"])
	}
	if v, ok := info["Size"].(int64); !ok || v != 128 {
		t.Fatalf("Size = %v, want int64(128)", info["Size"])
	}
	if v, ok := info["Filter"].(string); !ok || v != "bloom" {
		t.Fatalf("Filter = %v, want \"bloom\"", info["Filter"])
	}
	if _, ok := info["dropped-non-string-key"]; ok {
		t.Fatalf("非字符串 key 的元素不得进 map（continue 分支）, got %v", info)
	}
	if len(info) != 3 {
		t.Fatalf("map size = %d, want 3, got %v", len(info), info)
	}
}

// TestBloomFilter_Info_UnexpectedShapeReturnsError BF.INFO 应答形态异常（应答
// 不是交替数组，如代理损坏返回字符串）必须返回 error 而非 panic/静默空 map。
func TestBloomFilter_Info_UnexpectedShapeReturnsError(t *testing.T) {
	ctx := context.Background()
	bf := newFakeBloom(func(ctx context.Context, _ ...any) *redis.Cmd {
		cmd := redis.NewCmd(ctx)
		cmd.SetVal("corrupted-proxy-reply")
		return cmd
	})
	info, err := bf.Info(ctx)
	if err == nil {
		t.Fatal("异常应答形态必须返回 error")
	}
	if !strings.Contains(err.Error(), "unexpected BF.INFO response type") {
		t.Fatalf("错误必须指明应答形态异常，got: %v", err)
	}
	if info != nil {
		t.Fatalf("错误路径不得返回 map, got %v", info)
	}
}

// TestBloomFilter_Info_ErrorPropagates 通信失败原样上抛。
func TestBloomFilter_Info_ErrorPropagates(t *testing.T) {
	ctx := context.Background()
	bf := newFakeBloom(func(ctx context.Context, _ ...any) *redis.Cmd {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(context.DeadlineExceeded)
		return cmd
	})
	if _, err := bf.Info(ctx); err == nil {
		t.Fatal("Info 通信失败必须上抛 error")
	}
}
