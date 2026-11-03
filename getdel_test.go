package redisx

import (
	"context"
	"testing"
	"time"

	"github.com/oekit/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// TestGetDel（redisx#78）：一次性取即删核销——取到值且键被删除，
// 再取回 redis.Nil；键经 convertKey（与其它单发命令同口径）。
func TestGetDel(t *testing.T) {
	c, mr := newTestClient(t, "app")
	ctx := context.Background()
	if err := c.Set(ctx, "token", "t1", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	v, err := c.GetDel(ctx, "token").Result()
	if err != nil || v != "t1" {
		t.Fatalf("GetDel 应取到 t1，got %q err=%v", v, err)
	}
	if mr.Exists("app:token") {
		t.Error("取即删后物理键 app:token 应已删除")
	}
	for _, k := range mr.Keys() {
		if k == "token" {
			t.Errorf("裸键 %q 存在：GetDel 漏了 convertKey", k)
		}
	}

	// 恰好一次核销：第二次取回 redis.Nil
	if _, err := c.GetDel(ctx, "token").Result(); !errors.Is(err, redis.Nil) {
		t.Errorf("缺失键应回 redis.Nil，got %v", err)
	}
}

// TestGetDelCompat_NativePath：新 Redis（≥6.2，miniredis 支持 GETDEL）走原生单命令。
func TestGetDelCompat_NativePath(t *testing.T) {
	c, _ := newTestClient(t, "app")
	ctx := context.Background()
	if err := c.Set(ctx, "tok", "v1", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	v, err := c.GetDelCompat(ctx, "tok").Result()
	if err != nil || v != "v1" {
		t.Fatalf("GetDelCompat 应取到 v1，got %q err=%v", v, err)
	}
	if _, err := c.GetDelCompat(ctx, "tok").Result(); !errors.Is(err, redis.Nil) {
		t.Errorf("缺失键应回 redis.Nil，got %v", err)
	}
}

// noGetDelClient 模拟 Redis < 6.2：GETDEL 返回 unknown command，其余命令照常。
type noGetDelClient struct {
	redis.UniversalClient
}

func (c *noGetDelClient) GetDel(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, "getdel", key)
	cmd.SetErr(errors.New("ERR unknown command 'GETDEL'"))
	return cmd
}

// TestGetDelCompat_Fallback（redisx#78）：老 Redis 无 GETDEL 时回退到 Lua
// 原子 GET+DEL——不是两步 GET+DEL（并发下会双核销），核销语义不降级。
func TestGetDelCompat_Fallback(t *testing.T) {
	base, _ := newTestClient(t, "app")
	ctx := context.Background()
	c := NewClientFromClient(&noGetDelClient{UniversalClient: base.Client()}, WithPrefix("app"))

	if err := c.Set(ctx, "tok", "v1", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	v, err := c.GetDelCompat(ctx, "tok").Result()
	if err != nil || v != "v1" {
		t.Fatalf("回退路径应取到 v1，got %q err=%v", v, err)
	}
	if _, err := c.GetDelCompat(ctx, "tok").Result(); !errors.Is(err, redis.Nil) {
		t.Errorf("缺失键应回 redis.Nil，got %v", err)
	}
	// 回退路径同样套前缀：物理键 app:tok 已被取删，裸键 tok 从未存在
	if exists, _ := c.Exists(ctx, "tok").Result(); exists != 0 {
		t.Error("取即删后物理键应已删除")
	}
}

// TestGetDelByScript 口径：命中回值（含空串——Lua 空串为真值，照常取删），
// 缺失回 redis.Nil。
func TestGetDelByScript(t *testing.T) {
	c, _ := newTestClient(t, "app")
	ctx := context.Background()

	if err := c.Set(ctx, "empty", "", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	v, err := getDelByScript(ctx, c.Client(), c.convertKey("empty")).Result()
	if err != nil || v != "" {
		t.Fatalf("空串值应取删成功，got %q err=%v", v, err)
	}
	if _, err := getDelByScript(ctx, c.Client(), c.convertKey("empty")).Result(); !errors.Is(err, redis.Nil) {
		t.Errorf("缺失键应回 redis.Nil，got %v", err)
	}
}

// TestIsUnknownCmdErr 错误判定：仅「命令不存在」触发回退，其余错误照常上抛。
func TestIsUnknownCmdErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"unknown command", errors.New("ERR unknown command 'GETDEL'"), true},
		{"unknown subcommand", errors.New("ERR unknown subcommand"), true},
		{"case-insensitive", errors.New("ERR Unknown Command"), true},
		{"redis.Nil", redis.Nil, false},
		{"connection error", errors.New("dial tcp: connection refused"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := isUnknownCmdErr(tc.err); got != tc.want {
			t.Errorf("%s: isUnknownCmdErr(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

// TestPipeline_GetDel_UsesPrefix：管道取即删与单发同键（#78 与 #79 交叉面）。
func TestPipeline_GetDel_UsesPrefix(t *testing.T) {
	c, mr := newTestClient(t, "app")
	ctx := context.Background()
	if err := c.Set(ctx, "tk", "v1", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	p := c.Pipeline()
	getdel := p.GetDel(ctx, "tk")
	if _, err := p.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := getdel.Result(); err != nil || v != "v1" {
		t.Errorf("管道 GetDel 应取到 v1，got %q err=%v", v, err)
	}
	if mr.Exists("app:tk") || mr.Exists("tk") {
		t.Errorf("取即删后键应已删除，实际 keys=%v", mr.Keys())
	}
}
