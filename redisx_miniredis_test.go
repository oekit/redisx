package redisx

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestClient 启动 miniredis 并返回 ekit Client + 底层 miniredis 实例.
func newTestClient(t *testing.T, prefix string) (*Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c, err := NewClientFromAddr(mr.Addr(), "", 0, WithPrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c, mr
}

func TestClient_Prefix(t *testing.T) {
	c, _ := newTestClient(t, "app")
	if got := c.convertKey("k"); got != "app:k" {
		t.Errorf("expected 'app:k', got %q", got)
	}
	got := c.convertKeys([]string{"a", "b"})
	if len(got) != 2 || got[0] != "app:a" || got[1] != "app:b" {
		t.Errorf("expected [app:a app:b], got %v", got)
	}
}

func TestClient_Prefix_Empty(t *testing.T) {
	c, _ := newTestClient(t, "")
	if got := c.convertKey("k"); got != "k" {
		t.Errorf("expected 'k', got %q", got)
	}
}

func TestClient_Ping(t *testing.T) {
	c, _ := newTestClient(t, "x")
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Errorf("expected ping success, got %v", err)
	}
}

func TestClient_Client_ReturnsUnderlying(t *testing.T) {
	c, _ := newTestClient(t, "x")
	if c.Client() == nil {
		t.Error("expected non-nil underlying client")
	}
}

func TestStringOps(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	// Set + Get
	if err := c.Set(ctx, "k", "v", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Get(ctx, "k").Result(); err != nil || got != "v" {
		t.Errorf("expected v, got %q err=%v", got, err)
	}

	// SetNX
	ok, err := c.SetNX(ctx, "k", "v2", time.Minute).Result()
	if err != nil || ok {
		t.Errorf("SetNX on existing key: expected ok=false, got ok=%v err=%v", ok, err)
	}
	ok, _ = c.SetNX(ctx, "newkey", "v3", time.Minute).Result()
	if !ok {
		t.Error("SetNX on new key: expected ok=true")
	}

	// Del
	n, _ := c.Del(ctx, "k", "newkey", "missing").Result()
	if n != 2 {
		t.Errorf("expected Del to return 2, got %d", n)
	}

	// Exists
	_ = c.Set(ctx, "a", "1", 0)
	_ = c.Set(ctx, "b", "2", 0)
	n, _ = c.Exists(ctx, "a", "b", "missing").Result()
	if n != 2 {
		t.Errorf("expected Exists=2, got %d", n)
	}

	// Expire
	_ = c.Set(ctx, "ttl-key", "v", 0)
	ok, _ = c.Expire(ctx, "ttl-key", time.Minute).Result()
	if !ok {
		t.Error("Expire on existing key should return true")
	}
}

func TestCounterOps(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	// Incr/Decr/IncrBy/DecrBy
	n, _ := c.Incr(ctx, "n").Result()
	if n != 1 {
		t.Errorf("first Incr: expected 1, got %d", n)
	}
	n, _ = c.IncrBy(ctx, "n", 9).Result()
	if n != 10 {
		t.Errorf("IncrBy(9): expected 10, got %d", n)
	}
	n, _ = c.Decr(ctx, "n").Result()
	if n != 9 {
		t.Errorf("Decr: expected 9, got %d", n)
	}
	n, _ = c.DecrBy(ctx, "n", 4).Result()
	if n != 5 {
		t.Errorf("DecrBy(4): expected 5, got %d", n)
	}
}

func TestHashOps(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	if err := c.HSet(ctx, "h", "f1", "v1", "f2", "v2").Err(); err != nil {
		t.Fatal(err)
	}
	v, _ := c.HGet(ctx, "h", "f1").Result()
	if v != "v1" {
		t.Errorf("HGet f1: expected v1, got %q", v)
	}
	m, _ := c.HGetAll(ctx, "h").Result()
	if len(m) != 2 || m["f1"] != "v1" || m["f2"] != "v2" {
		t.Errorf("HGetAll: expected {f1:v1, f2:v2}, got %+v", m)
	}
	n, _ := c.HDel(ctx, "h", "f1").Result()
	if n != 1 {
		t.Errorf("HDel: expected 1, got %d", n)
	}
}

func TestListOps(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	if _, err := c.LPush(ctx, "l", "a", "b", "c").Result(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RPush(ctx, "l", "d").Result(); err != nil {
		t.Fatal(err)
	}
	// list = [c b a d]
	v, _ := c.LPop(ctx, "l").Result()
	if v != "c" {
		t.Errorf("LPop: expected c, got %q", v)
	}
	v, _ = c.RPop(ctx, "l").Result()
	if v != "d" {
		t.Errorf("RPop: expected d, got %q", v)
	}
}

func TestSetOps(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	_ = c
	_ = context.Background()
}

func TestListAndSetOps(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	// List: LRange / LLen
	_, _ = c.RPush(ctx, "l", "a", "b", "c").Result()
	r, _ := c.LRange(ctx, "l", 0, -1).Result()
	if len(r) != 3 || r[0] != "a" || r[2] != "c" {
		t.Errorf("LRange: expected [a b c], got %v", r)
	}
	n, _ := c.LLen(ctx, "l").Result()
	if n != 3 {
		t.Errorf("LLen: expected 3, got %d", n)
	}

	// Set: SAdd / SMembers / SIsMember / SRem
	_, _ = c.SAdd(ctx, "s", "x", "y", "z").Result()
	members, _ := c.SMembers(ctx, "s").Result()
	if len(members) != 3 {
		t.Errorf("SMembers: expected 3, got %v", members)
	}
	isMember, _ := c.SIsMember(ctx, "s", "x").Result()
	if !isMember {
		t.Error("SIsMember x: expected true")
	}
	isMember, _ = c.SIsMember(ctx, "s", "missing").Result()
	if isMember {
		t.Error("SIsMember missing: expected false")
	}
	n, _ = c.SRem(ctx, "s", "x").Result()
	if n != 1 {
		t.Errorf("SRem: expected 1, got %d", n)
	}
}

func TestBatch_OtherOps(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	b := c.NewBatch()
	b.Set(ctx, "k1", "v1", 0)
	b.Del(ctx, "k1", "missing")
	b.HSet(ctx, "h", "f1", "v1")
	b.HGet(ctx, "h", "f1")
	b.Expire(ctx, "h", time.Minute)
	b.Exists(ctx, "h")
	cmds, err := b.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 6 {
		t.Errorf("expected 6 cmds, got %d", len(cmds))
	}
}

func TestLua_RO(t *testing.T) {
	c, _ := newTestClient(t, "")
	ctx := context.Background()

	sha, err := c.ScriptLoad(ctx, "return 7").Result()
	if err != nil {
		t.Fatal(err)
	}

	res, err := c.EvalRO(ctx, "return 7", []string{}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := res.(int64); !ok || v != 7 {
		t.Errorf("EvalRO: expected int64(7), got %v (%T)", res, res)
	}

	res, err = c.EvalShaRO(ctx, sha, []string{}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := res.(int64); !ok || v != 7 {
		t.Errorf("EvalShaRO: expected int64(7), got %v (%T)", res, res)
	}
}

func TestSortedSetOps(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	members := []redis.Z{
		{Score: 1, Member: "a"},
		{Score: 2, Member: "b"},
		{Score: 3, Member: "c"},
	}
	if _, err := c.ZAdd(ctx, "z", members...).Result(); err != nil {
		t.Fatal(err)
	}
	card, _ := c.ZCard(ctx, "z").Result()
	if card != 3 {
		t.Errorf("ZCard: expected 3, got %d", card)
	}
	r, _ := c.ZRange(ctx, "z", 0, -1).Result()
	if len(r) != 3 || r[0] != "a" || r[2] != "c" {
		t.Errorf("ZRange: expected [a b c], got %v", r)
	}
	withScores, _ := c.ZRangeWithScores(ctx, "z", 0, -1).Result()
	if len(withScores) != 3 || withScores[2].Score != 3 {
		t.Errorf("ZRangeWithScores: unexpected %+v", withScores)
	}
	n, _ := c.ZRem(ctx, "z", "a").Result()
	if n != 1 {
		t.Errorf("ZRem: expected 1, got %d", n)
	}
}

func TestPipeline_TxPipeline(t *testing.T) {
	// 基础执行语义（空 prefix 无键转换）；带前缀落同键的回归见
	// pipeline_convertkey_test.go（redisx#79）。
	c, _ := newTestClient(t, "")
	ctx := context.Background()

	pipe := c.Pipeline()
	pipe.Set(ctx, "k1", "v1", 0)
	pipe.Set(ctx, "k2", "v2", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.Get(ctx, "k1").Result(); got != "v1" {
		t.Errorf("expected k1=v1, got %q", got)
	}

	// TxPipeline: 事务
	tx := c.TxPipeline()
	tx.Set(ctx, "tx-k", "10", 0)
	tx.Incr(ctx, "tx-k")
	if _, err := tx.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	// Set("10") + Incr -> 10 + 1 = 11
	got, _ := c.Get(ctx, "tx-k").Result()
	if got != "11" {
		t.Errorf("TxPipeline Incr after Set: expected 11, got %q", got)
	}
}

func TestPubSub(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub := c.Subscribe(ctx, "chan")
	defer sub.Close()
	// Wait for subscription to be ready
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	n, err := c.Publish(ctx, "chan", "hello").Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 subscriber, got %d", n)
	}

	msg, err := sub.ReceiveMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Payload != "hello" {
		t.Errorf("expected payload 'hello', got %q", msg.Payload)
	}
}

func TestLua_Eval(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	// 简单 Lua: INCR + 返 0
	res, err := c.Eval(ctx, "return 0", []string{}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := res.(int64); !ok || v != 0 {
		t.Errorf("expected int64(0), got %v (%T)", res, res)
	}

	// ScriptLoad + EvalSha
	sha, err := c.ScriptLoad(ctx, "return 1").Result()
	if err != nil {
		t.Fatal(err)
	}
	if sha == "" {
		t.Error("expected non-empty SHA")
	}
	exists, _ := c.ScriptExists(ctx, sha).Result()
	if len(exists) != 1 || !exists[0] {
		t.Errorf("ScriptExists: expected [true], got %v", exists)
	}
	res, err = c.EvalSha(ctx, sha, []string{}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := res.(int64); !ok || v != 1 {
		t.Errorf("expected int64(1), got %v (%T)", res, res)
	}
}

func TestBatch_Exec(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	b := c.NewBatch()
	b.Set(ctx, "k1", "v1", 0)
	b.Set(ctx, "k2", "v2", 0)
	b.Incr(ctx, "counter")
	b.IncrBy(ctx, "counter", 4)
	if b.Len() != 4 {
		t.Errorf("expected batch length 4, got %d", b.Len())
	}

	cmds, err := b.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cmds) != 4 {
		t.Errorf("expected 4 cmds returned, got %d", len(cmds))
	}

	// Verify state
	if got, _ := c.Get(ctx, "k1").Result(); got != "v1" {
		t.Errorf("k1: expected v1, got %q", got)
	}
	if got, _ := c.Get(ctx, "counter").Result(); got != "5" {
		t.Errorf("counter: expected 5, got %q", got)
	}

	// Discard 清空
	b.Discard()
	if b.Len() != 0 {
		t.Errorf("expected batch length 0 after Discard, got %d", b.Len())
	}
}

func TestCounter_HighLevel(t *testing.T) {
	// 用空 prefix 避免 Counter 内部对 key 加 prefix 跟测试期待不一致。
	c, _ := newTestClient(t, "")
	ctx := context.Background()
	counter := c.NewCounter("hits")

	// Incr(delta=1) -> Incr path
	n, err := counter.Incr(ctx, 1)
	if err != nil || n != 1 {
		t.Errorf("Incr(1): expected 1, got %d err=%v", n, err)
	}

	// Incr(delta=5) -> IncrBy path
	n, err = counter.Incr(ctx, 5)
	if err != nil || n != 6 {
		t.Errorf("Incr(5): expected 6, got %d err=%v", n, err)
	}

	// Get
	n, err = counter.Get(ctx)
	if err != nil || n != 6 {
		t.Errorf("Get: expected 6, got %d err=%v", n, err)
	}

	// Decr
	n, err = counter.Decr(ctx, 1)
	if err != nil || n != 5 {
		t.Errorf("Decr(1): expected 5, got %d err=%v", n, err)
	}

	// Decr(delta=3) -> DecrBy path
	n, err = counter.Decr(ctx, 3)
	if err != nil || n != 2 {
		t.Errorf("Decr(3): expected 2, got %d err=%v", n, err)
	}

	// IncrWithTTL: 新 key 应设置 TTL
	n, err = counter.IncrWithTTL(ctx, 1, 5*time.Second)
	if err != nil || n != 3 {
		t.Errorf("IncrWithTTL: expected 3, got %d err=%v", n, err)
	}
	ttl, _ := c.Client().TTL(ctx, "hits").Result()
	if ttl <= 0 || ttl > 5*time.Second {
		t.Errorf("expected TTL within (0, 5s], got %v", ttl)
	}

	// IncrWithTTL: 已有 TTL, 不应改变 TTL
	_, err = counter.IncrWithTTL(ctx, 1, 100*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ttl2, _ := c.Client().TTL(ctx, "hits").Result()
	if ttl2 > 5*time.Second+time.Second {
		t.Errorf("expected original TTL preserved, got %v (was %v)", ttl2, ttl)
	}

	// Reset
	if err := counter.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ = counter.Get(ctx)
	if n != 0 {
		t.Errorf("after Reset, Get should return 0, got %d", n)
	}
}

func TestCounter_Get_NonExistent(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	counter := c.NewCounter("never-set")
	n, err := counter.Get(context.Background())
	if err != nil {
		t.Errorf("expected nil error for missing key, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 for missing key, got %d", n)
	}
}

func TestNewClientFromConfig(t *testing.T) {
	// NewClientFromConfig 需要从 nacos 拉配置, 这里只测 error 路径
	_, err := NewClientFromConfig(context.Background(), RedisConfig{})
	if err == nil {
		t.Error("expected error for empty config (no Nacos available)")
	}
}

func TestNewClient_InvalidAddr(t *testing.T) {
	// 不可用地址：客户端是惰性连接，NewClient 不做强制 tracing 注入后
	// 构造恒成功；连接失败只在第一次命令时暴露。
	opt := &redis.UniversalOptions{Addrs: []string{"127.0.0.1:1"}}
	c, err := NewClient(opt)
	if err != nil {
		t.Fatalf("expected NewClient to succeed (lazy connect), got %v", err)
	}
	defer c.Close(context.Background())
	if c == nil {
		t.Error("expected non-nil client")
	}
	// 调一次命令验证 connect 行为
	pingCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = c.Ping(pingCtx).Err()
	if err == nil {
		t.Error("expected ping to fail for unreachable address")
	}
}

// 工具: str to int (避免引入 strconv 当只是用一次)
var _ = strconv.Itoa

// TestCounter_LuaKeysUsePrefix 回归：DecrFloor/IncrWithTTL 的 Lua 脚本走
// 裸 client，key 曾漏过 convertKey —— 与 Get/Incr 等前缀感知路径操作不同
// 的物理 key（配额读写分裂）。钉住：Lua 路径读写同一个带前缀 key。
func TestCounter_LuaKeysUsePrefix(t *testing.T) {
	c, mr := newTestClient(t, "ekit")
	ctx := context.Background()
	counter := c.NewCounter("quota")

	if _, err := counter.DecrFloor(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := counter.IncrWithTTL(ctx, 1, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	// 物理上只应存在带前缀的 key
	if !mr.Exists("ekit:quota") {
		t.Fatalf("prefixed key ekit:quota missing, keys=%v", mr.Keys())
	}
	for _, k := range mr.Keys() {
		if k == "quota" {
			t.Errorf("bare key %q exists: Lua path skipped prefix (read/write split)", k)
		}
	}

	// 前缀感知读路径与 Lua 写路径一致
	n, err := counter.Get(ctx)
	if err != nil || n != 1 {
		t.Errorf("Get after DecrFloor+IncrWithTTL = %d err=%v, want 1", n, err)
	}
}
