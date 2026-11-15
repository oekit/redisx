package redisx

import (
	"context"
	"net"
	"strconv"
	"strings"
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

// TestSet_ZeroExpirationIsPermanent 回归（redisx#216 文档口径）：Client.Set
// 的 expiration=0 直通 go-redis 语义——不下发 EX，写**永久键**（miniredis
// TTL 为 0 即无过期）；正值才带 TTL。该口径已写入 Client.Set/Pipeline.Set
// godoc 与包文档示例（示例改用正 TTL），测试钉住行为与文档一致——防止
// 后续有人把 0「修」成 cache 包的 24h 缺省口径而不改文档。
func TestSet_ZeroExpirationIsPermanent(t *testing.T) {
	c, mr := newTestClient(t, "x:")
	ctx := context.Background()

	// 物理键经 convertKey 取（prefix "x:" 再拼一个冒号 → "x::perm"）；
	// Exists 一并断言，避免「键不存在」与「永久键」都得到 TTL=0 的假通过。
	if err := c.Set(ctx, "perm", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	permKey := c.convertKey("perm")
	if !mr.Exists(permKey) {
		t.Fatalf("键 %q 应存在", permKey)
	}
	if ttl := mr.TTL(permKey); ttl != 0 {
		t.Errorf("expiration=0 应写永久键（无 TTL），got TTL=%v", ttl)
	}

	if err := c.Set(ctx, "ttl", "v", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	ttlKey := c.convertKey("ttl")
	if !mr.Exists(ttlKey) {
		t.Fatalf("键 %q 应存在", ttlKey)
	}
	if ttl := mr.TTL(ttlKey); ttl != time.Minute {
		t.Errorf("expiration>0 应带 TTL，got TTL=%v", ttl)
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

func TestPipeline_OtherOps(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	b := c.Pipeline()
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

func TestPipeline_Exec(t *testing.T) {
	c, _ := newTestClient(t, "x:")
	ctx := context.Background()

	b := c.Pipeline()
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

// TestPipeline_Discard_AfterExec_SafeNoOp 回归（redisx#169 复核口径）：对已
// Exec 的管道再 Discard 是安全 no-op——go-redis v9 的 Pipeliner.Discard 无
// 返回值（仅重置 cmds 切片，pipeline.go:94-96），「已 Exec 后 Discard 报
// 'pipeliner has been executed'」是 v8 行为，v9 无此错误面，包装无可透传
// 的 error。钉住：Exec 后 Discard 不 panic、队列归零、且不会把已执行的命令
// 再投递一遍（重复 Exec 只拿到空结果）。
func TestPipeline_Discard_AfterExec_SafeNoOp(t *testing.T) {
	c, _ := newTestClient(t, "")
	ctx := context.Background()

	p := c.Pipeline()
	p.Set(ctx, "discard:no-op", "v", time.Minute)
	if _, err := p.Exec(ctx); err != nil {
		t.Fatalf("exec: %v", err)
	}
	if p.Len() != 0 {
		t.Fatalf("queue must be drained after Exec, len=%d", p.Len())
	}

	p.Discard() // 已 Exec 的管道上 Discard：不得 panic
	if p.Len() != 0 {
		t.Fatalf("queue must stay empty after post-Exec Discard, len=%d", p.Len())
	}
	if _, err := c.Get(ctx, "discard:no-op").Result(); err != nil {
		t.Fatalf("previously executed command's effect must persist: %v", err)
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
	// 空配置：Host 为空构造期 fail-fast（#217），不再拨 ":6379" 碰运气
	_, err := NewClientFromConfig(context.Background(), RedisConfig{})
	if err == nil {
		t.Error("expected error for empty config")
	}
	if !strings.Contains(err.Error(), "redis.host") {
		t.Errorf("错误应点名配置键 redis.host（#217），got %v", err)
	}
}

// TestNewClientFromConfig_ValidatesHostPort 回归（redisx#217）：漏配
// redis.host 时旧实现拼出 ":6379"——Go dial 解析为本机回环，本地有 Redis
// 即 Ping 通过、数据写进错误实例且零日志；Port 越界同理停在晦涩的 dial
// 错误。钉住：两者都在构造期 fail-fast 且点名配置键，不发起拨号。
func TestNewClientFromConfig_ValidatesHostPort(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		cfg  RedisConfig
		want string
	}{
		{"host 为空", RedisConfig{Port: 6379}, "redis.host"},
		{"port 0（YAML 漏配）", RedisConfig{Host: "127.0.0.1"}, "redis.port"},
		{"port 超上界", RedisConfig{Host: "127.0.0.1", Port: 65536}, "redis.port"},
		{"port 负值", RedisConfig{Host: "127.0.0.1", Port: -1}, "redis.port"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewClientFromConfig(ctx, tc.cfg)
			if err == nil {
				t.Fatal("非法配置必须构造期报错")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("错误应点名 %s，got %v", tc.want, err)
			}
		})
	}
}

// TestNewClientFromConfig_OK 校验通过路径仍连通（miniredis）：验证只拦
// 非法配置，不误伤正常 host:port（含从 mr.Addr() 拆出的随机端口）。
func TestNewClientFromConfig_OK(t *testing.T) {
	mr := miniredis.RunT(t)
	host, portStr, err := net.SplitHostPort(mr.Addr())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewClientFromConfig(context.Background(), RedisConfig{Host: host, Port: port})
	if err != nil {
		t.Fatalf("合法配置不应报错，got %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Errorf("Ping 应通过，got %v", err)
	}
}

// TestNewClientFromConfig_IPv6JoinHostPort 回归（redisx#217 附带）：Host 为
// IPv6 字面量时 net.JoinHostPort 括号化——"%s:%d" 会拼出 "::1:6379" 非法
// 地址，错误停在 dial 期且不点名 host。这里只验地址形态（不真拨号），
// 通过拆解校验后的结果无法直接拿到，故用同口径拼接断言。
func TestNewClientFromConfig_IPv6JoinHostPort(t *testing.T) {
	got := net.JoinHostPort("::1", strconv.Itoa(6379))
	if got != "[::1]:6379" {
		t.Errorf("IPv6 地址应括号化，got %q", got)
	}
	// 真实构造路径：IPv6 host 校验通过后进入拨号，miniredis 不监听 ::1，
	// 断言错误来自拨号而非校验（即校验没把合法 IPv6 拦掉）。
	_, err := NewClientFromConfig(context.Background(), RedisConfig{Host: "::1", Port: 1})
	if err == nil {
		t.Fatal("未监听的地址应报错")
	}
	if strings.Contains(err.Error(), "redis.host") || strings.Contains(err.Error(), "redis.port") {
		t.Errorf("合法 IPv6 host/port 不应触发校验错误，got %v", err)
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
