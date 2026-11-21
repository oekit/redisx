package cache

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// --- #258：旧版本 Set(key, nil) 写出 "null" 的升级兼容 ---
//
// #247 让 Set/SetWithTTL 对 nil fail-fast，但历史版本允许 Set(key, nil)，
// 默认 JSON 序列化写出的字节是字面 "null"。旧读路径对该字节调
// json.Unmarshal("null", &result) 无错 no-op ⇒ Get 返回 &T{} 零值伪命中、
// GetOrLoad 看到 result != nil 不回源、MGet 把零值放进结果。滚动升级后旧
// 条目在 TTL（缺省 24h）内把空壳当真实实体返回且无法自愈。
//
// 当前口径：使用内置编解码器时，精确 "null" 按 **miss** 处理——Get 返回
// (nil, nil)、GetOrLoad 回源并覆盖（首次读即自愈）、MGet 缺席。按 null
// marker 处理会短路 Source，旧条目将永远无法自愈，故必须是 miss。

// TestRedisCache_Get_LegacyJSONNullIsMiss 回归 #258：预置 "null" 字节后
// Get 必须返回 miss（(nil, nil)），不是 &testUser{} 零值伪命中；GetBytes
// 仍原样返回原始字节（纯字节入口不解释内容）。
func TestRedisCache_Get_LegacyJSONNullIsMiss(t *testing.T) {
	client, mr := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	if err := mr.Set("x:legacy", legacyJSONNull); err != nil {
		t.Fatal(err)
	}

	got, err := c.Get(ctx, "legacy")
	if err != nil {
		t.Fatalf("Get must treat legacy \"null\" as miss, got error: %v", err)
	}
	if got != nil {
		t.Fatalf("legacy \"null\" must be a miss, not a zero-value hit (#258), got %+v", got)
	}

	raw, err := c.GetBytes(ctx, "legacy")
	if err != nil {
		t.Fatalf("GetBytes: %v", err)
	}
	if string(raw) != legacyJSONNull {
		t.Fatalf("GetBytes must keep returning raw bytes unchanged, got %q", raw)
	}
}

// TestRedisCache_GetOrLoad_LegacyJSONNullHealsOnRead 回归 #258：旧 "null"
// 条目必须回源并覆盖——首次 GetOrLoad 返回真实数据、落库字节不再是 "null"，
// 后续读取不再回源（自愈后按正常命中路径）。
func TestRedisCache_GetOrLoad_LegacyJSONNullHealsOnRead(t *testing.T) {
	client, mr := newTestRedisClient(t)
	sourceCalls := 0
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "x:",
		TTL:    time.Minute,
		Source: func(ctx context.Context, key string) (*testUser, error) {
			sourceCalls++
			return &testUser{ID: 7, Name: "healed"}, nil
		},
	})
	ctx := context.Background()

	if err := mr.Set("x:legacy", legacyJSONNull); err != nil {
		t.Fatal(err)
	}

	got, err := c.GetOrLoad(ctx, "legacy")
	if err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if got == nil || got.ID != 7 || got.Name != "healed" {
		t.Fatalf("legacy \"null\" must trigger Source and return real data (#258), got %+v", got)
	}
	if sourceCalls != 1 {
		t.Fatalf("Source must be called exactly once, got %d", sourceCalls)
	}

	// 回源覆盖：落库字节已是真实序列化值，不再是 "null"。
	raw, err := client.Get(ctx, "x:legacy").Result()
	if err != nil {
		t.Fatalf("get healed entry: %v", err)
	}
	if raw == legacyJSONNull {
		t.Fatal("GetOrLoad must overwrite the legacy \"null\" entry (#258)")
	}
	var stored testUser
	if uerr := json.Unmarshal([]byte(raw), &stored); uerr != nil {
		t.Fatalf("healed entry must be valid JSON, got %q: %v", raw, uerr)
	}
	if stored.ID != 7 || stored.Name != "healed" {
		t.Fatalf("healed entry mismatch: %+v", stored)
	}

	// 自愈后第二次读取走正常命中，不再回源。
	again, err := c.GetOrLoad(ctx, "legacy")
	if err != nil || again == nil || again.ID != 7 {
		t.Fatalf("second GetOrLoad must hit healed entry, got (%+v, %v)", again, err)
	}
	if sourceCalls != 1 {
		t.Fatalf("healed entry must not trigger Source again, calls=%d", sourceCalls)
	}
}

// TestRedisCache_MGet_LegacyJSONNullAbsent 回归 #258：MGet 对旧 "null"
// 条目按缺席处理（消费方回退 GetOrLoad 时享受回源自愈），同批真实键照常命中。
func TestRedisCache_MGet_LegacyJSONNullAbsent(t *testing.T) {
	client, mr := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	if err := mr.Set("x:legacy", legacyJSONNull); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "real", &testUser{ID: 3, Name: "real"}); err != nil {
		t.Fatal(err)
	}

	got, err := c.MGet(ctx, "legacy", "real", "missing")
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if _, ok := got["legacy"]; ok {
		t.Fatalf("legacy \"null\" must be absent from MGet result (#258), got %+v", got["legacy"])
	}
	if v := got["real"]; v == nil || v.ID != 3 || v.Name != "real" {
		t.Fatalf("real key must still hit, got %+v", v)
	}
	if len(got) != 1 {
		t.Fatalf("expected only the real key in result, got %d entries: %+v", len(got), got)
	}
}

// TestRedisCache_LegacyJSONNull_CustomCodecNotInterpreted 钉住兼容范围：
// 注入自定义编解码器时 "null" 按调用方定义的语义处理，本实现不替它解释。
func TestRedisCache_LegacyJSONNull_CustomCodecNotInterpreted(t *testing.T) {
	client, mr := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "x:",
		Serializer: func(v *testUser) ([]byte, error) {
			return json.Marshal(v)
		},
		Deserializer: func(data []byte, v *testUser) error {
			if string(data) == legacyJSONNull {
				*v = testUser{ID: 99, Name: "custom-null"}
				return nil
			}
			return json.Unmarshal(data, v)
		},
	})
	ctx := context.Background()

	if err := mr.Set("x:custom", legacyJSONNull); err != nil {
		t.Fatal(err)
	}

	got, err := c.Get(ctx, "custom")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil || got.ID != 99 || got.Name != "custom-null" {
		t.Fatalf("custom deserializer must own the \"null\" semantics, got %+v", got)
	}
}
