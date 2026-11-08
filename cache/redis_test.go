package cache

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	cachepkg "github.com/oekit/pkg/cache"
)

// testUser 测试用数据类型。
type testUser struct {
	ID   int
	Name string
}

// newTestRedisClient 启动一个内存版 Redis (miniredis) 并返回真实 go-redis client.
func newTestRedisClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client, mr
}

func TestRedisCache_GetSet(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "test:",
		TTL:    time.Minute,
	})
	ctx := context.Background()

	user := &testUser{ID: 1, Name: "alice"}
	if err := c.Set(ctx, "user:1", user); err != nil {
		t.Fatal(err)
	}

	got, err := c.Get(ctx, "user:1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != 1 || got.Name != "alice" {
		t.Errorf("expected alice, got %+v", got)
	}
}

func TestRedisCache_Get_Miss(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	got, err := c.Get(ctx, "missing")
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestRedisCache_Get_NullValueMarker(t *testing.T) {
	client, mr := newTestRedisClient(t)
	if err := mr.Set("x:null-key", nullValue); err != nil {
		t.Fatal(err)
	}
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	got, err := c.Get(ctx, "null-key")
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for null marker, got %+v", got)
	}
}

func TestRedisCache_Get_DeserializeError(t *testing.T) {
	client, mr := newTestRedisClient(t)
	if err := mr.Set("x:bad", "not valid json"); err != nil {
		t.Fatal(err)
	}
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	_, err := c.Get(ctx, "bad")
	if err == nil {
		t.Error("expected deserialize error, got nil")
	}
}

func TestRedisCache_SetWithTTL(t *testing.T) {
	client, mr := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	user := &testUser{ID: 1, Name: "ttl-test"}
	if err := c.SetWithTTL(ctx, "k", user, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	got, _ := c.Get(ctx, "k")
	if got == nil {
		t.Fatal("expected value before TTL expires")
	}

	mr.FastForward(100 * time.Millisecond) // advance miniredis time

	got, err := c.Get(ctx, "k")
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil after TTL, got %+v", got)
	}
}

func TestRedisCache_SetRaw_GetBytes(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	raw := []byte(`{"id":42,"name":"raw"}`)
	if err := c.SetRaw(ctx, "raw-key", raw, time.Minute); err != nil {
		t.Fatal(err)
	}

	got, err := c.GetBytes(ctx, "raw-key")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(raw) {
		t.Errorf("expected %q, got %q", raw, got)
	}

	// GetBytes on null marker
	if setErr := c.SetRaw(ctx, "null-marker", []byte(nullValue), time.Minute); setErr != nil {
		t.Fatal(setErr)
	}
	got, err = c.GetBytes(ctx, "null-marker")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil for null marker, got %v", got)
	}
}

func TestRedisCache_Del_DelBatch_Exists(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	for i, k := range []string{"a", "b", "c"} {
		v := &testUser{ID: i, Name: k}
		if err := c.Set(ctx, k, v); err != nil {
			t.Fatal(err)
		}
	}

	// Exists
	for _, k := range []string{"a", "b", "c"} {
		exists, err := c.Exists(ctx, k)
		if err != nil || !exists {
			t.Errorf("expected %q to exist, got exists=%v err=%v", k, exists, err)
		}
	}

	// Del single
	if err := c.Del(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	exists, _ := c.Exists(ctx, "a")
	if exists {
		t.Error("expected a to be deleted")
	}

	// DelBatch
	if err := c.DelBatch(ctx, "b", "c", "missing"); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"b", "c"} {
		exists, _ := c.Exists(ctx, k)
		if exists {
			t.Errorf("expected %q to be deleted", k)
		}
	}
}

func TestRedisCache_GetOrLoad_Hit(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:", TTL: time.Minute})
	ctx := context.Background()

	_ = c.Set(ctx, "k", &testUser{ID: 1, Name: "cached"})

	var sourceCalled bool
	c.cfg.Source = func(ctx context.Context, key string) (*testUser, error) {
		sourceCalled = true
		return nil, nil
	}

	got, err := c.GetOrLoad(ctx, "k")
	if err != nil || got == nil || got.Name != "cached" {
		t.Errorf("expected cached value, got %+v err=%v", got, err)
	}
	if sourceCalled {
		t.Error("Source should not be called on cache hit")
	}
}

func TestRedisCache_GetOrLoad_MissLoadAndFill(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "x:",
		TTL:    time.Minute,
		Source: func(ctx context.Context, key string) (*testUser, error) {
			return &testUser{ID: 7, Name: "loaded"}, nil
		},
	})
	ctx := context.Background()

	// First call: miss → load + fill
	got, err := c.GetOrLoad(ctx, "k")
	if err != nil || got == nil || got.Name != "loaded" {
		t.Errorf("expected loaded, got %+v err=%v", got, err)
	}

	// Second call: should hit cache (no Source call)
	c.cfg.Source = func(ctx context.Context, key string) (*testUser, error) {
		t.Error("Source should not be called on second call (cache hit)")
		return nil, nil
	}
	got, err = c.GetOrLoad(ctx, "k")
	if err != nil || got == nil || got.Name != "loaded" {
		t.Errorf("expected cached, got %+v err=%v", got, err)
	}
}

func TestRedisCache_GetOrLoad_NilSourceOnMiss(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	got, err := c.GetOrLoad(ctx, "missing")
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestRedisCache_GetOrLoad_SourceError(t *testing.T) {
	client, _ := newTestRedisClient(t)
	sentinel := errors.New("source down")
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "x:",
		Source: func(ctx context.Context, key string) (*testUser, error) {
			return nil, sentinel
		},
	})
	ctx := context.Background()

	got, err := c.GetOrLoad(ctx, "k")
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestRedisCache_GetOrLoad_NilSourceValue_StoresNullMarker(t *testing.T) {
	// 验证 Source 返回 nil 时, null marker 被写入 Redis, 后续 GetOrLoad
	// 命中 null marker 时**不会**再调 Source（防穿透正确工作）。
	client, _ := newTestRedisClient(t)
	var sourceCalls int
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix:  "x:",
		TTL:     time.Minute,
		NullTTL: time.Minute,
		Source: func(ctx context.Context, key string) (*testUser, error) {
			sourceCalls++
			return nil, nil
		},
	})
	ctx := context.Background()

	// 第一次：Source 返回 nil，应当存储 null marker
	got, err := c.GetOrLoad(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
	if sourceCalls != 1 {
		t.Errorf("expected 1 Source call on first GetOrLoad, got %d", sourceCalls)
	}

	// 第二次 GetOrLoad：应当命中 null marker，**不**再调 Source
	got, err = c.GetOrLoad(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil from second GetOrLoad (null marker hit), got %+v", got)
	}
	if sourceCalls != 1 {
		t.Errorf("expected still 1 Source call (null marker 防穿透), got %d", sourceCalls)
	}

	// 直接 Get 也应当识别 null marker
	got, err = c.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil from Get (null marker hit), got %+v", got)
	}
}

func TestRedisCache_GetInternal_NullMarkerVsMiss(t *testing.T) {
	// getInternal 必须能区分 null marker 命中 (isNullMarker=true) 和 key 不存在。
	// 这是 GetOrLoad 防穿透逻辑的支撑。
	client, mr := newTestRedisClient(t)
	if err := mr.Set("x:present", nullValue); err != nil {
		t.Fatal(err)
	}
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	// null marker 命中
	val, isNull, err := c.getInternal(ctx, "present")
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		t.Errorf("expected nil value, got %+v", val)
	}
	if !isNull {
		t.Error("expected isNullMarker=true for null marker hit")
	}

	// key 不存在
	val, isNull, err = c.getInternal(ctx, "missing")
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		t.Errorf("expected nil value, got %+v", val)
	}
	if isNull {
		t.Error("expected isNullMarker=false for key not present")
	}
}

func TestRedisCache_Refresh(t *testing.T) {
	client, _ := newTestRedisClient(t)
	var loadCount int
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "x:",
		TTL:    time.Minute,
		Source: func(ctx context.Context, key string) (*testUser, error) {
			loadCount++
			return &testUser{ID: loadCount, Name: key}, nil
		},
	})
	ctx := context.Background()

	// 先填一次
	_ = c.Set(ctx, "k", &testUser{ID: 1, Name: "v1"})

	// Refresh 应删除后重新加载
	got, err := c.Refresh(ctx, "k")
	if err != nil || got == nil || got.ID != 1 {
		t.Errorf("first refresh: got %+v err=%v", got, err)
	}
	if loadCount != 1 {
		t.Errorf("expected 1 Source call after first refresh, got %d", loadCount)
	}
}

func TestRedisCache_FullKey(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "app:user:"})
	if got := c.fullKey("123"); got != "app:user:123" {
		t.Errorf("expected 'app:user:123', got %q", got)
	}
}

func TestRedisCache_NullTTL_Default(t *testing.T) {
	client, _ := newTestRedisClient(t)
	// NullTTL=0（未设置）使用默认值 1 分钟（回归测试：此前 0 会静默禁用
	// 空值缓存，与文档「默认 1 分钟」相反）
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	if c.nullTTL != time.Minute {
		t.Errorf("expected nullTTL default 1m, got %v", c.nullTTL)
	}
}

func TestRedisCache_NullTTL_Disabled(t *testing.T) {
	client, _ := newTestRedisClient(t)
	// NullTTL=-1 显式禁用空值缓存（内部保留负值，>0 判断不成立）
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:", NullTTL: -1})
	if c.nullTTL >= 0 {
		t.Errorf("expected nullTTL disabled (negative), got %v", c.nullTTL)
	}
}

func TestRedisCache_NullTTL_Custom(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix:  "x:",
		NullTTL: 5 * time.Minute,
	})
	if c.nullTTL != 5*time.Minute {
		t.Errorf("expected 5m, got %v", c.nullTTL)
	}
}

func TestRedisCache_CustomSerializerDeserializer(t *testing.T) {
	client, _ := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "x:",
		TTL:    time.Minute,
		Serializer: func(v *testUser) ([]byte, error) {
			return []byte(v.Name + ":" + string(rune('0'+v.ID))), nil
		},
		Deserializer: func(data []byte, v *testUser) error {
			v.ID = int(data[len(data)-1] - '0')
			v.Name = string(data[:len(data)-2])
			return nil
		},
	})
	ctx := context.Background()

	_ = c.Set(ctx, "k", &testUser{ID: 5, Name: "bob"})
	got, err := c.Get(ctx, "k")
	if err != nil || got == nil || got.Name != "bob" || got.ID != 5 {
		t.Errorf("roundtrip failed, got %+v err=%v", got, err)
	}
}

func TestSingleFlightCache_OverRedis(t *testing.T) {
	// 验证 singleflight 包装 RedisCache 走完整 Get/Set/Del/Exists/DelBatch/Refresh 路径。
	client, _ := newTestRedisClient(t)
	var sourceCalls int
	inner := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "x:",
		TTL:    time.Minute,
		Source: func(ctx context.Context, key string) (*testUser, error) {
			sourceCalls++
			return &testUser{ID: sourceCalls, Name: key}, nil
		},
	})
	sf := cachepkg.NewSingleFlightCache(inner)
	ctx := context.Background()

	// Get hit
	if setErr := sf.Set(ctx, "k", &testUser{ID: 1, Name: "cached"}); setErr != nil {
		t.Fatal(setErr)
	}
	got, err := sf.Get(ctx, "k")
	if err != nil || got == nil || got.Name != "cached" {
		t.Errorf("expected cached, got %+v err=%v", got, err)
	}

	// Exists / Del / DelBatch
	if exists, _ := sf.Exists(ctx, "k"); !exists {
		t.Error("expected k to exist")
	}
	_ = sf.Del(ctx, "k")
	exists, _ := sf.Exists(ctx, "k")
	if exists {
		t.Error("expected k to be deleted")
	}

	_ = sf.SetWithTTL(ctx, "k2", &testUser{ID: 2, Name: "y"}, time.Minute)
	_ = sf.SetWithTTL(ctx, "k3", &testUser{ID: 3, Name: "z"}, time.Minute)
	if batchErr := sf.DelBatch(ctx, "k2", "k3"); batchErr != nil {
		t.Fatal(batchErr)
	}
	exists, _ = sf.Exists(ctx, "k2")
	if exists {
		t.Error("expected k2 to be deleted by DelBatch")
	}

	// GetOrLoad: miss → Source call → fill cache
	got, err = sf.GetOrLoad(ctx, "fresh")
	if err != nil || got == nil || got.Name != "fresh" {
		t.Errorf("expected Source-loaded, got %+v err=%v", got, err)
	}
	if sourceCalls != 1 {
		t.Errorf("expected 1 Source call, got %d", sourceCalls)
	}

	// GetOrLoad: hit (no extra Source call)
	got, err = sf.GetOrLoad(ctx, "fresh")
	if err != nil || got == nil || got.ID != 1 {
		t.Errorf("expected cached ID=1, got %+v err=%v", got, err)
	}
	if sourceCalls != 1 {
		t.Errorf("expected still 1 Source call (hit), got %d", sourceCalls)
	}

	// Refresh: re-load from Source
	got, err = sf.Refresh(ctx, "fresh")
	if err != nil || got == nil || got.ID != 2 {
		t.Errorf("expected refreshed ID=2, got %+v err=%v", got, err)
	}
	if sourceCalls != 2 {
		t.Errorf("expected 2 Source calls after refresh, got %d", sourceCalls)
	}
}

// TestRedisCache_SetRawEmptyBytes_NotNullMarker 回归：getInternal 曾把
// len==0 一并当 null marker——SetRaw 合法写入的空字节被永久伪造成
// "数据不存在"，GetOrLoad 不再调 Source。钉住：只有精确哨兵 __NULL__
// 才是 null marker，且 GetBytes 与 getInternal 口径一致。
func TestRedisCache_SetRawEmptyBytes_NotNullMarker(t *testing.T) {
	client, _ := newTestRedisClient(t)
	calls := 0
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "test:",
		TTL:    time.Minute,
		Source: func(ctx context.Context, key string) (*testUser, error) {
			calls++
			return &testUser{ID: 7, Name: "source"}, nil
		},
	})
	ctx := context.Background()

	if err := c.SetRaw(ctx, "empty-k", []byte{}, time.Minute); err != nil {
		t.Fatal(err)
	}

	// 空字节不算 null marker：GetOrLoad 正常走 Source 回填
	got, err := c.GetOrLoad(ctx, "empty-k")
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Errorf("Source should be called once for empty-bytes key, got %d (treated as permanent null marker?)", calls)
	}
	if got == nil || got.ID != 7 {
		t.Errorf("expected source value, got %+v", got)
	}

	// GetBytes 口径一致：空字节原样返回，不返回 nil
	raw, err := c.GetBytes(ctx, "empty-k")
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		t.Error("empty bytes must be returned as-is, not treated as null (GetBytes/getInternal inconsistent)")
	}
}

// TestRedisCache_SetRawLegacySentinel_NotNullMarker 回归（v0.4.0 哨兵迁移）：
// 旧哨兵 "__NULL__" 可被 SetRaw 合法写入精确碰撞——旧实现读路径把它误判为
// null marker（"数据不存在"，GetOrLoad 永久不回源）。新哨兵改为 \x00 保留
// 前缀后，该字面值必须作为普通数据往返。passthrough 编解码（raw 字节即
// 字符串）忠实还原"裸字节恰好等于旧哨兵"的碰撞场景。
func TestRedisCache_SetRawLegacySentinel_NotNullMarker(t *testing.T) {
	client, _ := newTestRedisClient(t)
	var sourceCalls int
	c := NewRedisCache[string](client, RedisConfig[string]{
		Prefix: "x:",
		TTL:    time.Minute,
		Serializer: func(v *string) ([]byte, error) {
			return []byte(*v), nil
		},
		Deserializer: func(data []byte, v *string) error {
			*v = string(data)
			return nil
		},
		Source: func(ctx context.Context, key string) (*string, error) {
			sourceCalls++
			s := "from-source"
			return &s, nil
		},
	})
	ctx := context.Background()

	if err := c.SetRaw(ctx, "legacy", []byte("__NULL__"), time.Minute); err != nil {
		t.Fatal(err)
	}

	// Get 必须返回 "__NULL__" 字符串本身，不得误判 null
	got, err := c.Get(ctx, "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != "__NULL__" {
		t.Errorf(`"__NULL__" 应作为普通值往返，got (%+v, %v)`, got, err)
	}

	// GetOrLoad 命中已存值：返回该值且不回源（旧实现误判 null 时返回 (nil, nil)）
	got, err = c.GetOrLoad(ctx, "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != "__NULL__" {
		t.Errorf(`GetOrLoad 应返回 "__NULL__" 本身，got (%+v, %v)`, got, err)
	}
	if sourceCalls != 0 {
		t.Errorf("已有缓存值不应回源，Source 被调用 %d 次", sourceCalls)
	}

	// GetBytes 原样返回
	raw, err := c.GetBytes(ctx, "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "__NULL__" {
		t.Errorf("GetBytes 应原样返回 %q, got %q", "__NULL__", raw)
	}

	// JSON 路径不受哨兵迁移影响：默认编解码走 json.Marshal（字符串带
	// 引号，写入 "\"__NULL__\""），从未与裸哨兵精确相等
	jc := NewRedisCache[string](client, RedisConfig[string]{Prefix: "x:", TTL: time.Minute})
	if setErr := jc.Set(ctx, "json-k", stringPtr("__NULL__")); setErr != nil {
		t.Fatal(setErr)
	}
	got, err = jc.Get(ctx, "json-k")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != "__NULL__" {
		t.Errorf("JSON 路径的 \"__NULL__\" 值应正常往返，got (%+v, %v)", got, err)
	}
}

// TestRedisCache_SetRawReservedPrefix_NullMarker 契约文档化测试（v0.4.0）：
// 以 \x00 开头的值保留为 null 标记前缀——SetRaw 写入精确哨兵字节后，读路径
// 按 null marker 处理（Get/GetBytes 返回 nil、GetOrLoad 不回源）。边界：
// \x00 开头但非精确哨兵的值**不认作** null（保留的是前缀空间，不是前缀
// 通配），走反序列化正常往返。
func TestRedisCache_SetRawReservedPrefix_NullMarker(t *testing.T) {
	client, _ := newTestRedisClient(t)
	var sourceCalls int
	c := NewRedisCache[string](client, RedisConfig[string]{
		Prefix: "x:",
		TTL:    time.Minute,
		Serializer: func(v *string) ([]byte, error) {
			return []byte(*v), nil
		},
		Deserializer: func(data []byte, v *string) error {
			*v = string(data)
			return nil
		},
		Source: func(ctx context.Context, key string) (*string, error) {
			sourceCalls++
			s := "from-source"
			return &s, nil
		},
	})
	ctx := context.Background()

	// 精确哨兵字节：按 null marker 处理
	if err := c.SetRaw(ctx, "reserved", []byte(nullValue), time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "reserved")
	if err != nil || got != nil {
		t.Errorf("保留哨兵应按 null marker 处理（Get 返回 (nil, nil)），got (%+v, %v)", got, err)
	}
	raw, err := c.GetBytes(ctx, "reserved")
	if err != nil || raw != nil {
		t.Errorf("GetBytes 对 null marker 应返回 nil，got (%v, %v)", raw, err)
	}
	if exists, e := c.Exists(ctx, "reserved"); e != nil || !exists {
		t.Errorf("null marker 应计存在（Exists=true），got exists=%v err=%v", exists, e)
	}
	got, err = c.GetOrLoad(ctx, "reserved")
	if err != nil || got != nil {
		t.Errorf("GetOrLoad 命中保留哨兵应返回 (nil, nil)，got (%+v, %v)", got, err)
	}
	if sourceCalls != 0 {
		t.Errorf("null marker 防穿透：Source 不应被调用，got %d 次", sourceCalls)
	}

	// 边界：\x00 开头但非精确哨兵 → 不认作 null（passthrough 正常往返）
	if rawErr := c.SetRaw(ctx, "boundary", []byte("\x00other"), time.Minute); rawErr != nil {
		t.Fatal(rawErr)
	}
	got, err = c.Get(ctx, "boundary")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != "\x00other" {
		t.Errorf("\\x00 开头非哨兵值不应被当作 null marker，got (%+v, %v)", got, err)
	}
}

func stringPtr(s string) *string { return &s }

func TestDefaultSerializer(t *testing.T) {
	user := &testUser{ID: 1, Name: "test"}
	data, err := defaultSerializer(user)
	if err != nil {
		t.Fatal(err)
	}

	var got testUser
	if err := defaultDeserializer(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.ID != 1 || got.Name != "test" {
		t.Errorf("expected {1 test}, got %+v", got)
	}
}

func TestDefaultSerializer_Nil(t *testing.T) {
	data, err := defaultSerializer[*testUser](nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "null" {
		t.Errorf("expected 'null', got %q", string(data))
	}
}

func TestRedisConfig_Defaults(t *testing.T) {
	cfg := RedisConfig[testUser]{
		Prefix: "test:",
		TTL:    5 * time.Minute,
		Source: func(ctx context.Context, key string) (*testUser, error) {
			return nil, nil
		},
	}

	if cfg.Prefix != "test:" {
		t.Errorf("expected 'test:', got %q", cfg.Prefix)
	}
	if cfg.TTL != 5*time.Minute {
		t.Errorf("expected 5m, got %v", cfg.TTL)
	}
}

func TestNullValue(t *testing.T) {
	// v0.4.0 起哨兵改为 \x00 保留前缀：合法 JSON 序列化输出不会出现以
	// 裸 \x00 开头的字节，SetRaw 写入的普通值不会自然碰撞（旧 "__NULL__"
	// 可被 SetRaw 精确碰撞，已废弃）
	const want = "\x00oekit:null"
	if nullValue != want {
		t.Errorf("expected %q, got %q", want, nullValue)
	}
	if !strings.HasPrefix(nullValue, "\x00") {
		t.Error("nullValue 必须以 \\x00 开头（保留前缀契约）")
	}
}

func TestFullKey(t *testing.T) {
	c := &RedisCache[testUser]{
		cfg: RedisConfig[testUser]{Prefix: "app:user:"},
	}
	if c.fullKey("123") != "app:user:123" {
		t.Errorf("expected 'app:user:123', got %q", c.fullKey("123"))
	}
}

func TestCustomSerializer(t *testing.T) {
	customSer := func(v *testUser) ([]byte, error) {
		return json.Marshal(struct {
			UserID   int    `json:"user_id"`
			UserName string `json:"user_name"`
		}{UserID: v.ID, UserName: v.Name})
	}

	cfg := RedisConfig[testUser]{
		Prefix:     "test:",
		TTL:        time.Minute,
		Serializer: customSer,
	}

	user := &testUser{ID: 1, Name: "test"}
	data, err := cfg.Serializer(user)
	if err != nil {
		t.Fatal(err)
	}

	// Verify custom format
	var m map[string]any
	_ = json.Unmarshal(data, &m)
	if m["user_id"].(float64) != 1 {
		t.Errorf("expected user_id=1, got %v", m["user_id"])
	}
	if m["user_name"].(string) != "test" {
		t.Errorf("expected user_name=test, got %v", m["user_name"])
	}
}

// TestRedisCache_SetWithTTL_GuardAgainstZeroTTL 表驱动（#113）：ttl<=0 必须
// fail-fast 返回错误、绝不落键——go-redis 对 TTL=0 不下发 EX ⇒ 此前静默写成
// 永久键，与同结构体 Set 的「TTL=0 → 24h 缺省」构成两种相反语义。护栏对齐
// 仓内 distlock/ratelimit 零值笔误 fail-fast 哲学。
func TestRedisCache_SetWithTTL_GuardAgainstZeroTTL(t *testing.T) {
	cases := []struct {
		name string
		ttl  time.Duration
	}{
		{"zero is permanent-key typo", 0},
		{"negative is also invalid", -1 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client, mr := newTestRedisClient(t)
			rc := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
			ctx := context.Background()

			err := rc.SetWithTTL(ctx, "k", &testUser{ID: 1, Name: "n"}, c.ttl)
			if err == nil {
				t.Fatalf("SetWithTTL(ttl=%s) 必须返回错误（#113）", c.ttl)
			}
			if !strings.Contains(err.Error(), "ttl") {
				t.Fatalf("错误信息必须指明 ttl 非法，got: %v", err)
			}
			// 关键反断言：报错路径不得留下任何键（尤其不得是永久键）。
			if mr.Exists("x:k") {
				t.Fatalf("SetWithTTL(ttl=%s) 报错后不得落键（旧实现落永久键，#113）", c.ttl)
			}
			// 底层键也不得存在（带前缀的全键口径）。
			if mr.Exists("x:k") {
				t.Fatal("full key must not exist after guard error")
			}
		})
	}
}

// TestRedisCache_SetWithTTL_PositiveTTLHasExpiry 钉住正 TTL 路径不受护栏影响：
// 写入成功且键带过期时间（miniredis TTL > 0），不是永久键。
func TestRedisCache_SetWithTTL_PositiveTTLHasExpiry(t *testing.T) {
	client, mr := newTestRedisClient(t)
	rc := NewRedisCache[testUser](client, RedisConfig[testUser]{Prefix: "x:"})
	ctx := context.Background()

	if err := rc.SetWithTTL(ctx, "k", &testUser{ID: 1, Name: "n"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	ttl := mr.TTL("x:k")
	if ttl <= 0 {
		// miniredis 对无 TTL 键返回 0；正 TTL 写入必须带过期（绝不永久键，#113）。
		t.Fatalf("positive TTL write must carry an expiry, got %s", ttl)
	}
}
