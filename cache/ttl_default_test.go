package cache

import (
	"context"
	"testing"
	"time"
)

// --- redisx#164：缺省 TTL（0→24h）口径单源 ---

// TestRedisCache_EffectiveTTL 单源推导的表测：0 → 24h 缺省；正值原样。
// GetOrLoad 回填与 Set 两个写入口径由 effectiveTTL 编译期绑定同一实现。
func TestRedisCache_EffectiveTTL(t *testing.T) {
	c := NewRedisCache[testUser](nil, RedisConfig[testUser]{Prefix: "t:"})
	if got := c.effectiveTTL(); got != 24*time.Hour {
		t.Fatalf("unset TTL must default to 24h, got %v", got)
	}

	c = NewRedisCache[testUser](nil, RedisConfig[testUser]{Prefix: "t:", TTL: 7 * time.Minute})
	if got := c.effectiveTTL(); got != 7*time.Minute {
		t.Fatalf("explicit TTL must pass through, got %v", got)
	}
}

// TestRedisCache_DefaultTTL_AppliedToWrites 集成钉：TTL 未设置（0）时，
// Set 与 GetOrLoad 回填两条写路径的键都必须带 24h 量级的过期（缺省生效），
// 而不是 go-redis 的「TTL=0 不下发 EX = 永久键」。
func TestRedisCache_DefaultTTL_AppliedToWrites(t *testing.T) {
	client, mr := newTestRedisClient(t)
	c := NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "dfttl:",
		Source: func(context.Context, string) (*testUser, error) {
			return &testUser{ID: 1, Name: "alice"}, nil
		},
	})
	ctx := context.Background()

	if err := c.Set(ctx, "via-set", &testUser{ID: 2, Name: "bob"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := c.GetOrLoad(ctx, "via-load"); err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}

	day := 24 * time.Hour
	for _, key := range []string{"dfttl:via-set", "dfttl:via-load"} {
		ttl := mr.TTL(key)
		if ttl <= 0 || ttl > day {
			t.Fatalf("%s ttl = %v, want in (0, 24h] (default TTL must be applied, not a permanent key)", key, ttl)
		}
	}
}
