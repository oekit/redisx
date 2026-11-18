package ratelimit

import "testing"

// --- redisx#242：bucketCache 单源契约（原 PeriodLimit.bucketFor 与
// ResilientLimiter.localFor 两份同构实现的公共语义直测） ---

// TestBucketCache_GetCachesSharedInstance get 首次 miss 调 build 并缓存，
// 此后同 key 跨调用共享同一实例（#125/#127：随用随建会让故障期间的每个
// 请求都拿到全新满桶，限流失效）。
func TestBucketCache_GetCachesSharedInstance(t *testing.T) {
	calls := 0
	c := bucketCache{build: func(string) LocalLimiter {
		calls++
		return newLocalBucket(1, 1)
	}}
	first := c.get("k")
	second := c.get("k")
	if first == nil || first != second {
		t.Fatalf("get must return the same cached instance, got %p then %p", first, second)
	}
	if calls != 1 {
		t.Fatalf("build called %d times, want 1 (cached across calls)", calls)
	}
}

// TestBucketCache_NilNotCached 工厂返回 nil 不缓存：条目即时回收，下次 get
// 重调工厂（按 key 动态提供/收回语义，#125/#127）。
func TestBucketCache_NilNotCached(t *testing.T) {
	provide := false
	calls := 0
	c := bucketCache{build: func(string) LocalLimiter {
		calls++
		if !provide {
			return nil
		}
		return newLocalBucket(1, 1)
	}}
	if got := c.get("k"); got != nil {
		t.Fatalf("nil build must surface nil, got %v", got)
	}
	if n := countBuckets(&c); n != 0 {
		t.Fatalf("nil result must not be cached, %d entries remain", n)
	}
	provide = true
	if got := c.get("k"); got == nil {
		t.Fatal("factory must be re-asked after nil (nil not cached)")
	}
	if calls != 2 {
		t.Fatalf("build called %d times, want 2 (nil not cached)", calls)
	}
}

// TestBucketCache_Clear 清空全部条目（回切/Close 清扫语义，#167）。
func TestBucketCache_Clear(t *testing.T) {
	c := bucketCache{build: func(string) LocalLimiter { return newLocalBucket(1, 1) }}
	c.get("k1")
	c.get("k2")
	if n := countBuckets(&c); n != 2 {
		t.Fatalf("expected 2 cached entries, got %d", n)
	}
	c.clear()
	if n := countBuckets(&c); n != 0 {
		t.Fatalf("clear must empty the cache, %d entries remain", n)
	}
}
