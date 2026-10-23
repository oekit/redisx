package redisx

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// skipIfNoRedis 跳过需要 Redis 的测试。
func skipIfNoRedis(t *testing.T) *Client {
	t.Helper()
	client, err := NewClientFromAddr("localhost:6379", "", 0)
	if err != nil {
		t.Skip("Redis not available, skipping test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("Redis not available, skipping test")
	}
	return client
}

func TestBatch_SetGet(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	batch := client.NewBatch()

	// 批量 Set
	batch.Set(ctx, "batch:test1", "value1", time.Minute)
	batch.Set(ctx, "batch:test2", "value2", time.Minute)
	batch.Set(ctx, "batch:test3", "value3", time.Minute)

	// 批量 Get
	cmd1 := batch.Get(ctx, "batch:test1")
	cmd2 := batch.Get(ctx, "batch:test2")
	cmd3 := batch.Get(ctx, "batch:test3")

	// 执行
	if _, err := batch.Exec(ctx); err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	// 验证结果
	if v, err := cmd1.Result(); err != nil || v != "value1" {
		t.Errorf("expected 'value1', got %q, err: %v", v, err)
	}
	if v, err := cmd2.Result(); err != nil || v != "value2" {
		t.Errorf("expected 'value2', got %q, err: %v", v, err)
	}
	if v, err := cmd3.Result(); err != nil || v != "value3" {
		t.Errorf("expected 'value3', got %q, err: %v", v, err)
	}

	// 清理
	client.Del(ctx, "batch:test1", "batch:test2", "batch:test3")
}

func TestBatch_Len(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	batch := client.NewBatch()
	if batch.Len() != 0 {
		t.Errorf("expected len 0, got %d", batch.Len())
	}

	ctx := context.Background()
	batch.Set(ctx, "key1", "val1", time.Minute)
	batch.Set(ctx, "key2", "val2", time.Minute)

	if batch.Len() != 2 {
		t.Errorf("expected len 2, got %d", batch.Len())
	}

	batch.Discard()
	if batch.Len() != 0 {
		t.Errorf("expected len 0 after discard, got %d", batch.Len())
	}
}

func TestCounter_IncrDecr(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	counter := client.NewCounter("counter:test")

	// 重置
	_ = counter.Reset(ctx)

	// 初始值应该是 0
	v, err := counter.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if v != 0 {
		t.Errorf("expected 0, got %d", v)
	}

	// 增加
	v, err = counter.Incr(ctx, 5)
	if err != nil {
		t.Fatalf("Incr failed: %v", err)
	}
	if v != 5 {
		t.Errorf("expected 5, got %d", v)
	}

	// 减少
	v, err = counter.Decr(ctx, 2)
	if err != nil {
		t.Fatalf("Decr failed: %v", err)
	}
	if v != 3 {
		t.Errorf("expected 3, got %d", v)
	}

	// 清理
	_ = counter.Reset(ctx)
}

func TestCounter_IncrWithTTL(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	counter := client.NewCounter("counter:ttl:test")

	// 重置
	_ = counter.Reset(ctx)

	// 增加并设置 TTL
	v, err := counter.IncrWithTTL(ctx, 1, 10*time.Second)
	if err != nil {
		t.Fatalf("IncrWithTTL failed: %v", err)
	}
	if v != 1 {
		t.Errorf("expected 1, got %d", v)
	}

	// 再次增加（不应该重置 TTL）
	v, err = counter.IncrWithTTL(ctx, 1, 10*time.Second)
	if err != nil {
		t.Fatalf("IncrWithTTL failed: %v", err)
	}
	if v != 2 {
		t.Errorf("expected 2, got %d", v)
	}

	// 清理
	_ = counter.Reset(ctx)
}

func TestBloomFilter_AddExists(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	// 每次运行用唯一 key（redisx#41）：BF.ADD 的语义是「本次新添加」还是
	// 「元素已在过滤器中」——固定 key 时本用例断言的是前者，前置状态由上一
	// 次运行残留 ⇒ 第二次跑 Add 必返 false，断言必红（复跑即假红）。
	// 唯一 key 让本用例不依赖任何前置状态。
	key := fmt.Sprintf("bloom:test:%d", time.Now().UnixNano())
	// 用完即删——不在共享 Redis 上留残骸（与同文件 counter 用例的清理口径一致）。
	defer func() { _ = client.Del(ctx, key).Err() }()

	bf, err := client.NewBloomFilter(ctx, key, 0.01, 1000)
	if err != nil {
		t.Skip("RedisBloom not available, skipping test")
	}

	// 添加元素
	added, err := bf.Add(ctx, "item1")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if !added {
		t.Error("expected item1 to be added")
	}

	// 再次添加（应该返回 false）
	added, err = bf.Add(ctx, "item1")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if added {
		t.Error("expected item1 to not be added again")
	}

	// 检查存在
	exists, err := bf.Exists(ctx, "item1")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected item1 to exist")
	}

	// 检查不存在
	exists, err = bf.Exists(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected nonexistent to not exist")
	}
}

// TestBloomFilter_ReuseExistingKey 锁定「key 已存在时复用既有过滤器」语义
// （redisx#41 附带建议）：BF.RESERVE 对已存在的 key 不覆盖，故第二次
// NewBloomFilter 传入不同的 errorRate/capacity 不报错、也不重建——调用方
// 拿到的是与既有实例参数不符的过滤器。该语义已写入 bloom.go 的 godoc，
// 此处断言它不会在后续改动中悄悄变成别的行为。
func TestBloomFilter_ReuseExistingKey(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	key := fmt.Sprintf("bloom:reuse:%d", time.Now().UnixNano())
	defer func() { _ = client.Del(ctx, key).Err() }()

	bf, err := client.NewBloomFilter(ctx, key, 0.01, 1000)
	if err != nil {
		t.Skip("RedisBloom not available, skipping test")
	}

	// 同 key 再建一次：参数不同（errorRate/capacity）也**不报错**——
	// 既有过滤器被复用，本次参数不生效。
	bf2, err := client.NewBloomFilter(ctx, key, 0.001, 10)
	if err != nil {
		t.Fatalf("reuse existing key should not error, got: %v", err)
	}
	if bf2 == nil {
		t.Fatal("expected non-nil bloom filter on reuse")
	}

	// 复用语义下：第一个过滤器加的元素，第二个过滤器同样可见（同一底层 key）。
	added, err := bf.Add(ctx, "shared")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if !added {
		t.Error("expected shared to be added")
	}
	exists, err := bf2.Exists(ctx, "shared")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected reused filter to see element added via first handle")
	}
}
