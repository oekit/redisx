package redisx

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestNewClientFromClientKeepsUnderlyingClient 包装既有客户端时：
// ① 不新建连接（用的就是传进来的那个）；
// ② 不改写它的连接参数（NewClient 会改，这里不能）。
func TestNewClientFromClientKeepsUnderlyingClient(t *testing.T) {
	opt := &redis.UniversalOptions{
		Addrs:        []string{"127.0.0.1:1"},
		DialTimeout:  7 * time.Second, // 故意与 redisx 的默认值不同 ⇒ 可判别是否被改写
		ReadTimeout:  9 * time.Second,
		WriteTimeout: 11 * time.Second,
		PoolTimeout:  13 * time.Second,
	}
	inner := redis.NewUniversalClient(opt)
	defer func() { _ = inner.Close() }()

	c := NewClientFromClient(inner)
	if c.client != inner {
		t.Fatal("包装后应复用传入的客户端，不该新建")
	}
	// 判别点：NewClientFromClient 根本不碰传入的 opt（NewClient 会改写它）。
	if opt.DialTimeout != 7*time.Second || opt.ReadTimeout != 9*time.Second ||
		opt.WriteTimeout != 11*time.Second || opt.PoolTimeout != 13*time.Second {
		t.Fatalf("NewClientFromClient 不得改写连接参数，实际: %+v", opt)
	}
}

// TestNewClientFromClientPrefix 前缀照常生效（与 NewClient 一致）。
func TestNewClientFromClientPrefix(t *testing.T) {
	inner := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{"127.0.0.1:1"}})
	defer func() { _ = inner.Close() }()
	c := NewClientFromClient(inner, WithPrefix("pb"))
	if c.prefix != "pb" {
		t.Fatalf("前缀未生效: %q", c.prefix)
	}
	if got := c.convertKey("k"); got != "pb:k" {
		t.Fatalf("key 未加前缀: %q", got)
	}
	_ = context.Background()
}
