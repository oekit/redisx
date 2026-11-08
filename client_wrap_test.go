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

// TestNewClient_DoesNotMutateCallerOptions 回归（#116）：NewClient 曾原地
// 改写调用方传入的 *redis.UniversalOptions——复用同一 opt 构建第二个实例
// （同配置双实例/测试矩阵常见）时，第二个实例拿到的是被第一次调用改写过的
// 配置，「调用方显式设置的字段不覆盖」的判定基线已被污染（比如调用方故意传
// DialTimeout: 0 想用 go-redis 自身缺省，第一次调用后被固化为 3s）。
// 修复后：默认值只填在内部副本上，调用方结构体保持原样，同一 opt 可复用。
func TestNewClient_DoesNotMutateCallerOptions(t *testing.T) {
	opt := &redis.UniversalOptions{
		Addrs:       []string{"127.0.0.1:1"}, // 不存在的服务：只测构造，不拨号
		DialTimeout: 0,                       // 显式零值：想用 go-redis 自身缺省
	}

	c1, err := NewClient(opt)
	if err != nil {
		t.Fatalf("first NewClient: %v", err)
	}
	defer func() { _ = c1.Close() }()

	// 第一次调用后，调用方的 opt 必须原样：五个会填默认值的字段不得被写。
	if opt.DialTimeout != 0 || opt.ReadTimeout != 0 || opt.WriteTimeout != 0 ||
		opt.PoolTimeout != 0 || opt.MinIdleConns != 0 {
		t.Fatalf("NewClient 不得改写调用方 opt（#116），实际: %+v", opt)
	}

	// 复用同一 opt 构建第二实例：配置基线不被第一次调用污染。
	c2, err := NewClient(opt)
	if err != nil {
		t.Fatalf("second NewClient from reused opt: %v", err)
	}
	defer func() { _ = c2.Close() }()

	if opt.DialTimeout != 0 || opt.ReadTimeout != 0 || opt.WriteTimeout != 0 ||
		opt.PoolTimeout != 0 || opt.MinIdleConns != 0 {
		t.Fatalf("复用 opt 的第二次 NewClient 也不得改写调用方 opt（#116），实际: %+v", opt)
	}
}

// TestNewClient_ExplicitFieldsPreserved 显式设置的字段不被缺省值覆盖
// （既有行为，与 #116 的 copy 改法共同钉住）。
func TestNewClient_ExplicitFieldsPreserved(t *testing.T) {
	opt := &redis.UniversalOptions{
		Addrs:        []string{"127.0.0.1:1"},
		DialTimeout:  7 * time.Second,
		ReadTimeout:  9 * time.Second,
		WriteTimeout: 11 * time.Second,
		PoolTimeout:  13 * time.Second,
		MinIdleConns: 3,
	}
	c, err := NewClient(opt)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	if opt.DialTimeout != 7*time.Second || opt.ReadTimeout != 9*time.Second ||
		opt.WriteTimeout != 11*time.Second || opt.PoolTimeout != 13*time.Second ||
		opt.MinIdleConns != 3 {
		t.Fatalf("显式字段不得被覆盖，也不得被改写，实际: %+v", opt)
	}
}
