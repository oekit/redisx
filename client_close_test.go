package redisx

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
)

// stubClient 是只实现 Ping/Close 的 UniversalClient 测试桩：其余方法内嵌 nil
// 接口，被误调即 panic，防止测试悄悄走到意料外的命令。
type stubClient struct {
	redis.UniversalClient
	pingErr  error
	closeErr error
	closes   int // Close 调用计数（幂等断言用）
}

func (s *stubClient) Ping(ctx context.Context) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx, "ping")
	if s.pingErr != nil {
		cmd.SetErr(s.pingErr)
		return cmd
	}
	cmd.SetVal("PONG")
	return cmd
}

func (s *stubClient) Close() error {
	s.closes++
	return s.closeErr
}

// TestClient_Close_Idempotent 回归（#148）：同一个 Client 连续 Close 两次
// （「显式 Close + defer Close」双路径的最小形态），第二次必须返回 nil。
//
// 修复前（Close() error 直接透传 go-redis）：第二次拿到 redis.ErrClosed
// （"redis: client is closed"）——正常的重复关闭被调用方当成关闭失败上报。
// 同时钉住带 ctx 的统一签名（本仓生命周期硬规则：Close(ctx) error）。
func TestClient_Close_Idempotent(t *testing.T) {
	c, _ := newTestClient(t, "close")
	ctx := context.Background()

	if err := c.Close(ctx); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("second Close must be idempotent nil (#148), got %v", err)
	}
}

// TestClient_Close_CachesFirstResult 回归（#148）：sync.Once 缓存**首次** Close
// 的结果——底层连接池只真正关一次，首次关闭的 error（含 nil）原样返回给每次
// 调用（不重新关、也不吞掉）。
func TestClient_Close_CachesFirstResult(t *testing.T) {
	errBoom := errors.New("close boom")
	s := &stubClient{closeErr: errBoom}
	c := NewClientFromClient(s)
	ctx := context.Background()

	for i := 1; i <= 2; i++ {
		if err := c.Close(ctx); !errors.Is(err, errBoom) {
			t.Fatalf("Close #%d: want errors.Is(err, errBoom), got %v", i, err)
		}
	}
	if s.closes != 1 {
		t.Fatalf("底层 Close 应只调一次（sync.Once），实际 %d 次", s.closes)
	}
}

// TestClient_Close_SuccessStaysNil 成功关闭后重复调用恒 nil（成功结果同样被
// 缓存，不会二次关闭退化成 redis.ErrClosed）。
func TestClient_Close_SuccessStaysNil(t *testing.T) {
	s := &stubClient{}
	c := NewClientFromClient(s)
	ctx := context.Background()

	if err := c.Close(ctx); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if s.closes != 1 {
		t.Fatalf("底层 Close 应只调一次，实际 %d 次", s.closes)
	}
}
