package redisx

import (
	"context"
	"errors"
	"testing"
)

// TestPingOrCleanup_SurfacesCloseError 回归（#149）：Ping 失败回收路径的 Close
// 错误必须可观测。
//
// 修复前（`_ = client.Client().Close()`）：连接池回收失败（半开连接、fd 回收
// 失败）被 `_ =` 整个吞掉——连接池已拨号而 Ping 失败，恰是唯一一次能观测到
// 连接回收失败的时机，吞掉后泄漏零信号。修复后 errors.Join 聚合上抛：
// Ping 错误与 Close 错误均可被 errors.Is/As 判定。
func TestPingOrCleanup_SurfacesCloseError(t *testing.T) {
	errPing := errors.New("ping boom")
	errClose := errors.New("close boom")
	c := NewClientFromClient(&stubClient{pingErr: errPing, closeErr: errClose})

	err := pingOrCleanup(context.Background(), c)
	if err == nil {
		t.Fatal("Ping 失败必须返回 error")
	}
	if !errors.Is(err, errPing) {
		t.Fatalf("Ping 错误必须上抛（%%w 包装可判定），got %v", err)
	}
	if !errors.Is(err, errClose) {
		t.Fatalf("Close 错误必须可观测（#149，修复前被 _ = 吞掉），got %v", err)
	}
}

// TestPingOrCleanup_PingErrorOnlyWhenCloseOK 回收成功时不引入噪声错误：返回
// 口径与历史一致（仅 Ping 错误、%w 包装、消息不变）。
func TestPingOrCleanup_PingErrorOnlyWhenCloseOK(t *testing.T) {
	errPing := errors.New("ping boom")
	s := &stubClient{pingErr: errPing}
	c := NewClientFromClient(s)

	err := pingOrCleanup(context.Background(), c)
	if !errors.Is(err, errPing) {
		t.Fatalf("want ping error, got %v", err)
	}
	if want := "redis ping failed: ping boom"; err.Error() != want {
		t.Fatalf("error message = %q, want %q", err.Error(), want)
	}
	if s.closes != 1 {
		t.Fatalf("Ping 失败应恰好回收一次，实际 %d 次", s.closes)
	}
}

// TestPingOrCleanup_OK Ping 成功路径零副作用：不关 client（后续正常使用）。
func TestPingOrCleanup_OK(t *testing.T) {
	s := &stubClient{}
	c := NewClientFromClient(s)

	if err := pingOrCleanup(context.Background(), c); err != nil {
		t.Fatalf("Ping 成功应返回 nil, got %v", err)
	}
	if s.closes != 0 {
		t.Fatalf("Ping 成功不得关闭 client，实际关闭 %d 次", s.closes)
	}
}
