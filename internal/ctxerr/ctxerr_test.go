package ctxerr

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestIs_ctxErr 单源判定的分支钉（redisx#162）：取消/超时（含包装形态）
// 命中，其它错误不命中——ratelimit 与 distlock 的降级路由共用本判定。
func TestIs_ctxErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"canceled", context.Canceled, true},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"wrapped canceled", fmt.Errorf("redis eval: %w", context.Canceled), true},
		{"wrapped deadline", fmt.Errorf("dial: %w", context.DeadlineExceeded), true},
		{"redis error", errors.New("redis: connection refused"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Is(tc.err); got != tc.want {
				t.Fatalf("Is(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
