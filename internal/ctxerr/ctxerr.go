// Package ctxerr 提供调用方 context 取消/超时错误的统一判定（redisx#162）。
//
// ratelimit 与 distlock 两个包对「调用方主动放弃 ≠ Redis 故障、不触发降级」
// 的口径此前各持一份逐字相同的 isCtxErr，靠注释互相指认维持一致；收敛到
// internal 单源后，口径由编译期绑定——两个包的降级判定永远同调同一实现。
package ctxerr

import (
	"context"
	"errors"
)

// Is 报告 err 是否为调用方 ctx 的取消/超时（context.Canceled /
// context.DeadlineExceeded，含包装形态）。go-redis 会把 ctx 错误原样或包装后
// 返回，errors.Is 均可识别。
//
// 语义约定（两包共用）：调用方自己的主动放弃不是后端故障——ratelimit 不把
// limiter 打进降级态、distlock 不路由进进程内降级实现，错误原样上抛。
func Is(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
