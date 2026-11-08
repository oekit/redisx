package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oekit/logx"
	"github.com/redis/go-redis/v9"
)

// captureLogger 是实现完整 logx.Logger 的测试捕获器：记录 Warn/Error 事件
// （#120 验收：断言告警只进注入的 logger）。派生方法返回自身（测试足够）。
type captureLogger struct {
	mu     sync.Mutex
	events []captureEvent
	notify chan struct{} // 首次 Warn/Error 时关闭（异步告警的同步点）
}

type captureEvent struct {
	level string
	msg   string
	kv    []any
}

func newCaptureLogger() *captureLogger {
	return &captureLogger{notify: make(chan struct{})}
}

func (c *captureLogger) record(level, msg string, kv []any) {
	c.mu.Lock()
	if c.events == nil {
		close(c.notify)
	}
	c.events = append(c.events, captureEvent{level: level, msg: msg, kv: kv})
	c.mu.Unlock()
}

func (c *captureLogger) countByMsg(msg string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.events {
		if e.msg == msg {
			n++
		}
	}
	return n
}

func (c *captureLogger) Debug(msg string, kv ...any)                               {}
func (c *captureLogger) Info(msg string, kv ...any)                                {}
func (c *captureLogger) Warn(msg string, kv ...any)                                { c.record("warn", msg, kv) }
func (c *captureLogger) Error(msg string, kv ...any)                               { c.record("error", msg, kv) }
func (c *captureLogger) Debugf(format string, args ...any)                         {}
func (c *captureLogger) Infof(format string, args ...any)                          {}
func (c *captureLogger) Warnf(format string, args ...any)                          {}
func (c *captureLogger) Errorf(format string, args ...any)                         {}
func (c *captureLogger) CtxDebug(ctx context.Context, msg string, kv ...any)       {}
func (c *captureLogger) CtxInfo(ctx context.Context, msg string, kv ...any)        {}
func (c *captureLogger) CtxWarn(ctx context.Context, msg string, kv ...any)        {}
func (c *captureLogger) CtxError(ctx context.Context, msg string, kv ...any)       {}
func (c *captureLogger) CtxDebugf(ctx context.Context, format string, args ...any) {}
func (c *captureLogger) CtxInfof(ctx context.Context, format string, args ...any)  {}
func (c *captureLogger) CtxWarnf(ctx context.Context, format string, args ...any)  {}
func (c *captureLogger) CtxErrorf(ctx context.Context, format string, args ...any) {}
func (c *captureLogger) WithCtx(ctx context.Context) logx.Logger                   { return c }
func (c *captureLogger) WithService(name string) logx.Logger                       { return c }
func (c *captureLogger) WithAttrs(attrs ...logx.Attr) logx.Logger                  { return c }
func (c *captureLogger) WithGroup(name string) logx.Logger                         { return c }
func (c *captureLogger) SetLevel(level logx.Level)                                 {}

// 编译期契约：captureLogger 满足完整 logx.Logger。
var _ logx.Logger = (*captureLogger)(nil)

// swapDefault 暂时把进程级 logx.Default 指向捕获 logger，返回还原函数。
// 用于 #120 的双向验收：注入实例的告警不得落 Default（否则宿主 stderr 出现
// 第二套日志），未注入实例的告警必须仍落 Default（既有行为不回归）。
func swapDefault(t *testing.T) *captureLogger {
	t.Helper()
	prev := logx.Default()
	captured := newCaptureLogger()
	logx.SetDefault(captured)
	t.Cleanup(func() { logx.SetDefault(prev) })
	return captured
}

// newBrokenSerializerCache 构造「序列化必失败」的缓存：GetOrLoad 回填路径
// 必然触发内部告警 "serialize error"（同步、确定性）。
func newBrokenSerializerCache(t *testing.T, client redis.UniversalClient, lg logx.Logger) *RedisCache[testUser] {
	t.Helper()
	return NewRedisCache[testUser](client, RedisConfig[testUser]{
		Prefix: "t:",
		TTL:    time.Minute,
		Source: func(ctx context.Context, key string) (*testUser, error) {
			return &testUser{ID: 1, Name: "alice"}, nil
		},
		Serializer: func(*testUser) ([]byte, error) { return nil, errors.New("boom") },
		Logger:     lg,
	})
}

// TestRedisCache_InjectedLoggerOnly 验收判据①：注入自定义 logger 后，内部
// 告警只进该 logger；进程级 Default（负对照面）**零输出**——宿主不调
// logx.SetDefault 也不会出现第二套 stderr 日志。
func TestRedisCache_InjectedLoggerOnly(t *testing.T) {
	client, _ := newTestRedisClient(t)
	injected := newCaptureLogger()
	defaultCap := swapDefault(t) // 负对照面：Default 上不得出现本实例事件

	c := newBrokenSerializerCache(t, client, injected)
	_, _ = c.GetOrLoad(context.Background(), "k1")

	if got := injected.countByMsg("serialize error"); got != 1 {
		t.Fatalf("injected logger got %d 'serialize error' events, want 1", got)
	}
	if got := defaultCap.countByMsg("serialize error"); got != 0 {
		t.Fatalf("logx.Default got %d 'serialize error' events, want 0 (per-instance injection must bypass global)", got)
	}
}

// TestRedisCache_DefaultLoggerFallback 验收判据②（负对照）：不注入 logger
// 时行为不回归——告警仍落 logx.Default()（懒解析全局 Default 的既有口径）。
func TestRedisCache_DefaultLoggerFallback(t *testing.T) {
	client, _ := newTestRedisClient(t)
	defaultCap := swapDefault(t)

	c := newBrokenSerializerCache(t, client, nil)
	_, _ = c.GetOrLoad(context.Background(), "k1")

	if got := defaultCap.countByMsg("serialize error"); got != 1 {
		t.Fatalf("logx.Default got %d 'serialize error' events, want 1 (default behavior must not regress)", got)
	}
}

// TestRedisCache_TwoInstancesNoCrossTalk 验收判据③：两个实例各注入不同
// logger 同时运行，日志不串——证明注入是 per-instance 而非进程级。
func TestRedisCache_TwoInstancesNoCrossTalk(t *testing.T) {
	client, _ := newTestRedisClient(t)
	logA := newCaptureLogger()
	logB := newCaptureLogger()

	cacheA := newBrokenSerializerCache(t, client, logA)
	cacheB := newBrokenSerializerCache(t, client, logB)

	_, _ = cacheA.GetOrLoad(context.Background(), "k1") // 只触发 A 的告警
	_ = cacheB

	if got := logA.countByMsg("serialize error"); got != 1 {
		t.Fatalf("logger A got %d events, want 1", got)
	}
	if got := logB.countByMsg("serialize error"); got != 0 {
		t.Fatalf("logger B got %d events, want 0 (per-instance, not process-wide)", got)
	}
}
