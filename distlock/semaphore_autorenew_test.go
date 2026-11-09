package distlock

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/oekit/logx"
	"github.com/redis/go-redis/v9"
)

// newAutoRenewSemaphore 构造开启自动续期的信号量（真实时钟——watchdog 与
// Renew 共用 s.now=time.Now，端到端真实续期，不 sleep 凑绿：断言窗口均大于
// slotTTL 与续期周期，语义成立与否与调度抖动无关）。
func newAutoRenewSemaphore(t *testing.T, key string, max int, slotTTL, renewInterval, maxLifetime time.Duration, lg logx.Logger) (*Semaphore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	opts := []SemaphoreOption{
		WithSemaphoreAutoRenew(),
		WithSemaphoreRenewInterval(renewInterval),
		WithSemaphoreMaxAutoRenew(maxLifetime),
	}
	if lg != nil {
		opts = append(opts, WithSemaphoreLogger(lg))
	}
	return NewSemaphore(c, key, max, slotTTL, opts...), mr
}

// TestSemaphore_AutoRenew_KeepsSlot 验收判据①：长任务（远超 slotTTL）期间
// watchdog 持续续期保槽——观察窗口 700ms 远超 slotTTL=300ms，无续期时槽位
// 早已过期被回收；配额不被他人抢占（另一身份 Acquire 必须被拒）。
func TestSemaphore_AutoRenew_KeepsSlot(t *testing.T) {
	s, _ := newAutoRenewSemaphore(t, "sem:ar-keep", 1, 300*time.Millisecond, 50*time.Millisecond, 0, nil)
	ctx := context.Background()

	if ok, err := s.Acquire(ctx, "a"); err != nil || !ok {
		t.Fatalf("acquire a: ok=%v err=%v", ok, err)
	}

	// 700ms >> slotTTL(300ms)：watchdog 每 50ms 续期一次，槽位必须仍被 a 持有。
	time.Sleep(700 * time.Millisecond)
	if ok, err := s.Acquire(ctx, "b"); err != nil {
		t.Fatalf("acquire b: %v", err)
	} else if ok {
		t.Error("slot must stay held past the original expiry (auto renew keeps the slot)")
	}

	// Release 后立即让位（watchdog 已停，槽位真实释放）。
	if err := s.Release(ctx, "a"); err != nil {
		t.Fatalf("release a: %v", err)
	}
	if ok, err := s.Acquire(ctx, "b"); err != nil || !ok {
		t.Errorf("acquire b after release: ok=%v err=%v", ok, err)
	}
}

// TestSemaphore_NoAutoRenew_SlotExpires 负对照：不开自动续期时同样的时间
// 窗口内槽位过期被回收——判据①的绿必须来自续期本身，而非测试窗口巧合。
func TestSemaphore_NoAutoRenew_SlotExpires(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	s := NewSemaphore(c, "sem:ar-neg", 1, 300*time.Millisecond)
	ctx := context.Background()

	if ok, _ := s.Acquire(ctx, "a"); !ok {
		t.Fatal("acquire a")
	}
	time.Sleep(400 * time.Millisecond) // > slotTTL：a 不再续期，槽位过期
	if ok, err := s.Acquire(ctx, "b"); err != nil || !ok {
		t.Errorf("expired slot must be recycled without auto renew, ok=%v err=%v", ok, err)
	}
}

// TestSemaphore_AutoRenew_StopsOnRelease 验收判据②：Release 后 watchdog 必须停止
// ——观察窗口覆盖多个续期周期，命令数不增长（仍在续期 = EVALSHA 持续打 Redis）。
func TestSemaphore_AutoRenew_StopsOnRelease(t *testing.T) {
	s, mr := newAutoRenewSemaphore(t, "sem:ar-stop", 1, time.Minute, 30*time.Millisecond, 0, nil)
	ctx := context.Background()

	if ok, err := s.Acquire(ctx, "a"); err != nil || !ok {
		t.Fatalf("acquire a: ok=%v err=%v", ok, err)
	}
	// 等首个续期 tick 真的发生（watchdog 确实在转，对照才有意义）。
	time.Sleep(80 * time.Millisecond)

	if err := s.Release(ctx, "a"); err != nil {
		t.Fatalf("release a: %v", err)
	}
	before := mr.CommandCount()
	time.Sleep(150 * time.Millisecond) // ≥ 5 个续期周期
	after := mr.CommandCount()
	if after != before {
		t.Errorf("watchdog still renewing after Release: commands %d -> %d (#126: must stop)", before, after)
	}
}

// TestSemaphore_AutoRenew_SlotLostAlertAndExit 验收判据③：Renew 返回
// (false, nil)（槽位被外部回收，模拟配额被接管）时，watchdog 必须上报告警
// 并退出——「以为自己还占着配额」是静默脑裂形态。告警经注入 logger 捕获
// （notify channel 同步，不 sleep 凑绿）；退出经观察窗口命令数不增长钉住。
func TestSemaphore_AutoRenew_SlotLostAlertAndExit(t *testing.T) {
	s, mr := newAutoRenewSemaphore(t, "sem:ar-lost", 1, time.Minute, 30*time.Millisecond, 0, newCaptureLogger())
	ctx := context.Background()

	if ok, err := s.Acquire(ctx, "a"); err != nil || !ok {
		t.Fatalf("acquire a: ok=%v err=%v", ok, err)
	}
	lg := s.log.(*captureLogger)

	// 外部回收槽位（绕过 Release：模拟运维清理/配额被接管）。
	if err := s.client.Del(ctx, s.key).Err(); err != nil {
		t.Fatalf("del zset: %v", err)
	}
	waitWarn(t, lg)
	if n := lg.countByMsg("auto renew failed: semaphore slot lost or recycled"); n == 0 {
		t.Fatalf("slot-lost alert missing, events: %+v", lg.events)
	}

	// watchdog 必须已退出：观察窗口内零命令（继续打转 = 未退出）。
	before := mr.CommandCount()
	time.Sleep(150 * time.Millisecond)
	if after := mr.CommandCount(); after != before {
		t.Errorf("watchdog must exit after slot lost: commands %d -> %d", before, after)
	}
}

// TestSemaphore_AutoRenew_MaxLifetimeSafetyNet 安全网：忘记 Release 时，续期
// 到 maxAutoRenew 上限即放弃（告警可观测），槽位 slotTTL 后自然过期回收——
// 永久占槽 + goroutine 泄漏被 TTL 语义兜住。
func TestSemaphore_AutoRenew_MaxLifetimeSafetyNet(t *testing.T) {
	s, _ := newAutoRenewSemaphore(t, "sem:ar-max", 1, 200*time.Millisecond, 30*time.Millisecond, 250*time.Millisecond, newCaptureLogger())
	ctx := context.Background()

	if ok, err := s.Acquire(ctx, "a"); err != nil || !ok {
		t.Fatalf("acquire a: ok=%v err=%v", ok, err)
	}
	lg := s.log.(*captureLogger)
	waitWarn(t, lg)
	if n := lg.countByMsg("auto renew exceeded max lifetime, giving up slot"); n == 0 {
		t.Fatalf("max-lifetime alert missing, events: %+v", lg.events)
	}

	// maxLifetime(250ms) + slotTTL(200ms) 后槽位必须已回收：b 能进。
	time.Sleep(900 * time.Millisecond)
	if ok, err := s.Acquire(ctx, "b"); err != nil || !ok {
		t.Errorf("slot must be recyclable after max auto-renew lifetime, ok=%v err=%v", ok, err)
	}
}

// TestSemaphore_AutoRenew_UnlimitedKeepsSlot 负值 = 不限制（兼容旧行为）：
// 观察窗口越过默认安全网（10×slotTTL = 1s），槽位仍被续期保活。
func TestSemaphore_AutoRenew_UnlimitedKeepsSlot(t *testing.T) {
	s, _ := newAutoRenewSemaphore(t, "sem:ar-unlim", 1, 100*time.Millisecond, 20*time.Millisecond, -1, nil)
	ctx := context.Background()

	if ok, _ := s.Acquire(ctx, "a"); !ok {
		t.Fatal("acquire a")
	}
	time.Sleep(1300 * time.Millisecond) // > 10×slotTTL（默认安全网点）
	if ok, _ := s.Acquire(ctx, "b"); ok {
		t.Error("unlimited auto renew must keep the slot past the default safety-net point")
	}
}

// TestSemaphore_AutoRenew_ReacquireNoOrphan 同一 identifier 幂等重 Acquire：
// 旧 watchdog 被安全停掉（Swap），Release 一次后观察窗口零命令——不存在
// 孤儿续期 goroutine 在已释放槽位上打转（#33 同族，防误导性告警）。
func TestSemaphore_AutoRenew_ReacquireNoOrphan(t *testing.T) {
	s, mr := newAutoRenewSemaphore(t, "sem:ar-orphan", 2, time.Minute, 30*time.Millisecond, 0, nil)
	ctx := context.Background()

	if ok, _ := s.Acquire(ctx, "a"); !ok {
		t.Fatal("acquire a (1st)")
	}
	time.Sleep(80 * time.Millisecond)      // 首个 watchdog 至少续期一次
	if ok, _ := s.Acquire(ctx, "a"); !ok { // 幂等刷新 + watchdog 重启
		t.Fatal("re-acquire a must be idempotent")
	}
	if err := s.Release(ctx, "a"); err != nil {
		t.Fatalf("release a: %v", err)
	}
	before := mr.CommandCount()
	time.Sleep(150 * time.Millisecond)
	if after := mr.CommandCount(); after != before {
		t.Errorf("orphan watchdog still renewing after Release: commands %d -> %d", before, after)
	}
}

// TestSemaphore_AutoRenew_Options 选项注入语义：interval 非正值忽略（保留
// slotTTL/2 推导）、maxAutoRenew 三档取值原样保留、nil logger 保留缺省。
func TestSemaphore_AutoRenew_Options(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	s := NewSemaphore(c, "k", 1, time.Second,
		WithSemaphoreAutoRenew(),
		WithSemaphoreRenewInterval(-1), // 非正忽略
		WithSemaphoreMaxAutoRenew(30*time.Second),
		WithSemaphoreLogger(nil),
	)
	if !s.autoRenew {
		t.Error("WithSemaphoreAutoRenew must enable autoRenew")
	}
	if s.renewInterval != 0 {
		t.Errorf("non-positive renew interval must be ignored, got %v", s.renewInterval)
	}
	if s.maxAutoRenew != 30*time.Second {
		t.Errorf("maxAutoRenew = %v, want 30s", s.maxAutoRenew)
	}
	if s.log == nil {
		t.Error("nil logger must keep the default (non-nil) instance logger")
	}

	unlimited := NewSemaphore(c, "k", 1, time.Second, WithSemaphoreMaxAutoRenew(-1))
	if unlimited.maxAutoRenew >= 0 {
		t.Errorf("negative maxAutoRenew must be preserved (unlimited), got %v", unlimited.maxAutoRenew)
	}
}
