package distlock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- #159：watchdog 状态机单源后的共享行为钉 ---
//
// RedisLocker / Semaphore 两条端到端路径的 watchdog 行为（#33 重复启动、
// #39 maxLifetime 计时器、亚毫秒下限、陈旧持有者回填）由既有回归用例钉住
// （locker_autorenew_test.go / locker_autorenew_dup_test.go /
// semaphore_autorenew_test.go / locker_renew_subms_test.go）；本文件补齐
// 收敛后共享单元自身的分支语义测试——分支级等价的直接证据。

// waitForCond 条件等待：cond 为真即返回，超时 t.Fatalf。
func waitForCond(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestRenewIntervalFrom(t *testing.T) {
	cases := []struct {
		name       string
		configured time.Duration
		ttl        time.Duration
		want       time.Duration
	}{
		{"explicit config wins", 3 * time.Second, time.Minute, 3 * time.Second},
		{"derived ttl/2", 0, time.Minute, 30 * time.Second},
		{"sub-ms ttl clamps to 1ms", 0, 500 * time.Microsecond, time.Millisecond},
		{"tiny ttl clamps to 1ms", 0, time.Millisecond, time.Millisecond},
		{"negative configured falls to derivation", -time.Second, 10 * time.Second, 5 * time.Second},
		{"zero ttl clamps to 1ms", 0, 0, time.Millisecond},
		{"negative ttl clamps to 1ms", 0, -time.Second, time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := renewIntervalFrom(tc.configured, tc.ttl); got != tc.want {
				t.Fatalf("renewIntervalFrom(%v, %v) = %v, want %v", tc.configured, tc.ttl, got, tc.want)
			}
		})
	}
}

func TestRenewMaxLifetime(t *testing.T) {
	cases := []struct {
		name       string
		configured time.Duration
		ttl        time.Duration
		want       time.Duration
	}{
		{"unset defaults to 10x ttl", 0, time.Minute, 10 * time.Minute},
		{"negative means unlimited (no deadline)", -time.Second, time.Minute, 0},
		{"positive is explicit cap", 5 * time.Second, time.Minute, 5 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := renewMaxLifetime(tc.configured, tc.ttl); got != tc.want {
				t.Fatalf("renewMaxLifetime(%v, %v) = %v, want %v", tc.configured, tc.ttl, got, tc.want)
			}
		})
	}
}

// TestRenewWatchdog_RetryOnError 瞬时错误不放弃：renew 报错时经 onRetryErr
// 告警并在下一 tick 重试，goroutine 存活（第三个 tick 仍发起续期）。
func TestRenewWatchdog_RetryOnError(t *testing.T) {
	var calls atomic.Int64
	var retryErrs atomic.Int64
	release := make(chan struct{})
	w := &renewWatchdog{
		state:       &lockState{ownerID: "o", stopChan: make(chan struct{})},
		renewCtx:    context.Background(),
		interval:    5 * time.Millisecond,
		maxLifetime: 0,
		renew: func(context.Context) (bool, error) {
			if calls.Add(1) <= 2 {
				return false, errors.New("redis timeout")
			}
			close(release)
			return true, nil
		},
		onRetryErr: func(error) { retryErrs.Add(1) },
		onLost:     func() { t.Error("onLost must not fire on transient errors") },
		onGaveUp: func(time.Duration) {
			t.Error("onGaveUp must not fire without maxLifetime")
		},
	}
	w.startAutoRenew()
	defer stopState(w.state)

	select {
	case <-release:
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog gave up instead of retrying transient errors")
	}
	if got := retryErrs.Load(); got != 2 {
		t.Fatalf("onRetryErr fired %d times, want 2", got)
	}
}

// TestRenewWatchdog_LostExits 锁/槽丢失即退出：renew 返回 false 时告警一次并
// 退出 goroutine（不再发起后续续期）。
func TestRenewWatchdog_LostExits(t *testing.T) {
	var calls, lost atomic.Int64
	w := &renewWatchdog{
		state:       &lockState{ownerID: "o", stopChan: make(chan struct{})},
		renewCtx:    context.Background(),
		interval:    5 * time.Millisecond,
		maxLifetime: 0,
		renew: func(context.Context) (bool, error) {
			calls.Add(1)
			return false, nil
		},
		onRetryErr: func(error) { t.Error("onRetryErr must not fire on (false, nil)") },
		onLost:     func() { lost.Add(1) },
		onGaveUp:   func(time.Duration) { t.Error("onGaveUp must not fire without maxLifetime") },
	}
	w.startAutoRenew()

	waitForCond(t, 3*time.Second, "onLost must fire when renew reports (false, nil)", func() bool {
		return lost.Load() == 1
	})
	n := calls.Load()
	time.Sleep(60 * time.Millisecond)
	if got := calls.Load(); got != n {
		t.Fatalf("watchdog kept renewing after loss: %d -> %d calls", n, got)
	}
}

// TestRenewWatchdog_MaxLifetimeGivesUp 安全网：maxLifetime 到期后 onGaveUp
// 触发一次，续期停止。
func TestRenewWatchdog_MaxLifetimeGivesUp(t *testing.T) {
	var calls, gaveUps atomic.Int64
	w := &renewWatchdog{
		state:       &lockState{ownerID: "o", stopChan: make(chan struct{})},
		renewCtx:    context.Background(),
		interval:    5 * time.Millisecond,
		maxLifetime: 80 * time.Millisecond,
		renew: func(context.Context) (bool, error) {
			calls.Add(1)
			return true, nil
		},
		onRetryErr: func(error) { t.Error("onRetryErr must not fire") },
		onLost:     func() { t.Error("onLost must not fire while renewal succeeds") },
		onGaveUp:   func(time.Duration) { gaveUps.Add(1) },
	}
	w.startAutoRenew()

	waitForCond(t, 3*time.Second, "onGaveUp must fire at maxLifetime", func() bool {
		return gaveUps.Load() == 1
	})
	n := calls.Load()
	time.Sleep(100 * time.Millisecond)
	if got := calls.Load(); got != n {
		t.Fatalf("watchdog kept renewing after max lifetime: %d -> %d calls", n, got)
	}
}

// TestRenewWatchdog_StopState Stops stopChan/cancel 双通道停止：stopState 后
// goroutine 退出，不再续期。
func TestRenewWatchdog_StopState(t *testing.T) {
	var calls atomic.Int64
	w := &renewWatchdog{
		state:       &lockState{ownerID: "o", stopChan: make(chan struct{})},
		renewCtx:    context.Background(),
		interval:    5 * time.Millisecond,
		maxLifetime: 0,
		renew: func(context.Context) (bool, error) {
			calls.Add(1)
			return true, nil
		},
		onRetryErr: func(error) {},
		onLost:     func() {},
		onGaveUp:   func(time.Duration) {},
	}
	w.startAutoRenew()

	waitForCond(t, 3*time.Second, "watchdog must start renewing", func() bool {
		return calls.Load() >= 1
	})
	stopState(w.state)
	n := calls.Load()
	time.Sleep(60 * time.Millisecond)
	if got := calls.Load(); got != n {
		t.Fatalf("watchdog kept renewing after stop: %d -> %d calls", n, got)
	}
}

// TestNewRenewWatchdog_SwapStopsPreviousState 换入新 state 前先停掉同身份的
// 旧 state（#33 防御，单源后由 newRenewWatchdog 统一负责）：旧 stopChan 必须
// 被关闭。
func TestNewRenewWatchdog_SwapStopsPreviousState(t *testing.T) {
	var m sync.Map
	prev := &lockState{ownerID: "o", stopChan: make(chan struct{})}
	m.Store("ident", prev)

	w := newRenewWatchdog(&m, "ident", "ident")
	if w.state == nil || w.state.ownerID != "ident" {
		t.Fatalf("new state must be registered for ident, got %+v", w.state)
	}
	if w.state == prev {
		t.Fatal("new state must replace the previous one")
	}
	select {
	case <-prev.stopChan:
	default:
		t.Fatal("previous state must be stopped when swapped out (#33)")
	}
	if _, ok := m.Load("ident"); !ok {
		t.Fatal("new state must be present in the map")
	}
}
