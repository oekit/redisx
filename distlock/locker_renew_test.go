package distlock

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// --- #5：TryLock 自动续期（与 Lock 对称） ---

// TestTryLock_AutoRenew_RegistersState TryLock 成功后必须注册续期状态，
// Unlock 能停止续期——与 Lock 路径对称。
func TestTryLock_AutoRenew_RegistersState(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	l := NewRedisLocker(c, WithAutoRenew(), WithRenewInterval(20*time.Millisecond))
	ctx := context.Background()

	ownerID, err := l.TryLock(ctx, "try-renew-key", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	val, ok := l.locks.Load("try-renew-key")
	if !ok {
		t.Fatal("expected lockState registered after TryLock (symmetric with Lock)")
	}
	st := val.(*lockState)

	if err := l.Unlock(ctx, "try-renew-key", ownerID); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	select {
	case <-st.stopChan:
	case <-time.After(time.Second):
		t.Error("expected stopChan closed after Unlock")
	}
}

// TestTryLock_AutoRenew_KeepsLockAlive TryLock 后短 TTL 锁必须被续期保活：
// 观察窗口远超 TTL，若未续期 key 早已过期；Unlock 后停止续期即可过期。
func TestTryLock_AutoRenew_KeepsLockAlive(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	l := NewRedisLocker(c, WithAutoRenew(), WithRenewInterval(20*time.Millisecond))
	ctx := context.Background()

	ownerID, err := l.TryLock(ctx, "try-keep-key", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !mr.Exists("lock:try-keep-key") {
			t.Fatal("TryLock lock must be kept alive by auto-renew past its TTL")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Unlock 停止续期后，快进时钟锁应自然过期
	if err := l.Unlock(ctx, "try-keep-key", ownerID); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	mr.FastForward(3 * time.Second)
	if mr.Exists("lock:try-keep-key") {
		t.Error("lock must expire after Unlock stops auto-renew")
	}
}

// TestTryLock_NoAutoRenewByDefault 未启用 WithAutoRenew 时 TryLock 不得
// 注册续期状态（默认行为不变）。
func TestTryLock_NoAutoRenewByDefault(t *testing.T) {
	l, _ := newTestLocker(t)
	if _, err := l.TryLock(context.Background(), "plain-key", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, ok := l.locks.Load("plain-key"); ok {
		t.Error("TryLock must not register renew state without WithAutoRenew")
	}
}

// TestTryLock_AutoRenew_GracefulDegradation graceful 模式下 TryLock 同样
// 注册状态，Unlock 正常清理（与 Lock 路径状态机对称）。
func TestTryLock_AutoRenew_GracefulDegradation(t *testing.T) {
	l := NewRedisLocker(nil, WithAutoRenew(), WithGracefulDegradation())
	ctx := context.Background()

	if ownerID, err := l.TryLock(ctx, "g-key", time.Minute); err != nil || ownerID != "no-redis" {
		t.Fatalf("graceful TryLock: ownerID=%q err=%v", ownerID, err)
	}
	if _, ok := l.locks.Load("g-key"); !ok {
		t.Fatal("expected lockState registered in graceful mode")
	}
	if err := l.Unlock(ctx, "g-key", "no-redis"); err != nil {
		t.Errorf("graceful Unlock: %v", err)
	}
	if _, ok := l.locks.Load("g-key"); ok {
		t.Error("expected lockState removed after Unlock")
	}
}

// --- #5：手动 Renew ---

// TestRenew_Success 手动续期刷新 TTL：原 TTL 临近到期后续期，锁越过
// 原过期点仍存活；最终失去续期后自然过期。
func TestRenew_Success(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	l := NewRedisLocker(c)
	ctx := context.Background()

	ownerID, err := l.TryLock(ctx, "renew-key", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	mr.FastForward(150 * time.Millisecond) // 剩 50ms
	renewed, err := l.Renew(ctx, "renew-key", ownerID, time.Second)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if !renewed {
		t.Fatal("expected renew success for current owner")
	}
	mr.FastForward(500 * time.Millisecond) // 越过原 200ms 过期点
	if !mr.Exists("lock:renew-key") {
		t.Error("lock must survive past original TTL after manual renew")
	}
	mr.FastForward(3 * time.Second) // 越过续期 TTL
	if mr.Exists("lock:renew-key") {
		t.Error("lock must expire after renewed TTL passes")
	}
}

// TestRenew_LostOrWrongOwner 续期是 CAS：锁已过期/易主/ownerID 不匹配
// 返回 (false, nil)，不报错不复活。
func TestRenew_LostOrWrongOwner(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	l := NewRedisLocker(c)
	ctx := context.Background()

	ownerID, err := l.TryLock(ctx, "cas-key", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	mr.FastForward(500 * time.Millisecond) // 锁已过期

	renewed, err := l.Renew(ctx, "cas-key", ownerID, time.Minute)
	if err != nil || renewed {
		t.Errorf("expired lock must renew (false, nil), got (%v, %v)", renewed, err)
	}

	// 易主场景：新持有者持有，陈旧 ownerID 不得续期
	newOwner, err := l.TryLock(ctx, "cas-key", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	renewed, err = l.Renew(ctx, "cas-key", ownerID, time.Minute)
	if err != nil || renewed {
		t.Errorf("stale owner must not renew, got (%v, %v)", renewed, err)
	}
	renewed, err = l.Renew(ctx, "cas-key", newOwner, time.Minute)
	if err != nil || !renewed {
		t.Errorf("current owner must renew, got (%v, %v)", renewed, err)
	}
}

// TestRenew_Validation ttl 非法与 nil client（未开降级）必须 fail-fast；
// 开启降级时降级放行。
func TestRenew_Validation(t *testing.T) {
	l, _ := newTestLocker(t)
	if renewed, err := l.Renew(context.Background(), "k", "owner", 0); err == nil || renewed {
		t.Errorf("ttl=0 must be rejected, got (%v, %v)", renewed, err)
	}
	if renewed, err := NewRedisLocker(nil).Renew(context.Background(), "k", "owner", time.Minute); err == nil || renewed {
		t.Errorf("nil client without degradation must fail fast, got (%v, %v)", renewed, err)
	}
	if renewed, err := NewRedisLocker(nil, WithGracefulDegradation()).Renew(context.Background(), "k", "owner", time.Minute); err != nil || !renewed {
		t.Errorf("graceful degradation must renew-fake-succeed, got (%v, %v)", renewed, err)
	}
}
