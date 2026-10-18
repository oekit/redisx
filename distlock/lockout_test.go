package distlock

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newLockoutTest 构造 miniredis + 注入时钟的分布式 Lockout（阈值 3 / 窗口 10m / 惩罚 30m）。
func newLockoutTest(t *testing.T, clock *lockoutClock, mutate func(*LockoutConfig)) (*Lockout, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	cfg := LockoutConfig{
		KeyPrefix: "zeus:lockout:",
		Threshold: 3,
		Window:    10 * time.Minute,
		Penalty:   30 * time.Minute,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	l := NewLockout(redis.NewClient(&redis.Options{Addr: mr.Addr()}), cfg)
	l.now = clock.now
	t.Cleanup(func() { _ = l.client.Close() })
	return l, mr
}

// lockoutClock 可注入时钟（窗口判定走 Go 侧；EXPIRE/PTTL 走 miniredis 时钟）。
type lockoutClock struct {
	nowFn func() time.Time
}

func (c *lockoutClock) now() time.Time { return c.nowFn() }

func TestLockout_TripsAndExpires(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &lockoutClock{nowFn: func() time.Time { return now }}
	l, mr := newLockoutTest(t, clock, nil)
	ctx := context.Background()

	// 阈值前未锁定
	for i := 1; i <= 2; i++ {
		n, err := l.Fail(ctx, "user:42")
		if err != nil {
			t.Fatalf("fail %d: %v", i, err)
		}
		if n != int64(i) {
			t.Fatalf("fail %d count = %d, want %d", i, n, i)
		}
	}
	locked, remain, err := l.Check(ctx, "user:42")
	if err != nil || locked {
		t.Fatalf("below threshold Check = (%v, %v, %v), want unlocked", locked, remain, err)
	}

	// 第 3 次：触发锁定，剩余时长接近全额 Penalty
	n, err := l.Fail(ctx, "user:42")
	if err != nil || n != 3 {
		t.Fatalf("trip fail = (%d, %v), want count 3", n, err)
	}
	locked, remain, err = l.Check(ctx, "user:42")
	if err != nil || !locked {
		t.Fatalf("Check after trip = (%v, %v, %v), want locked", locked, remain, err)
	}
	if remain <= 29*time.Minute || remain > 30*time.Minute {
		t.Fatalf("remain = %v, want ~30m", remain)
	}

	// 锁定到期：miniredis 时钟越过 Penalty（窗口 ZSET 的 PEXPIRE=10m 也随之消亡）
	mr.FastForward(31 * time.Minute)
	now = now.Add(31 * time.Minute) // 注入时钟同步推进（窗口判定走 Go 侧毫秒）
	locked, remain, err = l.Check(ctx, "user:42")
	if err != nil || locked {
		t.Fatalf("Check after penalty expiry = (%v, %v, %v), want unlocked", locked, remain, err)
	}

	// 重复违规者：窗口已消亡，从零起算——两次失败不锁，第三次重新锁定
	for i := 1; i <= 2; i++ {
		if _, err = l.Fail(ctx, "user:42"); err != nil {
			t.Fatalf("fail %d: %v", i, err)
		}
	}
	if locked, _, _ = l.Check(ctx, "user:42"); locked {
		t.Fatal("fresh window must restart counting")
	}
	if _, err = l.Fail(ctx, "user:42"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if locked, _, _ = l.Check(ctx, "user:42"); !locked {
		t.Fatal("repeat offender must re-lock at threshold")
	}
}

func TestLockout_WindowSliding(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &lockoutClock{nowFn: func() time.Time { return now }}
	l, _ := newLockoutTest(t, clock, nil)
	ctx := context.Background()

	// t0 两次失败 → 推进 11m（出窗）→ 两次失败仍在阈值下（旧失败已出窗）
	if _, err := l.Fail(ctx, "k"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	now = now.Add(11 * time.Minute)
	if _, err := l.Fail(ctx, "k"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if _, err := l.Fail(ctx, "k"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if locked, _, _ := l.Check(ctx, "k"); locked {
		t.Fatal("2 in-window failures must not lock (aged-out window restarts)")
	}
	// 第 3 次（窗口内共 3）→ 锁定
	now = now.Add(time.Second)
	if _, err := l.Fail(ctx, "k"); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if locked, _, _ := l.Check(ctx, "k"); !locked {
		t.Fatal("3 in-window failures must lock")
	}
}

func TestLockout_PenaltyNotExtended(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &lockoutClock{nowFn: func() time.Time { return now }}
	l, mr := newLockoutTest(t, clock, nil)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, _ = l.Fail(ctx, "k")
	}
	_, remain1, _ := l.Check(ctx, "k")

	// 锁定 15m 后继续失败：NX 语义保证惩罚不延长（剩余约 15m，而非重置 30m）
	mr.FastForward(15 * time.Minute)
	if _, err := l.Fail(ctx, "k"); err != nil {
		t.Fatalf("fail while locked: %v", err)
	}
	_, remain2, _ := l.Check(ctx, "k")
	if remain2 >= remain1 {
		t.Fatalf("penalty must not extend: remain1=%v remain2=%v", remain1, remain2)
	}
	if remain2 <= 14*time.Minute || remain2 >= 16*time.Minute {
		t.Fatalf("remain2 = %v, want ~15m (original penalty intact)", remain2)
	}
}

func TestLockout_Reset(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &lockoutClock{nowFn: func() time.Time { return now }}
	l, _ := newLockoutTest(t, clock, nil)
	ctx := context.Background()

	l.Reset(ctx, "ghost") // 未登记 no-op

	for i := 0; i < 3; i++ {
		_, _ = l.Fail(ctx, "k")
	}
	if locked, _, _ := l.Check(ctx, "k"); !locked {
		t.Fatal("must be locked before reset")
	}

	// 管理员解封：Reset 后未锁定，计数从零起算
	if err := l.Reset(ctx, "k"); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if locked, _, _ := l.Check(ctx, "k"); locked {
		t.Fatal("Check after reset must be unlocked")
	}
	for i := 0; i < 2; i++ {
		_, _ = l.Fail(ctx, "k")
	}
	if locked, _, _ := l.Check(ctx, "k"); locked {
		t.Fatal("count must restart from 0 after reset")
	}
}

func TestLockout_KeyIsolation(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &lockoutClock{nowFn: func() time.Time { return now }}
	l, _ := newLockoutTest(t, clock, nil)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, _ = l.Fail(ctx, "user:1")
	}
	if locked, _, _ := l.Check(ctx, "user:2"); locked {
		t.Fatal("other key must be independent")
	}
	if locked, _, _ := l.Check(ctx, "user:1"); !locked {
		t.Fatal("locked key must stay locked")
	}
}

func TestLockout_Validation(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := LockoutConfig{Threshold: 3, Window: time.Minute, Penalty: time.Minute}
	bad := []LockoutConfig{
		{Threshold: 0, Window: time.Minute, Penalty: time.Minute},
		{Threshold: -1, Window: time.Minute, Penalty: time.Minute},
		{Threshold: 3, Window: 0, Penalty: time.Minute},
		{Threshold: 3, Window: time.Minute, Penalty: 0},
	}
	for i, c := range bad {
		l := NewLockout(redis.NewClient(&redis.Options{Addr: mr.Addr()}), c)
		if _, err := l.Fail(context.Background(), "k"); err == nil {
			t.Fatalf("bad config #%d should error on use", i)
		}
	}
	// nil client：无降级（防爆破语义降级放行 = 静默失效）
	l := NewLockout(nil, cfg)
	if _, err := l.Fail(context.Background(), "k"); err == nil {
		t.Fatal("nil client must error")
	}
	// 空 key
	l2 := NewLockout(redis.NewClient(&redis.Options{Addr: mr.Addr()}), cfg)
	if _, err := l2.Fail(context.Background(), ""); err == nil {
		t.Fatal("empty key must error")
	}
	// KeyPrefix 缺省
	l3 := NewLockout(redis.NewClient(&redis.Options{Addr: mr.Addr()}), cfg)
	if l3.prefix != defaultLockoutKeyPrefix {
		t.Fatalf("prefix = %q, want default %q", l3.prefix, defaultLockoutKeyPrefix)
	}
}
