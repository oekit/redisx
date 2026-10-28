package distlock

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestRenew_SubMillisecondTTL_KeepsLock 回归测试（#54）：亚毫秒 TTL 的续期
// **不得**把锁删掉。
//
// 缺陷成因：续期把 TTL 量化为 `ttl.Milliseconds()`，亚毫秒 ⇒ 0 ⇒
// Redis 收到 `PEXPIRE key 0` ⇒ 按 Redis 语义**立即删除该键**并回复 1
// ⇒ Go 侧返回 (true, nil) ⇒ **锁已经没了，调用方却被告知续期成功**。
//
// 后果：调用方在「自己并不持有锁」的状态下继续写临界区 ⇒ 另一实例可同时
// SetNX 成功 ⇒ 静默脑裂（跨实例重复执行同一业务动作）。
//
// 修法：续期量化的下限是 1ms（与写路径 go-redis formatMs 一致），
// 且 Lua 脚本自身也拒执行 ≤0 的 PEXPIRE（双保险）。
func TestRenew_SubMillisecondTTL_KeepsLock(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	l := NewRedisLocker(c)
	ctx := context.Background()

	const subTTL = 500 * time.Microsecond

	ownerID, err := l.TryLock(ctx, "subms-key", subTTL)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if !mr.Exists("lock:subms-key") {
		t.Fatal("TryLock 后锁不存在")
	}

	renewed, err := l.Renew(ctx, "subms-key", ownerID, subTTL)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if !renewed {
		t.Fatalf("Renew 应成功（TTL 量化到 1ms），got renewed=%v err=%v", renewed, err)
	}
	if !mr.Exists("lock:subms-key") {
		t.Fatal("Renew 报告成功，但锁已被删除（PEXPIRE 0 = 立即删键）——调用方会在不持锁的状态下写临界区")
	}
}

// TestRenewScript_RejectsNonPositiveTTL 回归测试（#54）：Lua 脚本自身**不得**
// 执行 ≤0 毫秒的 PEXPIRE（那是「删键」）。
//
// 这是双保险：即便调用方绕过 ttlMillis 直接传 0，脚本也只返回 0。
func TestRenewScript_RejectsNonPositiveTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	// 先造一个锁：value = owner
	if err := c.Set(ctx, "lock:k", "owner", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	res, err := renewScript.Run(ctx, c, []string{"lock:k"}, "owner", 0).Int()
	if err != nil {
		t.Fatalf("script: %v", err)
	}
	if res != 0 {
		t.Fatalf("脚本对 ≤0 毫秒应返回 0，实际 %d", res)
	}
	if !mr.Exists("lock:k") {
		t.Fatal("脚本执行了 PEXPIRE 0 ⇒ 锁被删除（这正是 #54 的缺陷形态）")
	}
}

// TestTTLMillis 回归护栏：TTL 量化下限是 1ms，且毫秒值不被截断错。
func TestTTLMillis(t *testing.T) {
	cases := []struct {
		ttl  time.Duration
		want int64
	}{
		{500 * time.Microsecond, 1}, // 亚毫秒 → 抬到 1ms（写路径同款）
		{time.Millisecond, 1},
		{1500 * time.Microsecond, 1},
		{2 * time.Millisecond, 2},
		{time.Second, 1000},
		{90 * time.Second, 90000},
	}
	for _, tc := range cases {
		if got := ttlMillis(tc.ttl); got != tc.want {
			t.Errorf("ttlMillis(%v) = %d，期望 %d", tc.ttl, got, tc.want)
		}
	}
}

// TestRenew_AutoRenew_SubMillisecondTTL 回归测试（#54）：配了自动续期且 TTL
// 为亚毫秒时，锁**不得**在第一次续期 tick 就消失。
func TestRenew_AutoRenew_SubMillisecondTTL(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	l := NewRedisLocker(c, WithAutoRenew())
	ctx := context.Background()

	const subTTL = 500 * time.Microsecond

	if _, err := l.TryLock(ctx, "subms-auto-key", subTTL); err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	// 观察窗口覆盖多次续期 tick（间隔下限 1ms）。
	time.Sleep(20 * time.Millisecond)

	if !mr.Exists("lock:subms-auto-key") {
		t.Fatal("自动续期把锁删掉了（PEXPIRE 0）——第一次 tick 就失锁")
	}
}
