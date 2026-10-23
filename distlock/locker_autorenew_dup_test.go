package distlock

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// --- #33：Lock() 重复启动自动续期 goroutine ---

// countAutoRenewGoroutines 统计当前存活的「自动续期 goroutine」数量。
//
// 续期 goroutine 由 startAutoRenew 内的 async.GoSafe 闭包派生，其运行时栈帧里
// 必然出现 "startAutoRenew"；据此按栈文本精确计数，比 runtime.NumGoroutine()
// 可靠（后者会把测试框架自身的 goroutine 一并算进来，无法定位到具体是谁）。
func countAutoRenewGoroutines() int {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), "startAutoRenew")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// waitAutoRenewGoroutines 等待续期 goroutine 数量变为 want；超时即测试失败。
func waitAutoRenewGoroutines(t *testing.T, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if countAutoRenewGoroutines() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("auto-renew goroutine count = %d, want %d (timeout %v)",
		countAutoRenewGoroutines(), want, timeout)
}

// TestLock_StartsOnlyOneAutoRenewGoroutine 回归测试（#33）：
//
// Lock() 内部调用 TryLock()，而 TryLock() 成功时已经启动过一次自动续期；
// Lock() 拿到 ownerID 后又启动一次 ⇒ 同一把锁起了两个续期 goroutine。
// 第二次 startAutoRenew 用新 state 覆盖旧 state，Unlock() 的 LoadAndDelete
// 只停得到后启动的那个 ⇒ 先启动的那个成孤儿：一直活到下个 tick 才发现锁已
// 不属于自己，并打出误导性的 "auto renew failed: lock lost or not owned"。
//
// 断言：观察窗口内续期 goroutine 数量恒为 1（出现 2 即为重复启动）；
// Unlock 后必须归零（无孤儿 goroutine 残留）。
func TestLock_StartsOnlyOneAutoRenewGoroutine(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })

	l := NewRedisLocker(c,
		WithAutoRenew(),
		// 续期间隔取得足够长：观察窗口内不会真的续期，也不会让「续期失败 →
		// goroutine 自行退出」把孤儿 goroutine 掩盖掉。
		WithRenewInterval(10*time.Second),
	)
	ctx := context.Background()

	// 前序用例可能刚取消掉续期 goroutine，等它退出后再取干净基线。
	waitAutoRenewGoroutines(t, 0, 3*time.Second)

	ownerID, err := l.Lock(ctx, "dup-renew-key", time.Second, time.Second)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// 观察窗口：续期 goroutine 数量必须恒为 1 —— 一旦出现 2 即为重复启动。
	observed := 0
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		n := countAutoRenewGoroutines()
		if n > 1 {
			t.Fatalf("Lock started %d auto-renew goroutines, want exactly 1 (duplicate start)", n)
		}
		if n == 1 {
			observed++
		}
		time.Sleep(20 * time.Millisecond)
	}
	if observed == 0 {
		t.Fatal("no auto-renew goroutine observed after Lock")
	}

	// Unlock 后不得残留续期 goroutine：孤儿 goroutine 的存在正是本 issue 的症状。
	if err := l.Unlock(ctx, "dup-renew-key", ownerID); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	waitAutoRenewGoroutines(t, 0, 3*time.Second)
}
