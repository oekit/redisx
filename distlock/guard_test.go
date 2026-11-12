package distlock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newGuardTest 构造 miniredis + BruteForceGuard（阈值 3 / 窗口 10m / 锁定 30m）。
func newGuardTest(t *testing.T, memFallback bool) (*BruteForceGuard, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	g := NewBruteForceGuard(c, GuardConfig{
		KeyPrefix:   "zeus:lockout:",
		MaxFails:    3,
		Window:      10 * time.Minute,
		LockTTL:     30 * time.Minute,
		MemFallback: memFallback,
	})
	return g, mr
}

// TestBruteForceGuard_RedisPath_TripsAndResets 分布式主路径：阈值前放行、
// 第 N 次失败锁定（Check 给余量时长）、Reset 立即解封、LockTTL 到期自动解除
// ——组合口在 Redis 正常时就是 distlock.Lockout 的完整语义。
func TestBruteForceGuard_RedisPath_TripsAndResets(t *testing.T) {
	g, mr := newGuardTest(t, false)
	ctx := context.Background()

	for i := 1; i <= 2; i++ {
		locked, err := g.Fail(ctx, "user:42")
		if err != nil || locked {
			t.Fatalf("fail %d = (%v, %v), want (false, nil)", i, locked, err)
		}
	}
	locked, remain, err := g.Check(ctx, "user:42")
	if err != nil || locked || remain != 0 {
		t.Fatalf("below threshold Check = (%v, %v, %v), want unlocked", locked, remain, err)
	}

	locked, err = g.Fail(ctx, "user:42")
	if err != nil || !locked {
		t.Fatalf("trip fail = (%v, %v), want (true, nil)", locked, err)
	}
	locked, remain, err = g.Check(ctx, "user:42")
	if err != nil || !locked {
		t.Fatalf("Check after trip = (%v, %v, %v), want locked", locked, remain, err)
	}
	if remain <= 29*time.Minute || remain > 30*time.Minute {
		t.Fatalf("remain = %v, want ~30m", remain)
	}

	// Reset 解封
	if err := g.Reset(ctx, "user:42"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if locked, _, err := g.Check(ctx, "user:42"); err != nil || locked {
		t.Fatalf("Check after reset = (%v, %v), want unlocked", locked, err)
	}

	// 重新触发后 LockTTL 到期自动解除（miniredis 时钟推进锁定标记的 PTTL）
	for i := 0; i < 3; i++ {
		if _, err := g.Fail(ctx, "user:42"); err != nil {
			t.Fatalf("re-trip fail %d: %v", i, err)
		}
	}
	mr.FastForward(31 * time.Minute)
	if locked, _, err := g.Check(ctx, "user:42"); err != nil || locked {
		t.Fatalf("Check after lock ttl expiry = (%v, %v), want unlocked", locked, err)
	}
}

// TestBruteForceGuard_MemFallbackOnRedisFailure 验收判据①（降级路由）：Redis
// 故障时 Fail/Check 自动路由到进程内实现，阈值/窗口/锁定时长同源生效——
// 第 3 次失败锁定、余量可查，全程无 error 上抛（调用方无需特殊处理）。
func TestBruteForceGuard_MemFallbackOnRedisFailure(t *testing.T) {
	g, mr := newGuardTest(t, true)
	ctx := context.Background()

	mr.Close() // Redis 故障：降级路径唯一生效期

	for i := 1; i <= 2; i++ {
		locked, err := g.Fail(ctx, "user:42")
		if err != nil {
			t.Fatalf("fail %d must not surface redis error under MemFallback: %v", i, err)
		}
		if locked {
			t.Fatalf("fail %d locked = true, want false (below threshold)", i)
		}
	}
	locked, err := g.Fail(ctx, "user:42")
	if err != nil {
		t.Fatalf("trip fail must not surface redis error under MemFallback: %v", err)
	}
	if !locked {
		t.Fatal("3rd failure must lock (mem path, same-origin threshold)")
	}
	locked, remain, err := g.Check(ctx, "user:42")
	if err != nil {
		t.Fatalf("check must not surface redis error under MemFallback: %v", err)
	}
	if !locked || remain <= 29*time.Minute || remain > 30*time.Minute {
		t.Fatalf("mem Check = (%v, %v), want locked with ~30m remain", locked, remain)
	}
}

// TestBruteForceGuard_MemMaxEntriesCapacityCap 钉住 redisx#157：进程内降级层
// 的容量护栏（MemMaxEntries）真实生效——表满后新 key 的失败样本被丢弃
// （fail-open，内存不随攻击者的枚举速度增长），已登记 key 的计数不受影响。
// 无护栏时攻击者枚举 key（如用户名）每次 RecordFail 都新增条目，内存无界。
func TestBruteForceGuard_MemMaxEntriesCapacityCap(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	g := NewBruteForceGuard(c, GuardConfig{
		KeyPrefix:     "zeus:lockout:",
		MaxFails:      3,
		Window:        10 * time.Minute,
		LockTTL:       30 * time.Minute,
		MemFallback:   true,
		MemMaxEntries: 2, // 护栏：表容量 2
	})
	ctx := context.Background()
	mr.Close() // 走进程内降级层

	// 登满两条目（u1/u2 各一次失败——条目已登记）
	for _, k := range []string{"user:1", "user:2"} {
		if _, err := g.Fail(ctx, k); err != nil {
			t.Fatalf("seed fail %s: %v", k, err)
		}
	}
	// 枚举新 key：表满丢样本 fail-open——既不报错也不锁（样本被丢弃）
	for i := 0; i < 5; i++ {
		locked, err := g.Fail(ctx, "user:attacker-enum")
		if err != nil {
			t.Fatalf("over-cap fail must fail-open without error (i=%d): %v", i, err)
		}
		if locked {
			t.Fatalf("over-cap samples must be dropped, never accumulate to lock (i=%d)", i)
		}
	}
	// 已登记 key 不受影响：u1 的后续失败照常累计并触发锁定
	for i := 2; i <= 3; i++ {
		locked, err := g.Fail(ctx, "user:1")
		if err != nil {
			t.Fatalf("registered key fail %d: %v", i, err)
		}
		if locked != (i == 3) {
			t.Fatalf("registered key fail %d locked=%v, want %v", i, locked, i == 3)
		}
	}
}

// TestBruteForceGuard_MemFallbackOffSurfacesError 负对照：MemFallback 关闭时
// Redis 故障原样上抛（防爆破语义降级放行会静默失效，调用方显式取舍）。
func TestBruteForceGuard_MemFallbackOffSurfacesError(t *testing.T) {
	g, mr := newGuardTest(t, false)
	ctx := context.Background()

	mr.Close()
	if _, err := g.Fail(ctx, "user:42"); err == nil {
		t.Error("Fail must surface redis error with MemFallback off")
	}
	if _, _, err := g.Check(ctx, "user:42"); err == nil {
		t.Error("Check must surface redis error with MemFallback off")
	}
	if err := g.Reset(ctx, "user:42"); err == nil {
		t.Error("Reset must surface redis error with MemFallback off")
	}
}

// TestBruteForceGuard_ResetDuringDegradation 验收判据②（降级边界）：降级窗口
// 内 Reset 保证进程内视角清零（Check 解除），分布式侧不可达——Redis 恢复后
// 分布式侧旧锁定仍在（文档声明的边界，显式钉住）。
func TestBruteForceGuard_ResetDuringDegradation(t *testing.T) {
	g, mr := newGuardTest(t, true)
	ctx := context.Background()

	// Redis 正常：先在分布式侧触发锁定
	for i := 0; i < 3; i++ {
		if _, err := g.Fail(ctx, "user:42"); err != nil {
			t.Fatalf("fail %d: %v", i, err)
		}
	}
	if locked, _, _ := g.Check(ctx, "user:42"); !locked {
		t.Fatal("expected distributed lock tripped")
	}

	mr.Close()
	if err := g.Reset(ctx, "user:42"); err != nil {
		t.Fatalf("degraded reset must succeed (mem side cleared): %v", err)
	}
	if locked, _, err := g.Check(ctx, "user:42"); err != nil || locked {
		t.Fatalf("degraded Check after reset = (%v, %v), want mem-side unlocked", locked, err)
	}

	// Redis 恢复：分布式侧旧锁定仍在（降级 Reset 触达不到它）。
	if err := mr.Restart(); err != nil {
		t.Fatalf("restart miniredis: %v", err)
	}
	locked, remain, err := g.Check(ctx, "user:42")
	if err != nil {
		t.Fatalf("check after recovery: %v", err)
	}
	if !locked || remain <= 29*time.Minute {
		t.Fatalf("distributed lock must survive degraded reset (#128 boundary), got (%v, %v)", locked, remain)
	}
}

// TestBruteForceGuard_CtxCancelNotDegraded 调用方 ctx 取消不是 Redis 故障：
// 不路由到进程内实现，原样上抛（与 ratelimit rescue 的口径一致）。
func TestBruteForceGuard_CtxCancelNotDegraded(t *testing.T) {
	g, _ := newGuardTest(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := g.Fail(ctx, "user:42"); !errors.Is(err, context.Canceled) {
		t.Errorf("Fail on canceled ctx = %v, want ctx error (not degraded)", err)
	}
	if _, _, err := g.Check(ctx, "user:42"); !errors.Is(err, context.Canceled) {
		t.Errorf("Check on canceled ctx = %v, want ctx error (not degraded)", err)
	}
	if err := g.Reset(ctx, "user:42"); !errors.Is(err, context.Canceled) {
		t.Errorf("Reset on canceled ctx = %v, want ctx error (not degraded)", err)
	}
}

// TestBruteForceGuard_FailLockedAfterWindowSlide 回归（redisx#171）：LockTTL
// > Window 时，窗口滑动后活跃锁定内的 Fail 不得误报「未锁定」。旧实现按
// 「本次窗口计数 >= 阈值」复刻锁定判定——窗口滑走后旧失败出窗，第 N+1 次
// Fail 返回 count=1 → 1>=N 为 false，而 Check（锁定标记 PTTL 未到期）仍报
// locked=true；按包文档 `if locked, _ := guard.Fail(ctx, ...); locked` 门控
// 的调用方会放过处于拒绝态的请求。修复后 Fail 的锁定态以脚本侧锁定标记
// （SET PX penalty 的存在性）为唯一口径，与 Check 同源。
func TestBruteForceGuard_FailLockedAfterWindowSlide(t *testing.T) {
	// 短窗口（300ms）+ 长锁定（5s）构造 LockTTL > Window 的脱节形态，
	// 真实时钟即可跨过窗口，不依赖时钟注入。
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	g := NewBruteForceGuard(c, GuardConfig{
		KeyPrefix: "zeus:lockout:",
		MaxFails:  3,
		Window:    300 * time.Millisecond,
		LockTTL:   5 * time.Second,
	})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		locked, err := g.Fail(ctx, "user:42")
		if err != nil {
			t.Fatalf("fail %d: %v", i, err)
		}
		if locked != (i == 2) {
			t.Fatalf("fail %d locked = %v, want %v", i, locked, i == 2)
		}
	}
	if locked, _, err := g.Check(ctx, "user:42"); err != nil || !locked {
		t.Fatalf("precondition: Check after trip = (%v, %v), want locked", locked, err)
	}

	// 窗口滑走（旧失败全部出窗）而锁定标记仍在（PTTL 未到期）。
	time.Sleep(350 * time.Millisecond)

	locked, err := g.Fail(ctx, "user:42")
	if err != nil {
		t.Fatalf("fail after slide: %v", err)
	}
	if !locked {
		t.Fatal("Fail inside active lock after window slide must report locked (redisx#171)")
	}
	if locked, _, err := g.Check(ctx, "user:42"); err != nil || !locked {
		t.Fatalf("Check after slide = (%v, %v), want still locked", locked, err)
	}
}

// TestBruteForceGuard_ValidationFailFast 配置/参数非法在使用时报错不 panic
// （与 distlock 其余入口的 fail-fast 口径一致；nil client 即便开了
// MemFallback 也属配置错误，不得静默退化成纯进程内防爆破）。
func TestBruteForceGuard_ValidationFailFast(t *testing.T) {
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	cases := map[string]*BruteForceGuard{
		"nil client":  NewBruteForceGuard(nil, GuardConfig{MaxFails: 3, Window: time.Minute, LockTTL: time.Minute, MemFallback: true}),
		"max fails 0": NewBruteForceGuard(c, GuardConfig{MaxFails: 0, Window: time.Minute, LockTTL: time.Minute}),
		"window 0":    NewBruteForceGuard(c, GuardConfig{MaxFails: 3, Window: 0, LockTTL: time.Minute}),
		"lock ttl 0":  NewBruteForceGuard(c, GuardConfig{MaxFails: 3, Window: time.Minute, LockTTL: 0}),
	}
	for name, g := range cases {
		if _, err := g.Fail(ctx, "k"); err == nil {
			t.Errorf("%s: Fail must fail fast", name)
		}
		if _, _, err := g.Check(ctx, "k"); err == nil {
			t.Errorf("%s: Check must fail fast", name)
		}
		if err := g.Reset(ctx, "k"); err == nil {
			t.Errorf("%s: Reset must fail fast", name)
		}
	}

	g := NewBruteForceGuard(c, GuardConfig{MaxFails: 3, Window: time.Minute, LockTTL: time.Minute})
	if _, err := g.Fail(ctx, ""); err == nil {
		t.Error("empty key must fail fast")
	}
}
