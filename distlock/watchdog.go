package distlock

import (
	"context"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
)

// lockState 管理单个锁/槽位的自动续期状态（per-key 隔离）。
// ownerID 记录本状态对应的持有者（RedisLocker 的锁 ownerID / Semaphore 的
// identifier），Unlock 时校验，防止超时后的陈旧持有者误杀新持有者的续期。
type lockState struct {
	ownerID  string
	stopChan chan struct{}
	cancel   context.CancelFunc
	once     sync.Once
}

// renewWatchdog 是自动续期 watchdog 状态机的**包内单源**（#159）：RedisLocker
// 与 Semaphore 的 startAutoRenew 此前整份复制约 90 行，#33（重复启动孤儿
// goroutine）与 #39（maxLifetime 计时器被 defer 提前 Stop）两个历史 bug 都
// 被迫在两份拷贝里各打一遍补丁。收敛后 interval 推导、maxLifetime 三态、
// lifetimeTimer 生命周期、Swap-停旧收尾、「瞬时错误重试 / 锁槽丢失即退出」
// 的分支语义全部只有一份；两个类型只注入各自的 renew 闭包与告警文案
// （onRetryErr / onLost / onGaveUp），逐分支行为与历史两份拷贝等价。
type renewWatchdog struct {
	// state 是本次续期的运行态（ownerID/stopChan/cancel），已由
	// [newRenewWatchdog] 原子换入所属类型的状态表。
	state *lockState
	// renewCtx 与锁/槽位绑定而非调用方 context：独立 derived context，
	// 调用方 ctx 取消不中断续期。
	renewCtx context.Context
	// interval 续期间隔（[renewIntervalFrom] 推导）；maxLifetime 续期最大
	// 生命周期（pkg/distlock.MaxLifetime 三态，0 = 无 deadline）。
	interval    time.Duration
	maxLifetime time.Duration
	// renew 单次续期：true = 成功；false = 锁/槽已丢失（watchdog 必须退出，
	// 继续执行会与新持有者并发/超出配额）；error = 通信故障（下一 tick 重试，
	// goroutine 退出即锁在 TTL 后静默失效，不能因抖动放弃）。
	renew func(ctx context.Context) (bool, error)
	// onRetryErr 瞬时错误告警；onLost 锁/槽丢失告警；onGaveUp 达最大生命
	// 周期放弃续期告警（安全网触发，参数为生效的 maxLifetime 值）。
	onRetryErr func(err error)
	onLost     func()
	onGaveUp   func(maxLifetime time.Duration)
	// cleanup 在 goroutine 因锁/槽丢失或达 maxLifetime 退出时调用，供所属
	// 类型从状态表 CompareAndDelete 本次 state（#257）。此前清理只发生在
	// Unlock/Release 路径：Redis 故障/逐出后 watchdog 自行退出，sync.Map
	// 条目永久残留——高基数锁键下形成无界泄漏。CompareAndDelete 以指针
	// 身份校验，窗口期新持有者换入的 state 不会被误删。可为 nil（单测
	// 直接构造 watchdog 时）。
	cleanup func()
}

// newRenewWatchdog 创建续期 watchdog 并把新状态原子换入 m。mapKey 是状态表的
// 键（RedisLocker 的锁 key / Semaphore 的 identifier），ownerID 是状态归属
// 身份（RedisLocker 的锁 value / Semaphore 的 identifier）——Locker 的状态表
// 按 key 索引、owner 记在 state 内，两者不可混用。覆盖旧 state 前先停掉它
// （防御性，#33）：同一键的旧 state 若仍在续期，它的 goroutine 只能等下一个
// tick 才发现锁/槽已不属于自己——既泄漏 goroutine，又打出误导性的丢失告警。
// 新获取成功（SetNX 通过/幂等刷新成功）意味着旧持有者的锁已不存在或槽位仍是
// 本人的，停掉旧续期是安全的。
func newRenewWatchdog(m *sync.Map, mapKey, ownerID string) *renewWatchdog {
	renewCtx, cancel := context.WithCancel(context.Background())
	st := &lockState{ownerID: ownerID, stopChan: make(chan struct{}), cancel: cancel}
	if old, ok := m.Swap(mapKey, st); ok {
		if prev, ok := old.(*lockState); ok {
			stopState(prev)
		}
	}
	return &renewWatchdog{
		state:    st,
		renewCtx: renewCtx,
		// 身份校验清理（#257）：只删「本 state 仍是 map 当前值」的条目。
		// 若窗口期内新持有者已换入新 state（Acquire 成功/幂等刷新），
		// CompareAndDelete 返回 false 原样保留，不会误杀新续期。
		cleanup: func() { m.CompareAndDelete(mapKey, st) },
	}
}

// startAutoRenew 启动续期 goroutine。方法名刻意保持 startAutoRenew：#33 回归
// 测试按运行时栈帧文本计数续期 goroutine（帧名含该标记），收敛实现不改观测面。
func (w *renewWatchdog) startAutoRenew() {
	state := w.state
	stopChan := state.stopChan
	renewCtx := w.renewCtx

	async.GoSafe(func() {
		ticker := time.NewTicker(w.interval)
		defer ticker.Stop()

		// 计时器必须在 goroutine 内部创建并停止（#39）：defer 是函数级的，
		// 放在创建 watchdog 的函数里会在其返回的瞬间就 Stop 掉计时器 ——
		// 派发完 goroutine 后该函数立刻返回 ⇒ lifetimeC 永不就绪 ⇒
		// `case <-lifetimeC` 是死代码 ⇒ maxLifetime 安全网从未生效。
		var lifetimeTimer *time.Timer
		var lifetimeC <-chan time.Time
		if w.maxLifetime > 0 {
			lifetimeTimer = time.NewTimer(w.maxLifetime)
			defer lifetimeTimer.Stop()
			lifetimeC = lifetimeTimer.C
		}

		for {
			select {
			case <-renewCtx.Done():
				return
			case <-stopChan:
				return
			case <-lifetimeC:
				// 达到最大续期生命周期：放弃续期，锁/槽在 TTL/slotTTL 后自然
				// 过期回收。这是忘记 Unlock/Release 时的安全网（默认 10×ttl）。
				w.cleanupState()
				w.onGaveUp(w.maxLifetime)
				return
			case <-ticker.C:
				ok, err := w.renew(renewCtx)
				if err != nil {
					// 瞬时错误（网络抖动、failover）不放弃：下一个 tick 重试。
					// 续期 goroutine 一旦退出，锁/槽将在 TTL 后静默失效。
					w.onRetryErr(err)
					continue
				}
				if !ok {
					// 锁/槽已丢失（易主/过期回收/被接管）：显式告警后退出。
					// 继续续期会与新持有者并发（静默脑裂形态）。
					// 退出前 CompareAndDelete 清理状态表（#257），否则该条目
					// 只能等 Unlock/Release 清理——而丢锁场景通常不会再有
					// 一次成功的 Unlock ⇒ 永久残留。
					w.cleanupState()
					w.onLost()
					return
				}
			}
		}
	})
}

// cleanupState 调用所属类型的身份校验清理（#257）；未注入时 no-op（单测
// 直接构造 renewWatchdog 的既有形态不需要状态表）。
func (w *renewWatchdog) cleanupState() {
	if w.cleanup != nil {
		w.cleanup()
	}
}

// stopState 停掉一个续期状态（幂等，once 保护）：取消 derived context 并
// 关闭 stopChan，goroutine 的两个退出通道同时就绪。
func stopState(st *lockState) {
	st.once.Do(func() {
		if st.cancel != nil {
			st.cancel()
		}
		close(st.stopChan)
	})
}

// renewIntervalFrom 推导自动续期间隔（#159 单源，Locker 与 Semaphore 同口径）：
// 显式配置（WithRenewInterval / WithSemaphoreRenewInterval）优先；未配置时
// ttl/2——续期失败的容忍窗口 = 剩余半个 TTL。亚毫秒 TTL 收紧到 1ms 下限，
// 避免高频 ticker 空转（续期毫秒量化的下限同为 1ms，见 ttlMillis/#54）；
// 下限只在**由 ttl 推导**时收紧，显式配置不动。推导结果仍非正时兜底 1s
// （time.NewTicker 对非正间隔会 panic）。
func renewIntervalFrom(configured, ttl time.Duration) time.Duration {
	interval := configured
	if interval <= 0 {
		interval = ttl / 2
		if interval < time.Millisecond {
			interval = time.Millisecond
		}
	}
	if interval <= 0 {
		interval = 1 * time.Second
	}
	return interval
}

// renewMaxLifetime 的三态逻辑（#159：0（未设置）= 10×ttl；负值 = 不限制
// （返回 0 表示无 deadline）；正值 = 显式上限）已随 #534 收敛进
// pkg/distlock.MaxLifetime——单源:pkg/distlock.MaxLifetime。
// 调用点：RedisLocker.startAutoRenew 与 Semaphore.startAutoRenew。
