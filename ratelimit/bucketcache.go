package ratelimit

import "sync"

// bucketCache 是 per-key 降级桶缓存（redisx#242）：把 PeriodLimit.bucketFor 与
// ResilientLimiter.localFor 两份逐行同构的「sync.Map + 工厂 → per-key
// singleflight 桶缓存」下沉为单源，两侧只差注入的 build 闭包——PeriodLimit
// 注入 bucketBuildFor（自定义工厂/配额推导），ResilientLimiter 注入 local
// （本地兜底工厂）。语义与下沉前完全一致：
//   - 工厂调用经 entry.once 收敛为 per-key singleflight（#166/#127）：并发
//     首次降级只有一个 goroutine 调用工厂，其余等待并拿到同一实例；
//   - 桶跨请求共享（#125：随用随建 = 故障期间每个请求都拿到全新满桶 =
//     限流失效）；
//   - 工厂返回 nil 不缓存：下次降级再调工厂（按 key 动态提供/收回语义）；
//   - clear 清空全部条目（回切/Close 清扫，#167，防高基数 key 长故障窗口
//     后常驻内存）。
type bucketCache struct {
	// build 返回 key 的兜底桶，仅在缓存 miss 时经 entry.once 单飞调用；由
	// 宿主构造期注入，判定路径只读。工厂可能在不同 key 上并发调用，须
	// 自行保证并发安全。
	build func(key string) LocalLimiter
	// m 是底层缓存：key → *bucketEntry，判定路径并发读写。
	m sync.Map
}

// get 返回 key 的兜底桶：Load 命中直接复用缓存实例；miss 时经 entry.once
// 单飞调 build 并 LoadOrStore，此后跨请求共享同一实例。工厂返回 nil 时该
// key 的「无桶」结论不缓存（仅当缓存仍指向本条目时删除——并发窗口里可能
// 已有更新条目，不可误删）。
func (c *bucketCache) get(key string) LocalLimiter {
	if v, ok := c.m.Load(key); ok {
		return v.(*bucketEntry).get()
	}
	// Load miss 与 LoadOrStore 之间无同步也安全：并发双方各持一个 entry，
	// 落选者的 entry 因 once 未触发而从未调用工厂，零副作用、即弃。
	e := &bucketEntry{build: func() LocalLimiter { return c.build(key) }}
	actual, _ := c.m.LoadOrStore(key, e)
	lim := actual.(*bucketEntry).get()
	if lim == nil {
		// 工厂返回 nil：该 key 的「无桶」结论不缓存。仅当缓存仍指向本条目
		// 时删除（并发窗口里可能已有更新条目，不可误删）。
		if cur, ok := c.m.Load(key); ok && cur == actual {
			c.m.Delete(key)
		}
	}
	return lim
}

// clear 清空全部缓存条目（回切/Close 清扫，#167）。
func (c *bucketCache) clear() {
	c.m.Clear()
}

// bucketEntry 是降级桶缓存条目（redisx#166）：once 把「工厂调用」从
// check-then-act 收敛为 per-key singleflight——并发首次降级时只有一个
// goroutine 调用工厂，其余等待并拿到同一实例（不再出现「Load miss 与
// LoadOrStore 之间无同步，落选者的桶实例被丢弃、自定义工厂被重复调用」
// 与「工厂只调一次（#127）」承诺不符的形态）。工厂返回 nil 时条目不驻留
// 缓存（保持「nil 不缓存、下次降级再调工厂」的按 key 动态提供/收回语义）。
type bucketEntry struct {
	once  sync.Once
	build func() LocalLimiter
	lim   LocalLimiter
}

// get 触发（或等待）本条目的单次工厂调用。
func (e *bucketEntry) get() LocalLimiter {
	e.once.Do(func() { e.lim = e.build() })
	return e.lim
}
