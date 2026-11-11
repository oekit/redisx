package distlock

import (
	pkgdistlock "github.com/oekit/pkg/distlock"
)

// Locker 是分布式锁的后端中立接口。etcdx#123 契约单源后，接口定义上提至
// [pkgdistlock.Locker]（pkg v0.45.0），本包以**类型别名**消费同一份契约：
// 方法集（TryLock/Lock/Unlock）与哨兵语义与历史版本逐字相同，任何历史上
// 实现 redisx/distlock.Locker 的类型同时满足 pkg 侧契约，反之亦然——
// 同进程两个写法指向同一 interface 类型，不再有「两仓接口各写一份、靠
// 注释互相提醒手工对账」的形态。
//
// 后端特有能力（自动续期 WithAutoRenew、手动续期 Renew、最大持锁时长等）
// 仍留在本包具体类型/构造选项上，不进中立接口；需要可判定的可选能力时按
// 接口断言外置（与 pkg/distlock 的实现约定一致）。
type Locker = pkgdistlock.Locker
