// Package distlock 提供分布式锁门面。
//
// 定义 Locker 接口，支持多种后端实现（Redis、etcd、ZooKeeper 等）。
// 当前提供 RedisLocker 实现。
//
// # 契约单源（etcdx#123，pkg v0.45.0 起）
//
// Locker 接口、三个哨兵错误（ErrLockNotAcquired / ErrLockTimeout /
// ErrLockNotHeld）与 WithLock 的实现已上提至 [github.com/oekit/pkg/distlock]
// 契约单源，本包以**别名/薄转发**消费：
//
//   - [Locker] = pkg/distlock.Locker（类型别名，同进程同一 interface 类型）；
//   - 三个哨兵是 pkg 侧同值变量的包级别名（文案不变，errors.Is 两侧任一
//     写法都命中——同进程只此一套哨兵值）；
//   - [WithLock] 薄转发 pkg 实现（签名/错误口径不变；释放失败的告警改由
//     pkg 记 log/slog，见 WithLock 注释）。
//
// 后端特有能力（自动续期、手动续期 Renew、最大持锁时长等）仍留在本包
// 具体类型/构造选项上，不进中立接口。跨后端互换（etcdx 等）从此共享同一
// 契约：同一份接口、同一套哨兵，不再靠注释互相提醒手工对账。
//
// # 核心特性
//
//   - Locker 接口（etcdx#123 契约单源的别名），面向接口编程
//   - SetNX 加锁 + Lua 原子释放（防止误删他人的锁）
//   - 支持单次尝试（TryLock）和带超时重试（Lock）
//   - 可选自动续期（WithAutoRenew，TryLock/Lock 对称生效，含最大生命周期
//     安全网）与手动续期（RedisLocker.Renew，Lua CAS 毫秒精度）；续期
//     watchdog 状态机在 Locker 与 Semaphore 单源同一份（#159）
//   - Semaphore 分布式信号量——ZSET 槽位配额（同一时刻 N 个持有者），
//     持有者崩溃后槽位 slotTTL 过期自愈；可选自动续期 watchdog
//     （WithSemaphoreAutoRenew，#126，与 Lock 的续期家族同构：间隔缺省
//     slotTTL/2、MaxAutoRenew 安全网、Release 即停）
//   - BruteForceGuard 防爆破锁对组合口（#128）：分布式 Lockout + 进程内
//     降级实现（concurrency/ratelimit.Lockout）+ Redis 故障时的双实现
//     路由，阈值/窗口/锁定时长库内同源，Fail/Check/Reset 对外统一
//   - 便捷包装 WithLock（加锁→执行→自动释放；实现上提 pkg 契约单源）
//   - 可选无 Redis 时降级放行（单实例部署场景）
//   - 可选 per-instance 日志注入（WithLogger / WithSemaphoreLogger，#120）：
//     watchdog 告警只进注入的 logger；缺省实例私有的 logx.Delegating("distlock")，
//     与历史行为一致
//   - 支持 redis.UniversalClient（兼容单机、集群、哨兵模式）；Lockout
//     三键共享 hash-tag，集群上 Lua 多键访问合法（#119）
//
// # 快速接入（推荐配合 ekit/data/redisx 使用）
//
//	redisClient, _ := redisx.NewClientFromAddr("localhost:6379", "", 0)
//	locker := distlock.NewRedisLocker(redisClient.Client(), distlock.WithPrefix("fuxi"), distlock.WithGracefulDegradation())
//
//	// 便捷包装（推荐）
//	err := distlock.WithLock(ctx, locker, "deploy:myapp:dev", 5*time.Minute, 10*time.Second, func() error {
//	    return doDeploy()
//	})
//
// # 完整服务集成示例
//
// 服务启动时初始化 Locker，建议作为全局变量或依赖注入：
//
//	var locker distlock.Locker
//
//	func init() {
//	    redisClient, err := redisx.NewClientFromAddr("localhost:6379", "", 0)
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    locker = distlock.NewRedisLocker(
//	        redisClient.Client(),
//	        distlock.WithPrefix("myservice"),
//	        distlock.WithGracefulDegradation(),
//	        distlock.WithOnLock(func(key string, acquired bool, err error) {
//	            if err != nil {
//	                log.Printf("[distlock] lock error: key=%s err=%v", key, err)
//	            }
//	        }),
//	    )
//	}
//
// # 使用场景
//
// 场景一：定时任务互斥（推荐 WithLock）
//
// 多实例部署时，确保只有一个实例执行定时任务：
//
//	err := distlock.WithLock(ctx, locker, "cron:daily-report", 10*time.Minute, 30*time.Second, func() error {
//	    return generateDailyReport()
//	})
//	if errors.Is(err, distlock.ErrLockTimeout) {
//	    log.Println("其他实例正在执行，跳过")
//	    return nil
//	}
//
// 场景二：资源操作保护（TryLock 单次尝试）
//
// 防止并发操作同一资源，失败立即返回：
//
//	ownerID, err := locker.TryLock(ctx, "order:"+orderID, 30*time.Second)
//	if errors.Is(err, distlock.ErrLockNotAcquired) {
//	    return errors.New("订单正在处理中，请稍后重试")
//	}
//	if err != nil {
//	    return err
//	}
//	defer locker.Unlock(ctx, "order:"+orderID, ownerID)
//	return processOrder(orderID)
//
// 场景三：部署流水线（Lock 带超时）
//
// 等待锁释放后执行部署：
//
//	ownerID, err := locker.Lock(ctx, "deploy:"+env, 5*time.Minute, 10*time.Minute)
//	if err != nil {
//	    return fmt.Errorf("获取部署锁失败: %w", err)
//	}
//	defer locker.Unlock(ctx, "deploy:"+env, ownerID)
//	return runDeploy(env)
//
// 场景四：任务互斥 + 长任务续期（TryLock 自动续期 / 手动 Renew）
//
// 非阻塞获取 + 持有期自动续期（WithAutoRenew 对 TryLock 与 Lock 对称生效）：
//
//	locker := distlock.NewRedisLocker(client,
//	    distlock.WithAutoRenew(),
//	    distlock.WithRenewInterval(10*time.Second),
//	    distlock.WithMaxAutoRenew(-1), // 数小时长任务：取消 10×ttl 上限
//	)
//	ownerID, err := locker.TryLock(ctx, "cron:sync", 30*time.Second)
//	if errors.Is(err, distlock.ErrLockNotAcquired) {
//	    return ErrTaskRunning // 被占快速失败，不排队
//	}
//	defer locker.Unlock(ctx, "cron:sync", ownerID)
//
// 或自管 watchdog 显式续期（不受 WithMaxAutoRenew 约束）：
//
//	ok, err := locker.(*distlock.RedisLocker).Renew(ctx, "cron:sync", ownerID, 30*time.Second)
//	if err == nil && !ok {
//	    return ErrLockLost // 锁已丢失/易主，必须停止工作
//	}
//
// 场景五：全局并发上限（Semaphore 分布式信号量）
//
// 多实例共享「同一时刻最多 N 个」的配额，崩溃持有者的槽位在 slotTTL 后
// 自动回收。长任务（克隆+推送之类可跑几十分钟）开启自动续期 watchdog
// （#126，与 Lock 的 WithAutoRenew 家族同构——不再需要下游手拼 watchdog）：
//
//	sem := distlock.NewSemaphore(redisClient, "quota:git-sync", 4, 30*time.Second,
//	    distlock.WithSemaphoreAutoRenew(),
//	    distlock.WithSemaphoreMaxAutoRenew(-1), // 数小时长任务：取消 10×slotTTL 上限
//	)
//	id := instanceID + ":" + taskID
//	ok, err := sem.Acquire(ctx, id) // false = 配额已满
//	if err != nil || !ok {
//	    return ErrTooBusy
//	}
//	defer sem.Release(ctx, id) // watchdog 随 Release 停止
//
// 或自管 watchdog 显式续期（不受 WithSemaphoreMaxAutoRenew 约束）：
//
//	if ok, err := sem.Renew(ctx, id); err == nil && !ok {
//	    return ErrSlotLost // 槽已过期回收/配额被接管，重新 Acquire
//	}
//
// 场景六：登录防爆破（BruteForceGuard 防爆破锁对，#128）
//
// 分布式 Lockout（跨实例共享计数）+ 进程内降级实现的一等组合口，阈值/
// 窗口/锁定时长库内同源，Redis 故障时（MemFallback 打开）自动路由进程内
// 实现——装配知识单源，下游不再整包手搓：
//
//	guard := distlock.NewBruteForceGuard(redisClient, distlock.GuardConfig{
//	    KeyPrefix:   "zeus:lockout:",
//	    MaxFails:    5,
//	    Window:      10 * time.Minute,
//	    LockTTL:     15 * time.Minute,
//	    MemFallback: true, // Redis 故障降级到进程内 Lockout（单实例有界）
//	})
//	if locked, _ := guard.Check(ctx, "user:42"); locked {
//	    return ErrTooManyAttempts // 429，余量经 guard.Check 查询
//	}
//	if err := verifyCredential(); err != nil {
//	    locked, _ := guard.Fail(ctx, "user:42")
//	    if locked {
//	        return ErrTooManyAttempts
//	    }
//	    return err
//	}
//	_ = guard.Reset(ctx, "user:42") // 登录成功解封（幂等）
//
// # 错误处理
//
// 包名下有三个哨兵错误（etcdx#123 契约单源后为 pkg/distlock 同值变量的
// 别名），用于判断锁获取失败的原因：
//
//   - [ErrLockNotAcquired]：TryLock 时锁被占用（非阻塞失败）
//   - [ErrLockTimeout]：Lock 超时未获取到锁
//   - [ErrLockNotHeld]：Unlock 时锁不存在或已易主（#115，本次未删任何锁，
//     调用方不得继续假装持有）
//
// 其他错误表示 Redis 通信问题，应记录日志并适当处理。
//
// # TTL 与超时建议
//
//   - TTL（锁持有时间）：应大于业务执行时间，防止锁提前过期
//   - Timeout（获取超时）：根据业务容忍度设置，建议为 TTL 的 2-3 倍
//   - 重试间隔：默认 100ms，可通过 WithRetryInterval 调整
//
// # 降级模式
//
// 启用 WithGracefulDegradation 后，当 Redis 不可用时（client 为 nil）：
//   - TryLock 返回 ("no-redis", nil)
//   - Lock 返回 ("no-redis", nil)
//   - Unlock 返回 nil
//
// 适用于开发环境或单实例部署场景，不建议在生产集群中使用。
package distlock
