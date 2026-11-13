## [v0.16.0] — 2026-11-13

- **BREAKING**：删除 Batch/NewBatch 旧类型（redisx#161 终局）——Batch 是 Pipeline 的功能子集旧类型（12 方法全为单行委托），前缀管道抽象收敛为 Pipeline/TxPipeline 单套。下游迁移：client.NewBatch() → client.Pipeline()（方法同构）。地板随迁 pkg v0.48.0 / logx v0.11.0 / concurrency v0.36.0。

## [v0.15.0] — 2026-11-12

- refactor(distlock)!: **消费 pkg v0.45.0 distlock 契约单源**（etcdx#123 治本 redisx 侧）——`Locker` 接口改为 `pkg/distlock.Locker` 的**类型别名**（`type Locker = distlockpkg.Locker`，方法集与历史逐字相同、同进程同一 interface 类型，redisx/etcdx 跨后端互换不再靠注释手工对账）；三个哨兵 `ErrLockNotAcquired`/`ErrLockTimeout`/`ErrLockNotHeld` 改为 pkg 侧同值变量的**包级别名**（`var ErrLockTimeout = pkgdistlock.ErrLockTimeout` 形态，文案不变，下游 `distlock.ErrX` 与 `pkgdistlock.ErrX` 任一写法 errors.Is 都命中——同进程只此一套哨兵值）；`WithLock` 删本地实现改**薄转发** `pkg/distlock.WithLock`（签名、错误包装 `lock <key>: %w`、fn 结果不被释放失败覆盖均不变，调用方零改动；释放纪律 `context.WithoutCancel` 单源上提）。⚠️ BREAKING：**无签名级破坏**；行为兼容性影响——WithLock 释放失败的告警改由 pkg 侧记 log/slog（component=distlock），不再路由 Locker 实现的 per-instance logx logger，`WithLogger` 注入面收窄为本包自身告警（watchdog 续期失败/锁丢失/超最大生命周期，落点不变）。redisx 历史上不存在 etcdx 侧的 sentinel.Is/IsOneOf 补丁机制（哨兵本就同文案），本版主要消除的是「接口/哨兵/WithLock 三处各持一份拷贝」的单源缺口
- refactor(distlock): #159 自动续期 watchdog 状态机单源——新增 `distlock/watchdog.go` 的 `renewWatchdog`，收编 RedisLocker 与 Semaphore 两处逐行同构的 startAutoRenew（约 90 行×2）及配套复制：interval 推导（ttl/2、亚毫秒 1ms 下限、1s 兜底）、maxLifetime 三态（0=10×ttl/负=不限/正=显式）、lifetimeTimer 在 goroutine 内创建（#39）、Swap 前停旧 state（#33）、「瞬时错误重试/锁槽丢失即退出」分支全部单源，两类型只注入各自的 renew 闭包与告警文案；goroutine 栈帧标记（startAutoRenew）与 `stopRenewState` 的 ownerID 比对回填口径逐分支保持等价——#33/#39/亚毫秒/陈旧持有者历史回归全数保留且绿，新增 `watchdog_test.go` 分支级单元钉（重试/丢失退出/安全网/停机/换入停旧）
- refactor(ratelimit): #162 `isCtxErr` 收敛 `internal/ctxerr` 单源——ratelimit 与 distlock 两包共调同一实现，「口径一致靠注释互相指认」改编译期绑定；tokenlimit.reserveN 的第三份内联 ctx 判定一并收编；#163 BloomFilter.MAdd/MExists 收敛 `bf.mDo`（除命令名外逐字相同的模板单源，两个导出方法各留一行）；#164 RedisCache 缺省 TTL（0→24h）收敛 `effectiveTTL` 单源——GetOrLoad 回填与 Set 两条写路径编译期绑定同一实现（与 SetWithTTL 的 ttl<=0 fail-fast 边界不变，#113），新增缺省 TTL 落键回归测试
- refactor: #161 `Batch` 标记 **Deprecated** 并改为内部委托 `*Pipeline`（保住 void 返回签名的兼容）——同一「前缀管道」抽象收敛到 Pipeline 一套实现（convertKey 口径天然同源），12 个方法行为不变、新增能力只进 Pipeline；文档指路 `Client.Pipeline`/`Client.TxPipeline`。未直接删除（导出 API，兼容期后随大版本移除）
- fix(ratelimit): #165 探活 watchdog 停机口——`PeriodLimit`/`TokenLimiter` 补 `Close(ctx) error`（sync.Once 幂等，对齐仓内生命周期硬规则），`ResilientLimiter.Close` 级联底层 PeriodLimit；此前探活循环只有「探活成功」一个出口，Redis 永久不可达且 limiter 被宿主丢弃时 goroutine 永不回收、每 100ms 对死服务器发起一次真实 EVAL 拨号。Close 后探活停转、startMonitor 为 no-op（不再自动回切），降级判定口径不变。回归：Close 后 monitorStarted 复位、探活调用计数冻结、重复 Close 幂等、停机后再次故障不重启监控（TokenLimiter/PeriodLimit 双路径）
- fix(ratelimit): #166 降级桶工厂 per-key singleflight——`PeriodLimit.bucketFor`/`ResilientLimiter.localFor` 的 Load-then-LoadOrStore 之间无同步，并发首次降级重复调用工厂、落选者桶实例被丢弃，与「工厂只调一次（#127）」承诺不符（自定义工厂带副作用/计数时语义漂移）；改为 `bucketEntry{once, build}` 条目，once 收敛工厂调用（落选者零副作用即弃），工厂返回 nil 的「不缓存、下次再调」动态语义保持。回归：32 并发首次降级工厂恰调 1 次（两实现）
- fix(ratelimit): #167 降级桶缓存无界增长——回切成功（探活钩子 onRecover / ResilientLimiter 首次 SourceRedis 判定，fallbackSeen 常态零开销）与 Close 时清空 `PeriodLimit.buckets`/`ResilientLimiter.localCache`：高基数 key（per-user/per-token）一次长故障窗口不再沉淀全量条目常驻内存；新故障周期重建桶（工厂按周期重调，语义诚实化）。回归：回切后缓存清零 + 新周期工厂再调；Close 幂等清理
- fix(ratelimit): #168 `NewPeriodLimit`/`NewTokenLimiter` 构造期校验 store 非 nil（直接 panic，与同函数既有 fail-fast 口径一致）——此前 nil store 不拦，故障延迟到首次判定的 nil interface panic 且堆栈落在 go-redis 内部帧，排查路径同「构造时忘传 client」。⚠️ BREAKING（构造期）：以 nil store 构造的调用方从「首次 Take panic」提前到「构造期 panic」（同样 fail-fast，时机前移），正常构造零影响
- build: Go 地板保持 `go 1.26.0`（ci.yml go-version '1.26' 成对无变更）；依赖地板对齐最新已发布版本：`github.com/oekit/pkg` v0.44.0 → **v0.45.0**（distlock 契约单源）、`logx` v0.9.2 → **v0.10.0**、`concurrency` v0.33.0 → **v0.34.0**（release_train 地板门禁）。现代语言特性：sync.Map.Clear（Go 1.24+）用于桶缓存清理、atomic.Bool 用于一次性清扫标记

关联问题单：redisx#159、redisx#161、redisx#162、redisx#163、redisx#164、redisx#165、redisx#166、redisx#167、redisx#168。

## [v0.14.1] — 2026-11-11

- fix(distlock): BruteForceGuard 的进程内降级层补容量护栏 MemMaxEntries（#157）——降级层以业务 key（如 username）为键，攻击者枚举 key 时每次 RecordFail 都新增条目、内存无界（下游手搓装配显式设 100k 护栏，#128 收编组合口时丢失）。0 = 不限（兼容既有调用方）；>0 时表满丢新样本 fail-open（已登记 key 不受影响，OverCapHits 观测），防爆破场景建议显式设置。

## [v0.14.0] — 2026-11-11

- feat(client)!: Client.Close 统一 `Close(ctx) error` + sync.Once 幂等（#148）——⚠️ BREAKING 签名变更。修复前 `Close() error` 两处违反本仓生命周期硬规则（AGENTS.md「组件 Close 统一 Close(ctx) error 签名 + sync.Once 幂等」）：① 无 ctx 参数，停机流程无法给关闭动作设期限；② 无幂等保护，直接透传 go-redis `baseClient.Close()`（ConnPool CAS 判重），「显式 Close + defer Close」双路径/多持有者各自 Close 时第二次拿到 `redis: client is closed`，正常重复关闭被调用方当成关闭失败上报（issue 探针实测）。修复后 sync.Once 缓存**首次**关闭结果：底层连接池只真正关一次、重复调用稳定返回同一结果（成功即恒 nil），与 etcdx.Client.Close(ctx) 同形——矩阵内「Close(ctx) 幂等」先例补齐 redisx 漏项。回归：TestClient_Close_Idempotent（连续 Close 两次第二次必须 nil——修复前透传 redis.ErrClosed 实测红）、TestClient_Close_CachesFirstResult（首次 error 缓存返回、底层只关一次）、TestClient_Close_SuccessStaysNil
- fix(config): NewClientFromConfig Ping 失败回收路径不再 `_ =` 吞掉 Close 错误（#149）。修复前 `_ = client.Client().Close()` 把连接池回收失败（半开连接、fd 回收失败）整个丢弃：连接池已拨号而 Ping 失败，恰是**唯一一次**能观测到连接回收失败的时机，吞掉后泄漏零信号；且同一 Close 两套口径（本函数 Ping 错误 `%w` 上抛、client.go Close 直返，唯独此处 `_` 丢弃）。修复后 errors.Join 聚合上抛（`redis ping failed: %w` + `redis close failed: %w`，errors.Is/As 双侧可判定），回收成功时返回口径与历史一致（仅 Ping 错误、消息不变）；Ping 失败回收改走 Client.Close(ctx) 幂等单源（#148），不绕过 sync.Once。回归：TestPingOrCleanup_SurfacesCloseError（Ping 失败 + Close 失败双信号上抛——修复前 `_ =` 吞掉 close 信号实测红）、TestPingOrCleanup_PingErrorOnlyWhenCloseOK、TestPingOrCleanup_OK
- ⚠️ breaking：`Client.Close()` → `Client.Close(ctx context.Context) error`——全仓调用点（doc.go 标准接入示例、测试）同步改签名；消费方（data 壳等 re-export）需随本版同步调用形态。现代语言特性：errors.Join 聚合双错误（各自 %w，Is/As 单源）；sync.Once + 结果缓存（sync.OnceFunc/OnceValue 不返回值，不贴合「重复调用返回同一 error」契约，故不取）；min/max 本轮无贴合点

关联问题单：redisx#148、redisx#149。

## [v0.13.2] — 2026-11-10

- - build: 依赖地板对齐（第二轮发版后 floor sync，无代码变更）

## [v0.13.1] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.13.0] — 2026-11-10

- - feat(ratelimit): PeriodLimit 补 rescue 降级三档（Off/FailOpen/BoundedLocal）+ WithRescueBucket 每 key 缓存降级桶，探活回切与 TokenLimiter 单源（#125）
- - feat(distlock): Semaphore 补 WithSemaphoreAutoRenew 自动续期家族——间隔缺省 slotTTL/2、MaxAutoRenew 安全网、Release 先停 watchdog（#126）
- - fix(ratelimit): ResilientLimiter local 工厂内置 per-key 缓存，每 key 真正只调一次，文档改写一致（#127）
- - feat(distlock): NewBruteForceGuard 防爆破锁对一等组合口——分布式+进程内双实现同阈值，Redis 故障自动路由 MemFallback（#128）
- - fix: NewClient(nil) 构造期 fail-fast 返回 error，不再 nil 指针 panic（#129）

关联问题单：redisx#125、redisx#126、redisx#127、redisx#128、redisx#129。

## [v0.12.0] — 2026-11-09

- fix(distlock): Unlock 假成功回归钉（#115 已由 #117 修复）
- fix(client): NewClient 不再原地改写调用方 UniversalOptions（#116）
- feat(ratelimit): ResilientLimiter——Redis 固定窗口+本地桶降级，三态口径单源（#118）
- fix(distlock): Lockout 三键 hash-tag——集群 CROSSSLOT 消除（#119）
- feat(distlock/cache): per-instance logger 注入点 WithLogger/RedisConfig.Logger（#120）

关联问题单：redisx#115、redisx#116、redisx#118、redisx#119、redisx#120。

## [v0.11.6] — 2026-11-08

- fix(distlock): Unlock 丢弃解锁脚本返回值的「假成功」关闭——脚本返回值转显式语义（#115）。修复前 `unlockScript.Run` 的返回值被 `_` 丢弃：锁因 TTL 过期易主后，陈旧持有者的 Unlock 拿到脚本返回 0（非本人锁未删）而 err==nil ⇒ **调用方得到「解锁成功」**，与同文件 Renew（同一情形返回 `(false, nil)` 要求调用方「停止当前工作」）的显式契约自相矛盾，调用方在不知情下继续假装持有、与新持有者并发写临界区。修复后 Unlock 返回值三态：nil = 锁已删除；新增哨兵 `ErrLockNotHeld`（errors.Is 判定）= 锁不存在或已易主、本次未删任何锁；其它 = Redis 通信故障（可重试）——与 Renew 的 `(bool, error)` 显式契约对齐（接口签名不动：Locker.Unlock 仍返回 error，etcdx 等同形后端不受影响）。onUnlock 回调收到与返回值一致的 err。**续期 watchdog 时序同步修正**（issue 第二半）：删锁（网络）先行、watchdog 后停——修复前先 LoadAndDelete 杀 watchdog 再走网络删锁，删锁网络瞬断时「续期已停而锁还挂着」⇒ 锁将在 TTL 后静默失效，调用方拿到的却是可重试 error、误以为「还没解开所以还持有」继续用临界区；现在仅当删锁结果明确（成功/非本人）才清理状态，通信故障时 watchdog 继续续期兜底、锁不静默丢失。状态清理保持「LoadAndDelete + ownerID 比对 + LoadOrStore 回填」竞态安全口径，绝不误杀新持有者的续期。回归用例：TTL 过期易主后陈旧持有者解锁得 ErrLockNotHeld 且新持有者锁完好、错误 ownerID 解锁显式报错不再假成功、通信故障时续期状态与 watchdog 原样保留、onUnlock 回调语义；TestAutoRenew_MaxLifetimeExpiry 按新契约改判 ErrLockNotHeld

关联问题单：redisx#115。

## [v0.11.5] — 2026-11-08

- fix(bloom): NewBloomFilter 对 BF.RESERVE 错误不再按 "exists" 文本匹配吞错——改用 BF.INFO 探测区分「已存在」与「真失败」（#112）。修复前 `strings.Contains(err.Error(), "exists")` 会把任何恰好含该词的错误消息（代理/集群客户端的连接级文案如 "connection pool exhausted, no free conn exists"）误判成「已存在，复用」并静默返回过滤器句柄，后续 BF.ADD 打在不存在的过滤器上必然失败且错误点远离根因；反之 RedisBloom 各版本改措辞则合法创建被误报失败。修复后：RESERVE 报错 → BF.INFO 探测——INFO 成功 ⇒ 过滤器确实存在，照旧复用（语义不变，且与 RedisBloom 各版本错误措辞、miniredis 与真实 Redis 的错误形态无关）；INFO 也失败 ⇒ RESERVE 失败另有原因（连接故障/模块缺失等），原始错误包装上抛不吞。回归钉：SetError 注入含 "exists" 的连接级错误消息断言不再被吞（旧实现必红）+ 模块缺失形态错误上抛
- fix(cache): RedisCache.SetWithTTL 对 ttl<=0 fail-fast 返回错误，不再静默写永久键（#113）——go-redis 对 TTL=0 不下发 EX ⇒ 旧实现直接透传 ttl=0 写出**永不过期键**，与同结构体 Set 的「TTL=0 → 24h 缺省」在同一类型上构成两种相反语义（零值笔误：漏传时长/时长变量未初始化），且经 twolevel.SetWithTTL 双层转发时 L1/L2 同时放大。对齐仓内护栏哲学（distlock Renew TTL 下限、ratelimit NewPeriodLimit quota<=0 fail-fast、#92 同族）：显式自定义 TTL 却 <=0 视为调用方 bug，响亮拒绝，不猜缺省也不落键（错误路径零写入）。Set 的 24h 缺省口径不变；twolevel 侧的同步护栏属 data 仓（L2 先写序下本修复已阻断放大链路），随 data 下一版跟进

关联问题单：redisx#112、redisx#113。

## [v0.11.4] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.11.3] — 2026-11-08

- docs(ratelimit): #99 收口——该单为 #91 的重报（tokenLimitLua TTL 整除 0 致 SETEX invalid expire time、限流器每次判定必失败），修复已随 v0.11.2（PR #94）落地（TTL 下限 1s，含 rate=1000/s、burst=1 的 miniredis 契约回归测试）；本版把 scripts.go 与回归测试的注释标注对齐重报单号，无行为变更

关联问题单：redisx#91、redisx#99。

## [v0.11.2] — 2026-11-07

- fix(ratelimit): tokenLimitLua TTL 整除为 0（rate > 2×burst）时 SETEX 0 报错关闭——TTL 下限 1s，限流器不再每次判定必失败（#91）
- fix(ratelimit): NewPeriodLimit 对 quota<=0 fail-fast panic——与 NewTokenLimiter 校验哲学对齐，全量静默拒绝面关闭（#92）
- fix(distlock): Lockout Window/Penalty 毫秒截断 0 关闭——复用 ttlMillis 下限 1ms，PEXPIRE 0 立即删键不再发生（#93）

关联问题单：redisx#91、redisx#92、redisx#93。

## [v0.11.1] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.11.0] — 2026-11-03

- feat(redisx): Pipeline()/TxPipeline() 改返键前缀代理 *Pipeline——convertKey 全覆盖、未覆盖命令编译期拦截（#79）
- feat(redisx): Client.GetDel/GetDelCompat 取即删——<6.2 时 Lua 单次原子回退，核销不双花（#78）
- ⚠️ breaking：Pipeline()/TxPipeline() 返回类型 redis.Pipeliner → *redisx.Pipeline，需要原生管道的场景走 Raw()

关联问题单：redisx#78、redisx#79。

## [v0.10.9] — 2026-11-02

- build: 矩阵内依赖地板 `pkg` v0.31.0 → v0.31.1（维护轮全量对齐；本版不含 API 变更）

## [v0.10.8] — 2026-10-29

- build: 依赖地板对齐——concurrency 抬到 v0.23.2

## [v0.10.7] — 2026-10-28

- fix(distlock): 续期的亚毫秒 TTL 不得删锁（#54）——PEXPIRE 0 在 Redis 里等价于立即删除

`Renew` 把 TTL 量化为毫秒（`ttl.Milliseconds()`），而 **0 < ttl < 1ms 时量化结果是 0** ⇒ Redis 收到 `PEXPIRE key 0` ⇒ 按 Redis 语义**直接删除该键**并回复 1 ⇒ Go 侧返回 `(true, nil)`。

⇒ **锁已经被删掉，但调用方被告知「续期成功」**（自动续期 watchdog 走同一条路径 ⇒ 同一个洞）。

后果形态最坏：调用方拿到 `(true, nil)` 后继续在「自己并不持有锁」的状态下写临界区 ⇒ 另一实例可同时 `SetNX` 成功 ⇒ **两个实例并发写同一资源**（静默脑裂）。配 `WithAutoRenew()` 时更短命：`interval = ttl/2 = 250µs`，取锁后 250µs 内第一次 tick 就把锁删掉，而 watchdog 要到下一拍才发现「lock lost」⇒ 炸法是跨实例重复执行同一业务动作，日志里只有一条 `auto renew failed: lock lost`，**看不出续期「成功」过**。

三处修：① 续期量化加**下限 1ms**（与写路径 go-redis `formatMs` 的抬升一致 ⇒ 不会出现「写 1ms、续期 0ms」的自相矛盾）；② Lua 脚本自身兜住：`ARGV[2] <= 0` 时只返回 0、**绝不执行 PEXPIRE 0**（双保险）；③ watchdog 间隔在由 ttl 推导时取 1ms 下限（显式 `WithRenewInterval` 不受影响）。

补 4 条回归用例：亚毫秒续期后**锁必须仍在**、脚本对 ≤0 毫秒返回 0 且不删键、`ttlMillis` 量化边界、亚毫秒 + 自动续期的观察窗口（#54）

## [v0.10.6] — 2026-10-28

- fix(distlock): `WithRetryInterval(0/负值)` 被无条件写入 ⇒ `Lock()` 里的 `time.NewTicker` 直接 panic（"non-positive interval for NewTicker"）——一个选项参数把进程带崩。修法：选项忽略非正值（保留默认 100ms），`Lock()` 再兜一层
- fix(distlock): `Unlock` 缺 nil client 的 fail-fast 检查（`TryLock`/`Renew` 都有）⇒ `unlockScript.Run` 在 nil interface 上调用 ⇒ 运行时 nil 指针 panic

## [v0.10.5] — 2026-10-27

- feat(client): 加 NewClientFromClient——包住既有 go-redis 客户端（纯包装：不新建连接、不改连接参数），供自己管连接池却想用 redisx 原语（Counter.IncrWithTTL 等）的消费方；另修 ratelimit 的 TokenLimiter_Fallback_Engage 时序竞态（CI 慢机上偶发红）

## [v0.10.4] — 2026-10-24

- fix(bloom): BloomFilter 测试状态泄漏——固定 key 复跑必红，改唯一 key + 用完即删；NewBloomFilter godoc 补齐「key 已存在时复用既有过滤器」语义（#41）

## [v0.10.3] — 2026-10-24

- fix(distlock): WithMaxAutoRenew 安全网不再形同虚设——计时器改在续期 goroutine 内创建与停止，最大生命周期到期即放弃续期、锁在 TTL 后自然过期；此前 defer 绑定在 startAutoRenew 作用域，函数返回瞬间计时器即被 Stop，安全网从未触发、锁被无限续期（#39）

## [v0.10.2] — 2026-10-23

- fix(distlock): Lock() 不再重复启动自动续期 goroutine——同一把锁只启一次续期，Unlock 后不再残留孤儿 goroutine，也不再打出误导性的「锁丢了」WARN（#33）

## [v0.10.1] — 2026-10-23

- docs: 更正旧模块路径的可拉取范围（≤ v0.4.0，不是 v0.3.1）与更名版本号（v0.5.0，不是 v0.4.0）——仍在旧路径 github.com/oekit/redis 的消费方请显式 pin v0.4.0 或迁到 github.com/oekit/redisx；不要依赖 go get github.com/oekit/redis@latest（proxy 的 @latest 指向 v0.10.0，会撞 module path 校验失败）（#31）

## [v0.10.0] — 2026-10-22

- Go 最低版本地板 1.24.0 → 1.26.0：依赖地板随上游上移（go-redis v9.23.0 / x/time v0.16.0 / x/sys v0.48.0 均要求 go>=1.26），CI 的 go-version 同步跟到 1.26 以满足「成对」判据（#25）

**BREAKING**: 最低 Go 版本地板 1.24.0 → 1.26.0——工具链被钉在 1.24/1.25 的下游（GOTOOLCHAIN=local）会直接报 "go: go.mod requires go >= 1.26.0"，需升到 Go 1.26+。对外发布的地板本就是 1.26+（oekit BOM 徽章与 go.mod 均为 1.26），故不改变对外契约，只影响直接依赖本仓的下游；业务层地板 1.24/1.25/1.26 均可（matrix-tools#33 裁决）。

## [v0.9.1] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：concurrency v0.5.4 → v0.17.0、logx v0.6.0 → v0.7.0、pkg v0.11.0 → v0.15.1

## [v0.9.0] — 2026-10-18

### feat(distlock)：分布式防爆破锁 `Lockout`（[#6](https://github.com/oekit/redisx/issues/6)——与 Semaphore 成对的共享惩罚）
- `NewLockout(rdb, LockoutConfig{KeyPrefix, Threshold, Window, Penalty})`：
  滑窗失败计数达阈值 → key 进入锁定拒绝态持续 Penalty → 成功 `Reset` 清零
  （兼管理员解封）；`Fail`（返回窗口计数）/`Check`（locked + 剩余时长，供 429
  文案）/`Reset` 全部 Lua 原子执行
- 语义对齐 concurrency/ratelimit.Lockout（进程内孪生）：惩罚时长**只按首次触发
  计算**（锁定标记 `SET PX NX`，锁定期间的后续失败不延长）；到期自动解除，
  窗口内旧失败仍在时下一次 Fail 即重新锁定（重复违规者）；空闲 key 窗口
  PEXPIRE 自然消亡
- 多实例共享计数是本类型的存在理由：攻击者把失败尝试打散到不同实例即可绕过
  进程内锁；典型装配 = Redis 本类型为主 + concurrency/ratelimit.Lockout 进程内
  降级层（防爆破语义降级放行 = 静默失效，本类型不内置降级，nil client fail-fast）
- 实现坑（测试抓出）：Redis `EXPIRE` 是秒、`PEXPIRE` 是毫秒——窗口参数按毫秒
  传递错用 EXPIRE 会让窗口 key 长生不老、旧失败滞留导致「过期后首次失败即重锁」
- 测试 6 例（miniredis + 注入时钟双时区：窗口判定走 Go 毫秒、标记 TTL 走
  miniredis FastForward）：触发/到期/滑窗重算/惩罚不延长/Reset/键隔离/校验

## [v0.8.0] — 2026-10-18

- feat(distlock): 新增 `Semaphore` 分布式信号量（ZSET 槽位 + slotTTL + 持有者
  续期，#4）——`Acquire` Lua 原子「清过期槽→已持有幂等刷新→容量检查→占槽」，
  成员集合大小任何时刻不超 max；`Renew` 仅未过期槽可续（过期槽不得复活，
  配额可能已被他人接管），false=槽已丢失应停止工作；`Release` 幂等；
  `Cleanup(olderThan)` 巡检清理返回条数（宽限语义）；score 为过期 Unix 毫秒
  时间戳，持有者崩溃后槽位 slotTTL 过期自愈，多实例共享全局并发上限
  （收编 git-ferry-core redisGuard 同款语义）；不提供无 Redis 降级——配额
  语义降级放行即静默超限，nil client 一律报错
- feat(distlock): `RedisLocker.Renew(ctx, key, ownerID, ttl) (bool, error)`
  手动续期（#5）——暴露 renewScript 的 Lua CAS（GET==owner→PEXPIRE 毫秒
  精度），调用方自管 watchdog，不受 WithMaxAutoRenew 安全网约束；
  false=锁已丢失/易主，error=Redis 通信故障；方法落具体类型不进 Locker
  接口（etcdx 等后端实现保持接口同形可互换）
- feat(distlock)!: `WithAutoRenew` 覆盖 TryLock（#5）——TryLock 成功即注册
  续期状态，与 Lock 对称；「非阻塞获取 + 持有期续期」不再需要
  Lock(timeout=0) 模拟非阻塞的绕法；graceful 降级路径同步注册状态保持
  Unlock 状态机对称。行为变更：此前 WithAutoRenew+TryLock 组合不续期，
  现在会保活（未启用该选项的调用方零影响）
- docs: distlock doc.go 补任务互斥续期与信号量场景示例

## [v0.7.6] — 2026-10-17

- fix(deps): logx 地板 v0.5.0 → v0.6.0（零代码改动）

## [v0.7.5] — 2026-10-17

- fix(deps): pkg 地板 v0.10.0 → v0.11.0；concurrency 地板 v0.5.3 → v0.5.4（事故版已撤）

## [v0.7.3] — 2026-10-17

- fix(deps): pkg 地板 v0.9.3 → v0.10.0（拆分版，零代码改动）；concurrency 地板 v0.5.2 → v0.5.3（事故版已撤）

## [v0.7.1] — 2026-10-17

- fix(deps): 内部依赖地板全量对齐最新——pkg v0.6.0 → v0.9.3、concurrency v0.1.5 → v0.5.2、logx v0.1.9 → v0.5.0；go 最低版本降至 1.24.0——go-redis v9.22.0、x/time v0.14.0、x/sys v0.41.0 锁 go1.24 兼容版

## [v0.7.0] — 2026-10-16

- feat(cache): `RedisCache` 新增 `MGet` 批量读取（管道一次往返，实现
  pkg/cache `BatchGetter[T]` 可选能力）——缺席语义与 (nil, nil) 双关一致：
  不存在与 null marker（nullValue 哨兵精确识别）一律缺席，损坏条目按
  miss（与 getInternal 口径一致）；纯读不回填不触发 Source；反序列化
  失败整批报错；消费方（如 data/cached GetByIDs）对缺席键回退 GetOrLoad
  享受 marker 短路，N 次往返收敛为 1 次
- fix(ci): 修存量 lint——redis_test 两处 govet shadow + tokenlimit_test
  SA4000（两次 Allow 有副作用但表达式字面相同，拆变量规避误报）
- deps: github.com/oekit/pkg v0.4.0 → v0.6.0（BatchGetter 接口）

## [v0.6.1] — 2026-10-16

- fix(ratelimit): 探活应答形态校验——`probe()` 此前只检查 `Err()==nil`，探活脚本应答形态
  异常（代理损坏、不兼容实现、mock 残缺应答）会被误判为已恢复、立刻回切 Redis；现要求
  应答为整数 1（probeLua 常量），否则保持兜底等待下一次探测。消除 TokenLimiter 兜底
  回切与后台探活 goroutine 的竞态闪烁
  （`TestTokenLimiter_UnknownResponse_FallbackEngages` 修复前 5 跑 1 挂）

## [v0.6.0] — 2026-10-16（补录）

- feat(cache)!: 缓存接口引用迁移 pkg/cache——data 转门面前置手术
  （接口住 pkg 下游，Redis 后端依赖接口）

## [v0.5.1] — 2026-10-17

- **docs**：CHANGELOG 版本段勘误（改名段实为 v0.5.0，与 v0.4.0 契约段重名问题修正）；README 补
  「规划」——cache 后端扩展模板（实现 data `CacheAside[T]` + 过 cachetest 契约套件）、两级缓存
  （pubsub 失效广播）单独立项；cache 组成行补契约套件说明

## [v0.5.0] — 2026-10-16

- **breaking（0.x minor）**：模块更名 `github.com/oekit/redis` → `github.com/oekit/redisx`，
  消除与 go-redis（`package redis`）的包名撞车与 `/redis/redis` 路径口吃
- 结构重排：原 `redisx` 子包上收为根包（`redisx.NewClientFromConfig` 等直接
  `import "github.com/oekit/redisx"`）；原 `redis/` 子包更名 `cache/`（package cache，
  `redis.RedisCache` → `cache.RedisCache`）；distlock/ratelimit 路径随之变为
  `github.com/oekit/redisx/distlock`、`github.com/oekit/redisx/ratelimit`
- 旧路径 `github.com/oekit/redis` 的可拉取范围 **≤ v0.4.0**——实测 v0.4.0 的
  `go.mod` 仍声明 `module github.com/oekit/redis`（更名自 **v0.5.0** 起才生效），
  `go get github.com/oekit/redis@v0.4.0` 可安装可编译；**v0.3.1 不是旧路径的
  最后版本**（它只是「挂弃用通告」的那一版，发布时刻还晚于 v0.4.0 3 分钟）。
  ⚠️ **不要依赖 `go get github.com/oekit/redis@latest`**：Go module proxy 一旦
  缓存了路径，`@latest` 按该路径下最高版本解析 ⇒ 解析到 **v0.10.0**，而它的
  `go.mod` 已是 `github.com/oekit/redisx` ⇒ 直接撞路径不匹配报错：
  `module declares its path as: github.com/oekit/redisx but was required as: github.com/oekit/redis`
  （2026-10-23 实测，见 [oekit/redisx#31](https://github.com/oekit/redisx/issues/31)）
- 迁移：import 路径机械替换 `github.com/oekit/redis/redisx` → `github.com/oekit/redisx`、
  `github.com/oekit/redis/redis` → `github.com/oekit/redisx/cache`、
  `github.com/oekit/redis/distlock|ratelimit` → `github.com/oekit/redisx/distlock|ratelimit`

## [v0.4.0] — 2026-10-16

RedisCache 后端契约化：接口断言 + cachetest 契约套件接入 + null 哨兵碰撞修复。

### Breaking（数据格式）
- **null 哨兵变更**：`__NULL__` → `"\x00oekit:null"`（以 `\x00` 开头的值保留为
  null 标记专用前缀）。旧哨兵可被 `SetRaw` 合法写入精确碰撞（写入字面
  `__NULL__` 即被永久误判为"数据不存在"，GetOrLoad 不再回源）；新哨兵带
  `\x00` 前缀，合法 JSON 序列化输出不会自然撞上。**存量影响**：旧缓存中由
  GetOrLoad 写入的 `__NULL__` 标记不再被识别——读回按普通值反序列化（默认
  JSON 下报错），该 key 的防穿透短暂失效，标记自身按 NullTTL（默认 1 分钟）
  自然过期后恢复；零已知消费方，无实际影响面。走 JSON 序列化的 Set/GetOrLoad
  正常值不受影响（json.Marshal 字符串带引号，从未与裸哨兵精确相等）
- `SetRaw` 新增契约约束：data 不得以 `\x00` 开头（保留为 null 标记前缀；
  精确等于哨兵的字节按 null marker 处理，其余 `\x00` 开头值不认作 null）

### 新增
- 编译期接口断言：`RedisCache` 显式满足 `data/cache` 门面 `Cache[T]` /
  `CacheAside[T]`（int/string 双类型实例化）
- 接入 `github.com/oekit/data/cachetest` 契约测试套件（8 场景 × string/
  结构体双值类型，每场景独立 miniredis 并开启 `ClockTTL` 对齐真实时间过期
  语义，`-race` 全绿）——通用缓存语义不再在本包重复编写

### 修复
- `DelBatch` 空 keys 由报错（Redis 裸 `DEL` 要求至少 1 个 key，返回
  wrong number of arguments）改为幂等 no-op——契约套件 `del_and_delbatch`
  场景（空 DelBatch 不应报错）发现的违约，与 `Del` 幂等语义对齐

### 变更
- import 路径正名：`github.com/oekit/data/data/cache` → `github.com/oekit/data/cache`
  （跟随 data v0.4.0 目录正名）

### 依赖
- `github.com/oekit/data` v0.3.3 → v0.4.0（v0.4.0 发布前本地经 go.work
  replace 解析）

## [v0.3.1] — 2026-10-16

- ⚠️ 旧路径弃用通告版：模块已更名为 github.com/oekit/redisx（**更名自 v0.5.0
  起生效**——本版本与 v0.4.0 的 `go.mod` 仍声明旧路径 `github.com/oekit/redis`）；
  本版本代码与 v0.3.0 完全一致，仅 README 挂弃用通告
  > 本版本的 tag 发布于 **2026-10-16 10:20:14**，**晚于**同属旧路径线的 `v0.4.0`
  > （10:17:14）3 分钟——旧路径线内版本号回退。**旧路径的最后一个可安装版本是
  > `v0.4.0`**，不是本版本。事实与处置见
  > [oekit/redisx#31](https://github.com/oekit/redisx/issues/31)；跨仓门禁口径见
  > [oekit/matrix-tools#88](https://github.com/oekit/matrix-tools/issues/88)。

## [v0.3.0] — 2026-10-16

新增 `ratelimit/` 分布式限流子包（Lua 脚本与三态返回语义忠实移植 go-zero core/limit）。

### 新增
- `ratelimit.PeriodLimit`——Redis 固定窗口限流（INCRBY+EXPIRE 原子脚本）：
  `Take(ctx, key)` 返回三态 `Allowed`/`HitQuota`/`OverQuota`（Redis 故障返回
  `Unknown`+err，不消耗配额）；`WithPeriodAlign()` 对齐自然周期（如"每日 5 条"）；
  **不做本地兜底**——固定窗口计数无法本地近似，fail-open/closed 由调用方决定
- `ratelimit.TokenLimiter`——Redis 令牌桶限流（双 key 惰性回补，Cluster
  hash-tag 同 slot）：`Allow(ctx)`/`AllowN(ctx, n)`；`WithTokenFallback()` 开启
  Redis 故障时进程内 golang.org/x/time/rate 本地兜底 + 后台探活自动回切
  （`WithTokenPingInterval` 默认 100ms，探活超时 `WithTokenProbeTimeout`
  默认 1s、与间隔解耦）；未开启时故障 fail-closed
- 存储抽象收 go-redis v9 `redis.Scripter` 接口（Client/ClusterClient/Ring 均可用）；
  全部 ctx-only API；后台 goroutine 经 `concurrency/async.GoSafe` recover

### 依赖
- 新增直接依赖 `golang.org/x/time v0.16.0`（TokenLimiter 本地兜底，矩阵内与
  concurrency 同版本）；其余不变

## [v0.2.0] — 2026-10-15

可观测性统一轮。

### Breaking
- `redisx.NewClient` / `NewClientFromAddr` / `NewClientFromConfig` **不再自动注入 OTel tracing**
  （原先强制 `redisotel.InstrumentTracing`，无出口）；go.mod 不再依赖 go-redis extra

### 迁移
- 需要 Redis 命令 span 的服务在构造后一行显式接入（无 SDK 时 noop）：
  `redisotel.InstrumentTracing(c.Client())`（`github.com/oekit/tracing/redisotel`）；
  不需要的无需任何改动

## [v0.1.5] — 2026-10-15

- v0.1.5

## [v0.1.0] — 2026-10-15

自 ekit v0.34.0（git.enjoye.top/enjoydream/ekit，冻结维护）拆分而来的初始公开版本，完成拆分稳定化：模块路径统一为 `github.com/oekit/redisx`，多轮拆分/合并收敛至当前模块边界，组织基建（共享 CI workflow、golangci v2、Dependabot）就位。