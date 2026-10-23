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