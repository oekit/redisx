package cache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/oekit/logx"
	"github.com/oekit/pkg/cache"
	"github.com/oekit/pkg/contextx"
	pkgerrors "github.com/oekit/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// defaultNullTTL 是 NullTTL 未设置（0）时的空值缓存默认 TTL。
const defaultNullTTL = 1 * time.Minute

// defaultTTL 是 RedisConfig.TTL 未设置（0）时的缺省缓存 TTL。
const defaultTTL = 24 * time.Hour

// RedisConfig 配置 Redis 缓存实例。
type RedisConfig[T any] struct {
	// Prefix key 前缀，如 "app:user:"。
	Prefix string

	// TTL 缓存过期时间。默认 24 小时。
	TTL time.Duration

	// Source 缓存未命中时的数据获取函数（Cache-Aside 模式）。
	// 返回 (nil, nil) 表示数据不存在（会缓存空值防穿透）。
	Source cache.SourceFunc[T]

	// Serializer 自定义序列化函数。默认使用 JSON。
	Serializer func(*T) ([]byte, error)

	// Deserializer 自定义反序列化函数。默认使用 JSON。
	Deserializer func([]byte, *T) error

	// NullTTL 空值缓存过期时间（防穿透）。
	//   - 0（未设置）：使用默认值 1 分钟；
	//   - 负值：显式禁用空值缓存（Source 返回 nil 时不写 null marker，
	//     下次 GetOrLoad 会重新调用 Source）；
	//   - 正值：按指定时长缓存 null marker。
	NullTTL time.Duration

	// Logger per-instance 日志注入（#120，与 oagentkit sqllease 的
	// WithLeaderLogger 同形态的构造期注入点）：回填失败/序列化失败等内部
	// 告警只进该 logger——宿主用 zap/slog/hlog 时无需动进程级
	// logx.SetDefault（那会同时改掉所有 logx 使用方的落点），即可单独
	// 收编本实例的日志。nil（未设置）时使用本实例私有的
	// logx.Delegating("cache-redis")——与历史包级缺省行为完全一致
	// （懒解析 logx.Default，宿主未 SetDefault 时落 stderr）。
	Logger logx.Logger
}

// RedisCache 是基于 Redis 的泛型缓存实现。
// 实现 Cache[T] 接口，当配置了 Source 时还实现 CacheAside[T] 接口。
type RedisCache[T any] struct {
	client  redis.UniversalClient
	cfg     RedisConfig[T]
	nullTTL time.Duration
	// defaultJSON 标记 Serializer/Deserializer 均为内置 JSON 实现（构造期
	// 未注入自定义编解码器）。读路径的历史 "null" 兼容（#258）只对内置
	// 编解码启用：自定义编解码器的字节值域由调用方定义，不替它解释。
	defaultJSON bool
	// log 是 per-instance 日志（#120）：cfg.Logger 注入或缺省
	// logx.Delegating("cache-redis")，构造期定死、运行期只读。
	log logx.Logger
}

// 编译期接口契约：RedisCache 实现 data/cache 门面接口（泛型接口取具体
// 类型实例化断言，int/string 双类型覆盖）。接口语义（miss 非 error、
// null marker 防穿透、Del 幂等等）由 github.com/oekit/pkg/cache/cachetest
// 契约套件跨实现固化，本包 contract_suite_test.go 接入。
var (
	_ cache.Cache[int]         = (*RedisCache[int])(nil)
	_ cache.CacheAside[int]    = (*RedisCache[int])(nil)
	_ cache.CacheAside[string] = (*RedisCache[string])(nil)
)

// NewRedisCache 创建一个基于 Redis 的泛型缓存实例。
func NewRedisCache[T any](client redis.UniversalClient, cfg RedisConfig[T]) *RedisCache[T] {
	nullTTL := cfg.NullTTL
	if nullTTL == 0 {
		// 0 = 未设置 → 使用默认 1 分钟（防穿透默认开启）；
		// 负值保留原值（显式禁用），写入 null marker 前以 nullTTL > 0 判断。
		nullTTL = defaultNullTTL
	}

	c := &RedisCache[T]{
		client:  client,
		cfg:     cfg,
		nullTTL: nullTTL,
		// 默认编解码判定必须在下方「缺省填充」之前（填充后无法区分用户
		// 显式传 defaultSerializer 与未传）。
		defaultJSON: cfg.Serializer == nil && cfg.Deserializer == nil,
		log:         logx.Delegating("cache-redis"),
	}
	if cfg.Logger != nil {
		c.log = cfg.Logger
	}

	if c.cfg.Serializer == nil {
		c.cfg.Serializer = defaultSerializer[T]
	}
	if c.cfg.Deserializer == nil {
		c.cfg.Deserializer = defaultDeserializer[T]
	}

	return c
}

// Get 获取缓存（纯缓存查询，不自动回填）。
// 返回 (nil, nil) 表示数据不存在 — 包括 key 不存在和命中 null marker 两种情况。
// 调用方需要 null marker 区分时使用 Exists 或 GetRaw。
func (c *RedisCache[T]) Get(ctx context.Context, key string) (*T, error) {
	ctx = contextx.OrBackground(ctx)
	val, _, err := c.getInternal(ctx, key)
	return val, err
}

// getInternal 内部方法，返回 (值, 是否命中 null marker, error)。
// 命中 null marker 时返回 (nil, true, nil)；
// key 不存在返回 (nil, false, nil)；
// 命中旧版本 Set(key, nil) 写出的精确 "null" 时返回 (nil, false, nil)
// （#258 升级兼容：按 miss 处理以便 GetOrLoad 回源覆盖自愈）；
// 命中有效值返回 (&T, false, nil)；
// 命中损坏条目（反序列化失败）同样返回 (nil, false, nil)——与空字节同口径
// 按 miss 处理（#213），由 GetOrLoad 回源覆盖完成自愈。
// 用于 GetOrLoad 区分"未命中"和"命中 null marker"，避免防穿透失效。
func (c *RedisCache[T]) getInternal(ctx context.Context, key string) (*T, bool, error) {
	fullKey := c.fullKey(key)

	data, err := c.client.Get(ctx, fullKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, pkgerrors.Wrapf(err, "cache get")
	}

	// 空值标记命中（仅认精确哨兵）：ekit 写入的 null marker 是 nullValue
	// 字符串。len==0 曾一并当作 null——SetRaw/损坏写入的空字节会被永久
	// 伪造成"数据不存在"，GetOrLoad 不再调 Source（防穿透变永久伪 miss），
	// 且 GetBytes 只认哨兵，两个读入口口径不一致。
	// 空字节按"条目损坏"处理：视为未命中走回源覆盖，既不伪 null 也不反序列化报错。
	if len(data) == 0 {
		return nil, false, nil
	}
	if string(data) == nullValue {
		return nil, true, nil
	}
	// 升级兼容（#258）：旧版本 Set(key, nil) 经内置 JSON 序列化写出精确
	// "null" 字节；json.Unmarshal("null", &result) 是无错 no-op ⇒ 旧实现
	// 把它还原成 &T{} 零值伪命中，GetOrLoad 见 result != nil 直接返回、
	// 永不回源。按 **miss**（而非 null marker）处理：标记会短路 Source，
	// 旧条目将永远无法自愈；miss 让 GetOrLoad 回源并覆盖，首次读即修复。
	// 仅内置编解码启用（自定义字节值域不替调用方解释）。
	if c.isLegacyJSONNull(data) {
		return nil, false, nil
	}

	var result T
	if err := c.cfg.Deserializer(data, &result); err != nil {
		// 非空但不可反序列化的条目（SetRaw 写入非 JSON、部分写入、序列化
		// 格式变更、他人绕过封装直写）同样按"条目损坏 → miss"处理（#213），
		// 与上方空字节口径对齐：此前硬报错会让 GetOrLoad 短路返回、Source
		// 永不被调用，坏条目存活到 TTL（缺省 24h）期间所有读取全部失败且
		// 无自愈路径。归一为 miss 后 Source 回源覆盖即完成自愈；纯 Get 调用
		// 方拿到与"key 不存在"一致的 miss 口径（GetBytes 仍原样返回原始
		// 字节，需要区分损坏与否的走它）。仅告警不阻断。
		c.log.Error("corrupt entry, treating as miss", "key", key, logx.ErrKey, err)
		return nil, false, nil
	}
	return &result, false, nil
}

// GetOrLoad 获取缓存，未命中时通过 Source 加载并回填（Cache-Aside 模式）。
// 必须配置 Source 函数，否则等同于 Get。
//
// 命中 null marker 时直接返回 (nil, nil)，**不会**再调 Source，
// 避免对不存在的 key 形成 thundering-herd 击穿。
func (c *RedisCache[T]) GetOrLoad(ctx context.Context, key string) (*T, error) {
	ctx = contextx.OrBackground(ctx)
	// 1. 先查缓存
	result, isNullMarker, err := c.getInternal(ctx, key)
	if err != nil {
		return nil, err
	}
	if result != nil {
		return result, nil
	}
	if isNullMarker {
		// 命中 null marker：直接返回，不调 Source（防穿透）
		return nil, nil
	}

	// 2. 缓存未命中，调用 source
	if c.cfg.Source == nil {
		return nil, nil
	}

	source, err := c.cfg.Source(ctx, key)
	if err != nil {
		return nil, err
	}

	// 3. 回填缓存
	if source == nil {
		// 缓存空值防穿透：直写 nullValue 哨兵（#247）——不经 c.Set 回填，
		// Set 对 nil fail-fast 与此无联动；此处写的是精确哨兵字节而非
		// "null" JSON，读路径按 null marker 识别（防穿透语义成立）。
		if c.nullTTL > 0 {
			if setErr := c.client.Set(ctx, c.fullKey(key), nullValue, c.nullTTL).Err(); setErr != nil {
				c.log.Error("set null value error", "key", key, logx.ErrKey, setErr)
			}
		}
		return nil, nil
	}

	data, err := c.cfg.Serializer(source)
	if err != nil {
		c.log.Error("serialize error", "key", key, logx.ErrKey, err)
		return source, nil // 序列化失败仍返回数据
	}

	ttl := c.effectiveTTL()
	if err := c.client.Set(ctx, c.fullKey(key), data, ttl).Err(); err != nil {
		c.log.Error("set error", "key", key, logx.ErrKey, err)
	}

	return source, nil
}

// GetBytes 获取原始字节数据（不经过序列化）。
func (c *RedisCache[T]) GetBytes(ctx context.Context, key string) ([]byte, error) {
	ctx = contextx.OrBackground(ctx)
	fullKey := c.fullKey(key)
	data, err := c.client.Get(ctx, fullKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	if string(data) == nullValue {
		return nil, nil
	}
	return data, nil
}

// Set 设置缓存。
//
// value == nil fail-fast（#247）：默认 JSON 序列化对 nil 指针产出字节 "null"，
// 旧读路径既不认 nullValue 哨兵、json.Unmarshal("null", &result) 又是无错
// no-op ⇒ Get 返回 &T{} **零值伪命中**；同操作的 LocalCache 则存 nil、
// Get 返回 (nil, nil)，同一 cache.Cache[T] 接口跨后端语义分叉。写侧继续
// 拒绝：读侧虽已把存量 "null" 按 miss 自愈（#258），新写入仍不应制造
// 「存了个值、读出来是 miss」的静默语义。表达「数据不存在」请走 GetOrLoad
// 的 null marker（见 NullTTL），删除键请走 Del。
func (c *RedisCache[T]) Set(ctx context.Context, key string, value *T) error {
	ctx = contextx.OrBackground(ctx)
	if value == nil {
		return pkgerrors.New("cache Set: value 为 nil——序列化型后端未定义 nil 值编码，写 \"null\" 字节会被读路径还原成零值伪命中（#247）；表达「无值」请走 GetOrLoad 的 null marker，删除键请走 Del")
	}
	fullKey := c.fullKey(key)

	data, err := c.cfg.Serializer(value)
	if err != nil {
		return pkgerrors.Wrapf(err, "cache serialize")
	}

	return c.client.Set(ctx, fullKey, data, c.effectiveTTL()).Err()
}

// effectiveTTL 返回本实例的生效 TTL（redisx#164，缺省口径单源）：
// cfg.TTL == 0（未设置）→ 24h 缺省；> 0 原样。GetOrLoad 回填与 Set 两个
// 写入口径从此编译期绑定同一实现，不再「注释里说口径一致」；与
// SetWithTTL 的 ttl<=0 fail-fast（#113）的显式自定义口径边界不变。
func (c *RedisCache[T]) effectiveTTL() time.Duration {
	if c.cfg.TTL == 0 {
		return defaultTTL
	}
	return c.cfg.TTL
}

// SetWithTTL 设置缓存（自定义 TTL）。
//
// ttl 必须 > 0，<=0 返回错误（#113）：go-redis 对 TTL=0 不下发 EX ⇒ 写出
// **永久键**——与本类型 Set 的「TTL=0 → 24h 缺省」口径在同一结构体上构成
// 两种相反语义，零值笔误（漏传时长/时长变量未初始化）会静默落成不过期键，
// 且经 twolevel.SetWithTTL 双层转发时 L1/L2 同时放大。对齐仓内护栏哲学
// （distlock/ratelimit 对同类零值笔误 fail-fast）：显式自定义 TTL 却 <=0 视为
// 调用方 bug，响亮拒绝，不猜缺省也不写永久键。
//
// value == nil 同样 fail-fast（#247，护栏与 Set 同口径）：nil 经默认 JSON
// 序列化写 "null" 字节，Get 读回零值伪命中，且与 LocalCache 的 (nil, nil)
// 口径跨后端分叉——自定义 TTL 的写入口不豁免该契约。
func (c *RedisCache[T]) SetWithTTL(ctx context.Context, key string, value *T, ttl time.Duration) error {
	ctx = contextx.OrBackground(ctx)
	if ttl <= 0 {
		return pkgerrors.Newf("cache SetWithTTL: ttl 必须 > 0（得到 %s）——0 会经 go-redis 写成永久键，与 Set 的 24h 缺省口径冲突（#113）", ttl)
	}
	if value == nil {
		return pkgerrors.New("cache SetWithTTL: value 为 nil——序列化型后端未定义 nil 值编码，写 \"null\" 字节会被读路径还原成零值伪命中（#247）；表达「无值」请走 GetOrLoad 的 null marker，删除键请走 Del")
	}
	fullKey := c.fullKey(key)

	data, err := c.cfg.Serializer(value)
	if err != nil {
		return pkgerrors.Wrapf(err, "cache serialize")
	}

	return c.client.Set(ctx, fullKey, data, ttl).Err()
}

// SetRaw 设置原始字节数据（不经过序列化）。
//
// 契约约束：data 不得以 \x00 开头——该前缀保留为 null 标记（见 nullValue）。
// 写入精确等于哨兵的字节会被读路径按 null marker 处理（Get/GetBytes 返回
// nil、GetOrLoad 不回源）；写入其他 \x00 开头值属违反契约，不认作 null，
// 走反序列化（默认 JSON 下报错）。
//
// ttl 必须 > 0，<=0 返回错误（redisx#172，与 [RedisCache.SetWithTTL] 的 #113
// 护栏同口径）：go-redis 对 TTL=0 不下发 EX ⇒ 静默写成永久键——显式自定义
// ttl 的入口对零值笔误（漏传时长/时长变量未初始化）一律响亮拒绝，不猜缺省
// 也不写永久键。
func (c *RedisCache[T]) SetRaw(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	ctx = contextx.OrBackground(ctx)
	if ttl <= 0 {
		return pkgerrors.Newf("cache SetRaw: ttl 必须 > 0（得到 %s）——0 会经 go-redis 写成永久键，与 Set 的 24h 缺省口径冲突（#113/#172）", ttl)
	}
	return c.client.Set(ctx, c.fullKey(key), data, ttl).Err()
}

// Del 删除缓存。
func (c *RedisCache[T]) Del(ctx context.Context, key string) error {
	ctx = contextx.OrBackground(ctx)
	return c.client.Del(ctx, c.fullKey(key)).Err()
}

// DelBatch 批量删除缓存。
// 空 keys 为幂等 no-op（不报错）——Redis DEL 要求至少 1 个 key，裸 DEL
// 会报 wrong number of arguments，此处显式短路对齐 cachetest 契约。
func (c *RedisCache[T]) DelBatch(ctx context.Context, keys ...string) error {
	ctx = contextx.OrBackground(ctx)
	if len(keys) == 0 {
		return nil
	}
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = c.fullKey(key)
	}
	return c.client.Del(ctx, fullKeys...).Err()
}

// MGet 批量获取（管道一次往返，契约见 pkg/cache.BatchGetter——缺席语义
// 与 (nil, nil) 双关一致：不存在与 null marker 一律缺席；纯读不回填、
// 不触发 Source）。损坏条目（反序列化失败）按 miss 缺席（#213，与
// getInternal 单键口径一致），不整批报错。
// 缺席键的消费方应回退 GetOrLoad——真 miss 回源回填，marker 命中在
// 本实现的 GetOrLoad 内短路，两种情况都不会重复回源。
func (c *RedisCache[T]) MGet(ctx context.Context, keys ...string) (map[string]*T, error) {
	ctx = contextx.OrBackground(ctx)
	if len(keys) == 0 {
		return map[string]*T{}, nil
	}
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = c.fullKey(key)
	}
	res, err := c.client.MGet(ctx, fullKeys...).Result()
	if err != nil {
		return nil, pkgerrors.Wrapf(err, "cache mget")
	}

	out := make(map[string]*T, len(keys))
	for i, raw := range res {
		if raw == nil {
			continue // key 不存在
		}
		s, ok := raw.(string)
		if !ok {
			continue
		}
		if s == nullValue {
			continue // null marker 命中：缺席（消费方回退 GetOrLoad 享受短路）
		}
		// 升级兼容（#258，与 getInternal 同口径）：旧版本 Set(key, nil) 写出
		// 的精确 "null" 按 miss 缺席，消费方回退 GetOrLoad 时回源覆盖自愈。
		if c.isLegacyJSONNull([]byte(s)) {
			continue
		}
		if len(s) == 0 {
			continue // 损坏条目按 miss（与 getInternal 口径一致）
		}
		var v T
		if err := c.cfg.Deserializer([]byte(s), &v); err != nil {
			// 损坏条目按 miss 缺席（#213）：单键 getInternal 已归一为 miss，
			// 此处口径对齐——一条坏数据不再让整批硬失败（其余命中键照常返回）。
			c.log.Error("corrupt entry, treating as miss", "key", keys[i], logx.ErrKey, err)
			continue
		}
		out[keys[i]] = &v
	}
	return out, nil
}

// 编译期能力契约：RedisCache 具备批量读取能力。
var _ cache.BatchGetter[int] = (*RedisCache[int])(nil)
var _ cache.BatchGetter[string] = (*RedisCache[string])(nil)

// Exists 检查缓存是否存在。
func (c *RedisCache[T]) Exists(ctx context.Context, key string) (bool, error) {
	ctx = contextx.OrBackground(ctx)
	n, err := c.client.Exists(ctx, c.fullKey(key)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Refresh 刷新缓存（删除后重新从 Source 加载）。
//
// 必须配置 Source 函数——未配置时**入口 fail-fast 返回 error**（redisx#215），
// 在 Del 之前拦截：纯缓存用法（只配 Prefix/TTL，Set/Get 完全合法）按 godoc
// 字面调 Refresh 时，旧实现先 Del 再走 GetOrLoad，而 GetOrLoad 在 Source==nil
// 时返回 (nil, nil)——调用方拿到「刷新完成、值为 nil」，实际缓存已被清空且
// 永不回源（把 nil 当「数据不存在」写回业务层即静默数据丢失）。
//
// Del 失败直接上抛（redisx#215），不再只记日志：删除失败后 GetOrLoad 读到
// 的是旧值，旧值冒充刷新结果返回同样是错误面断裂；中止即中止，调用方可重试。
func (c *RedisCache[T]) Refresh(ctx context.Context, key string) (*T, error) {
	ctx = contextx.OrBackground(ctx)
	if c.cfg.Source == nil {
		return nil, pkgerrors.New("cache Refresh: Source not configured")
	}
	if err := c.Del(ctx, key); err != nil {
		return nil, pkgerrors.Wrapf(err, "cache refresh del")
	}
	return c.GetOrLoad(ctx, key)
}

func (c *RedisCache[T]) fullKey(key string) string {
	return c.cfg.Prefix + key
}

// nullValue 是 null marker（空值缓存标记）的哨兵字节串。
//
// 保留前缀契约：以 \x00 开头的值保留为 null 标记专用——合法 JSON 序列化
// 输出不会出现以裸 \x00 开头的字节（控制字符会被转义为 \u0000 文本），
// 普通值不会自然撞上该前缀；SetRaw 等原始写入入口**不得写入以 \x00
// 开头的值**。读路径按**精确相等**识别 null marker：等于 nullValue 的
// 条目按 null 处理（Get/GetBytes 返回 nil、GetOrLoad 不回源）；其余
// \x00 开头值不认作 null（走反序列化，默认 JSON 下报错）——保留的是
// 前缀空间，不是前缀通配语义。
//
// 历史：v0.4.0 前哨兵为 "__NULL__"，可被 SetRaw 合法写入精确碰撞（写入
// 字面 "__NULL__" 即被永久误判为"数据不存在"），故改为 \x00 保留前缀。
const nullValue = "\x00oekit:null"

// legacyJSONNull 是历史版本 Set(key, nil) 经内置 JSON 序列化写出的精确
// 字节串（#258 升级兼容的判定值）。
const legacyJSONNull = "null"

// isLegacyJSONNull 判定条目是否为旧版本写出的 "null" 伪值。仅在内置
// Serializer/Deserializer 下启用（defaultJSON）；自定义编解码器自行定义
// 字节语义，本实现不替它把 "null" 解释成 miss。
func (c *RedisCache[T]) isLegacyJSONNull(data []byte) bool {
	return c.defaultJSON && string(data) == legacyJSONNull
}

func defaultSerializer[T any](v *T) ([]byte, error) {
	return json.Marshal(v)
}

func defaultDeserializer[T any](data []byte, v *T) error {
	return json.Unmarshal(data, v)
}
