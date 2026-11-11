package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/oekit/logx"
	"github.com/oekit/pkg/cache"
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
		log:     logx.Delegating("cache-redis"),
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
	val, _, err := c.getInternal(ctx, key)
	return val, err
}

// getInternal 内部方法，返回 (值, 是否命中 null marker, error)。
// 命中 null marker 时返回 (nil, true, nil)；
// key 不存在返回 (nil, false, nil)；
// 命中有效值返回 (&T, false, nil)。
// 用于 GetOrLoad 区分"未命中"和"命中 null marker"，避免防穿透失效。
func (c *RedisCache[T]) getInternal(ctx context.Context, key string) (*T, bool, error) {
	fullKey := c.fullKey(key)

	data, err := c.client.Get(ctx, fullKey).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("cache get: %w", err)
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

	var result T
	if err := c.cfg.Deserializer(data, &result); err != nil {
		return nil, false, fmt.Errorf("cache deserialize: %w", err)
	}
	return &result, false, nil
}

// GetOrLoad 获取缓存，未命中时通过 Source 加载并回填（Cache-Aside 模式）。
// 必须配置 Source 函数，否则等同于 Get。
//
// 命中 null marker 时直接返回 (nil, nil)，**不会**再调 Source，
// 避免对不存在的 key 形成 thundering-herd 击穿。
func (c *RedisCache[T]) GetOrLoad(ctx context.Context, key string) (*T, error) {
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
		// 缓存空值防穿透
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
func (c *RedisCache[T]) Set(ctx context.Context, key string, value *T) error {
	fullKey := c.fullKey(key)

	data, err := c.cfg.Serializer(value)
	if err != nil {
		return fmt.Errorf("cache serialize: %w", err)
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
func (c *RedisCache[T]) SetWithTTL(ctx context.Context, key string, value *T, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("cache SetWithTTL: ttl 必须 > 0（得到 %s）——0 会经 go-redis 写成永久键，与 Set 的 24h 缺省口径冲突（#113）", ttl)
	}
	fullKey := c.fullKey(key)

	data, err := c.cfg.Serializer(value)
	if err != nil {
		return fmt.Errorf("cache serialize: %w", err)
	}

	return c.client.Set(ctx, fullKey, data, ttl).Err()
}

// SetRaw 设置原始字节数据（不经过序列化）。
//
// 契约约束：data 不得以 \x00 开头——该前缀保留为 null 标记（见 nullValue）。
// 写入精确等于哨兵的字节会被读路径按 null marker 处理（Get/GetBytes 返回
// nil、GetOrLoad 不回源）；写入其他 \x00 开头值属违反契约，不认作 null，
// 走反序列化（默认 JSON 下报错）。
func (c *RedisCache[T]) SetRaw(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	return c.client.Set(ctx, c.fullKey(key), data, ttl).Err()
}

// Del 删除缓存。
func (c *RedisCache[T]) Del(ctx context.Context, key string) error {
	return c.client.Del(ctx, c.fullKey(key)).Err()
}

// DelBatch 批量删除缓存。
// 空 keys 为幂等 no-op（不报错）——Redis DEL 要求至少 1 个 key，裸 DEL
// 会报 wrong number of arguments，此处显式短路对齐 cachetest 契约。
func (c *RedisCache[T]) DelBatch(ctx context.Context, keys ...string) error {
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
// 不触发 Source）。反序列化失败按整批错误处理（与单键 Get 同口径）。
// 缺席键的消费方应回退 GetOrLoad——真 miss 回源回填，marker 命中在
// 本实现的 GetOrLoad 内短路，两种情况都不会重复回源。
func (c *RedisCache[T]) MGet(ctx context.Context, keys ...string) (map[string]*T, error) {
	if len(keys) == 0 {
		return map[string]*T{}, nil
	}
	fullKeys := make([]string, len(keys))
	for i, key := range keys {
		fullKeys[i] = c.fullKey(key)
	}
	res, err := c.client.MGet(ctx, fullKeys...).Result()
	if err != nil {
		return nil, fmt.Errorf("cache mget: %w", err)
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
		if len(s) == 0 {
			continue // 损坏条目按 miss（与 getInternal 口径一致）
		}
		var v T
		if err := c.cfg.Deserializer([]byte(s), &v); err != nil {
			return nil, fmt.Errorf("cache deserialize: %w", err)
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
	n, err := c.client.Exists(ctx, c.fullKey(key)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Refresh 刷新缓存（删除后重新从 Source 加载）。
// 必须配置 Source 函数。
func (c *RedisCache[T]) Refresh(ctx context.Context, key string) (*T, error) {
	if err := c.Del(ctx, key); err != nil {
		c.log.Error("refresh del error", "key", key, logx.ErrKey, err)
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

func defaultSerializer[T any](v *T) ([]byte, error) {
	return json.Marshal(v)
}

func defaultDeserializer[T any](data []byte, v *T) error {
	return json.Unmarshal(data, v)
}
