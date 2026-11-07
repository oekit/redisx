package ratelimit

import "github.com/redis/go-redis/v9"

// periodLimitLua 固定窗口 Lua 脚本，逐字移植 go-zero core/limit/periodscript.lua
// （兼容 aliyun redis 的写法一并保留）。
//
// 参数：KEYS[1]=计数 key；ARGV[1]=配额 limit；ARGV[2]=窗口秒数 window。
// 返回：1=允许 2=恰好命中配额 0=超限。
// 首次计数（INCRBY 结果为 1）时设置 EXPIRE 窗口过期，之后不再续期——
// 这正是"固定窗口"：窗口起点由首个请求决定。
const periodLimitLua = `-- to be compatible with aliyun redis, we cannot use ` + "`local key = KEYS[1]`" + ` to reuse the key
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local current = redis.call("INCRBY", KEYS[1], 1)
if current == 1 then
    redis.call("expire", KEYS[1], window)
end
if current < limit then
    return 1
elseif current == limit then
    return 2
else
    return 0
end`

// tokenLimitLua 令牌桶 Lua 脚本，移植 go-zero core/limit/tokenscript.lua，
// 唯一偏离（#91）：TTL 取 math.max(1, ...) 下限。
//
// 参数：KEYS[1]=令牌数 key；KEYS[2]=上次回补时间戳 key；
// ARGV[1]=速率 rate；ARGV[2]=容量 capacity；ARGV[3]=当前 unix 秒；ARGV[4]=本次请求数 requested。
// 返回：true（整数 1）=允许；false（nil 应答）=拒绝。
// 惰性回补：按 (now-last_refreshed)*rate 回补令牌（封顶 capacity），
// 两个 key 均以 2*fill_time 的 TTL 落地；KEYS 须同 slot（hash-tag）。
//
// TTL 下限 1s（#91）：rate > 2*burst（如 1000/s、burst=1）时
// floor(capacity/rate*2) == 0，而 `SETEX key 0` 在 Redis 语义是报错
// "invalid expire time" ⇒ 脚本每次必失败（开 fallback 则永久降级本地桶，
// 不开则所有请求被拒）。与 distlock ttlMillis 的 1ms 下限（#54）同族护栏，
// 这里按脚本自己的单位（秒）取下限 1。
const tokenLimitLua = `-- to be compatible with aliyun redis, we cannot use ` + "`local key = KEYS[1]`" + ` to reuse the key
-- KEYS[1] as tokens_key
-- KEYS[2] as timestamp_key
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local fill_time = capacity/rate
local ttl = math.max(1, math.floor(fill_time*2))
local last_tokens = tonumber(redis.call("get", KEYS[1]))
if last_tokens == nil then
    last_tokens = capacity
end

local last_refreshed = tonumber(redis.call("get", KEYS[2]))
if last_refreshed == nil then
    last_refreshed = 0
end

local delta = math.max(0, now-last_refreshed)
local filled_tokens = math.min(capacity, last_tokens+(delta*rate))
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
if allowed then
    new_tokens = filled_tokens - requested
end

redis.call("setex", KEYS[1], ttl, new_tokens)
redis.call("setex", KEYS[2], ttl, now)

return allowed`

// probeLua 探活脚本：连通性 + 脚本执行能力的最小探测。
// redis.Scripter 接口没有 Ping，故以只含常量返回的 EVAL 代替；
// nil keys（EVAL ... 0）合法，不会触碰任何数据。
const probeLua = `return 1`

var (
	// periodScript 固定窗口脚本的 EVALSHA（miss 自动回退 EVAL）包装。
	periodScript = redis.NewScript(periodLimitLua)
	// tokenScript 令牌桶脚本的 EVALSHA（miss 自动回退 EVAL）包装。
	tokenScript = redis.NewScript(tokenLimitLua)
	// probeScript 探活脚本的 EVALSHA（miss 自动回退 EVAL）包装。
	probeScript = redis.NewScript(probeLua)
)
