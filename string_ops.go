package redisx

import (
	"context"
	"strings"
	"time"

	"github.com/oekit/pkg/errors"
	"github.com/redis/go-redis/v9"
)

// ==================== String 操作 ====================

// Get 获取 key 的值。
func (c *Client) Get(ctx context.Context, key string) *redis.StringCmd {
	return c.client.Get(ctx, c.convertKey(key))
}

// Set 设置 key 的值。
func (c *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	return c.client.Set(ctx, c.convertKey(key), value, expiration)
}

// SetNX 仅当 key 不存在时设置。
func (c *Client) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	return c.client.SetNX(ctx, c.convertKey(key), value, expiration)
}

// GetDel 原子返回 key 的值并删除该键（Redis GETDEL，要求 Redis >= 6.2）。
//
// 一次性凭据（OAuth state、授权 code、短信/图形验证码）的「恰好一次核销」
// 语义：GETDEL 单命令原子取删，并发下也只有一个调用方拿得到值——GET+DEL
// 两步在并发下会双核销。与其它单发命令同口径：convertKey 套 key 前缀，
// 超时/观测随底层客户端配置；key 不存在时返回 redis.Nil。
//
// 部署在 Redis < 6.2（无 GETDEL）时用 GetDelCompat（自动原子回退）。
func (c *Client) GetDel(ctx context.Context, key string) *redis.StringCmd {
	return c.client.GetDel(ctx, c.convertKey(key))
}

// GetDelCompat 语义与 GetDel 相同，额外兼容 Redis < 6.2（无 GETDEL）：
// GETDEL 返回 unknown command 时回退到 Lua「GET+DEL」单次原子执行——
// 不是两步 GET+DEL（并发下会双核销），核销语义不降级。
//
// 为什么不直接全部走 Lua：GETDEL 是服务端单命令，成本低于 EVAL；
// 回退只在老 Redis 上触发一次（后续命令仍会先试 GETDEL）。
// 管道版不提供 Compat：unknown command 只在 Exec 时暴露，无法在排队期
// 选择回退路径（见 Pipeline 类型文档）。
func (c *Client) GetDelCompat(ctx context.Context, key string) *redis.StringCmd {
	physical := c.convertKey(key)
	cmd := c.client.GetDel(ctx, physical)
	if err := cmd.Err(); err == nil || !isUnknownCmdErr(err) {
		return cmd
	}
	return getDelByScript(ctx, c.client, physical)
}

// getDelCompatLua 原子取即删（GET+DEL 在脚本内单次执行，Redis < 6.2 回退路径）。
// key 不存在时返回 false（Lua nil）——与 GETDEL 缺失键的 redis.Nil 口径对齐。
var getDelCompatLua = redis.NewScript(`
local v = redis.call("GET", KEYS[1])
if v then
    redis.call("DEL", KEYS[1])
end
return v
`)

// getDelByScript 用 Lua 原子 GET+DEL 实现取即删。physicalKey 必须是
// 已过 convertKey 的物理键。返回值口径与 *redis.StringCmd 一致：
// 命中回值（含空串），缺失回 redis.Nil。
func getDelByScript(ctx context.Context, cc redis.UniversalClient, physicalKey string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, "getdel", physicalKey)
	res, err := getDelCompatLua.Run(ctx, cc, []string{physicalKey}).Result()
	switch {
	case errors.Is(err, redis.Nil):
		cmd.SetErr(redis.Nil)
		return cmd
	case err != nil:
		cmd.SetErr(errors.Wrap(err, "getdel compat", "key", physicalKey))
		return cmd
	case res == nil:
		// 脚本返回 Lua nil/false：key 不存在，对齐 GETDEL 的 redis.Nil
		cmd.SetErr(redis.Nil)
		return cmd
	}
	if s, ok := res.(string); ok {
		cmd.SetVal(s)
		return cmd
	}
	cmd.SetErr(errors.New("getdel compat: unexpected reply type for key " + physicalKey))
	return cmd
}

// isUnknownCmdErr 判定服务端不支持该命令（老 Redis 无 GETDEL 等）。
// go-redis 只透传服务端错误文本，各版本/代理措辞略有差异，按子串判定。
func isUnknownCmdErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown command") || strings.Contains(msg, "unknown subcommand")
}

// ==================== Key 操作 ====================

// Del 删除一个或多个 key。
func (c *Client) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.client.Del(ctx, c.convertKeys(keys)...)
}

// Exists 检查 key 是否存在。
func (c *Client) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	return c.client.Exists(ctx, c.convertKeys(keys)...)
}

// Expire 设置 key 的过期时间。
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.client.Expire(ctx, c.convertKey(key), expiration)
}

// ==================== 计数器操作 ====================

// Incr 将 key 的值加 1。
func (c *Client) Incr(ctx context.Context, key string) *redis.IntCmd {
	return c.client.Incr(ctx, c.convertKey(key))
}

// IncrBy 将 key 的值增加指定增量。
func (c *Client) IncrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return c.client.IncrBy(ctx, c.convertKey(key), value)
}

// Decr 将 key 的值减 1。
func (c *Client) Decr(ctx context.Context, key string) *redis.IntCmd {
	return c.client.Decr(ctx, c.convertKey(key))
}

// DecrBy 将 key 的值减少指定减量。
func (c *Client) DecrBy(ctx context.Context, key string, value int64) *redis.IntCmd {
	return c.client.DecrBy(ctx, c.convertKey(key), value)
}
