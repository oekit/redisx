package redisx

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
)

// RedisConfig 是 Redis 连接配置，可直接从 YAML 反序列化。
//
// Host/Port 必填（redisx#217）：Host 为空或 Port 越界时 NewClientFromConfig
// 构造期 fail-fast 返回 error——漏配 redis.host 不是错误而是「改连本机」
// （":6379" 经 Go dial 解析为回环），本地恰好有 Redis 时 Ping 通过、数据
// 写进错误实例且零日志。
type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// NewClientFromConfig 从配置创建 Redis 客户端（构造后立即 Ping 验证连通性）。
//
// 构造期校验（redisx#217）：Host 为空、Port 不在 1..65535 时直接返回 error，
// 不发起任何拨号——避免漏配 redis.host 时 ":6379" 静默连本机、Ping 通过后
// 写错实例。地址用 net.JoinHostPort 拼接，Host 为 IPv6 字面量（如 "::1"）
// 时正确括号化为 "[::1]:6379"（"%s:%d" 会拼出非法地址，错误停在 dial 期
// 且不点名 host）。
//
// 用法：
//
//	client, err := redis.NewClientFromConfig(ctx, redis.RedisConfig{
//	    Host: cfg.Redis.Host, Port: cfg.Redis.Port,
//	    Password: cfg.Redis.Password, DB: cfg.Redis.DB,
//	})
func NewClientFromConfig(ctx context.Context, cfg RedisConfig) (*Client, error) {
	if cfg.Host == "" {
		return nil, fmt.Errorf("redisx: redis host is empty (config key redis.host)")
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return nil, fmt.Errorf("redisx: redis port %d out of range 1..65535 (config key redis.port)", cfg.Port)
	}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	client, err := NewClientFromAddr(addr, cfg.Password, cfg.DB)
	if err != nil {
		return nil, err
	}
	if err := pingOrCleanup(ctx, client); err != nil {
		return nil, err
	}
	return client, nil
}

// pingOrCleanup 连通性探测：Ping 失败时回收已创建的 client（连接池已拨号），
// 并把回收失败一并上抛（#149）。
//
// 契约（本仓错误处理硬规则：所有 error 禁止 `_` 忽略）：连接池已拨号而 Ping
// 失败，恰是**唯一一次**能观测到连接回收失败（半开连接、fd 回收失败）的时机，
// 回收错误不得静默丢弃——与 Ping 错误经 errors.Join 聚合上抛（两侧各自 %w
// 包装，errors.Is/As 判定口径不变）；回收成功时返回口径与历史一致（仅 Ping
// 错误）。回收走 Client.Close(ctx) 幂等单源（#148），不绕过 sync.Once。
func pingOrCleanup(ctx context.Context, client *Client) error {
	if err := client.Ping(ctx).Err(); err != nil {
		if cerr := client.Close(ctx); cerr != nil {
			return errors.Join(
				fmt.Errorf("redis ping failed: %w", err),
				fmt.Errorf("redis close failed: %w", cerr),
			)
		}
		return fmt.Errorf("redis ping failed: %w", err)
	}
	return nil
}
