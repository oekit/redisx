package redisx

import (
	"context"
	"errors"
	"fmt"
)

// RedisConfig 是 Redis 连接配置，可直接从 YAML 反序列化。
type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// NewClientFromConfig 从配置创建 Redis 客户端（构造后立即 Ping 验证连通性）。
//
// 用法：
//
//	client, err := redis.NewClientFromConfig(ctx, redis.RedisConfig{
//	    Host: cfg.Redis.Host, Port: cfg.Redis.Port,
//	    Password: cfg.Redis.Password, DB: cfg.Redis.DB,
//	})
func NewClientFromConfig(ctx context.Context, cfg RedisConfig) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
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
