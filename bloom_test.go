package redisx

import (
	"context"
	"strings"
	"testing"
)

// TestBloomFilter_ErrorContainingExists_NotSwallowed 回归钉（#112）：RESERVE
// 报错不再按错误文本猜「已存在」——错误消息恰好含 "exists" 的连接/代理级
// 故障（如 "connection pool exhausted, no free conn exists"）此前会被 Contains
// 文本匹配误判成「已存在，复用」，NewBloomFilter 返回 nil error，调用方拿到
// 一个打不进去的空过滤器句柄，真实失败被推远到 BF.ADD 才爆且根因不可见。
// 修复后：BF.INFO 探测同样失败 ⇒ RESERVE 的失败另有原因，原始错误上抛不吞。
func TestBloomFilter_ErrorContainingExists_NotSwallowed(t *testing.T) {
	c, mr := newTestClient(t, "")
	ctx := context.Background()

	// miniredis 不支持 BF.* 模块命令，经 SetError 让每条命令稳定返回带
	// "exists" 字样的连接级错误消息——旧实现对该消息必吞（Contains 命中）。
	mr.SetError("connection pool exhausted, no free conn exists")

	bf, err := c.NewBloomFilter(ctx, "bf:trap", 0.01, 1000)
	if err == nil {
		t.Fatal("含 exists 的连接级错误不得被吞（#112）：NewBloomFilter 必须返回错误")
	}
	if bf != nil {
		t.Fatal("错误路径不得返回过滤器句柄")
	}
	if !strings.Contains(err.Error(), "connection pool exhausted") {
		t.Fatalf("返回错误必须保留 RESERVE 原始错误，got: %v", err)
	}
}

// TestBloomFilter_ModuleMissing_ErrorPropagates 表驱动补第二形态：错误不含
// "exists"（模块缺失的 unknown command）同样必须上抛——锁住「删除文本匹配」
// 不退化成「只吞特定文案」。miniredis 对 BF.RESERVE/BF.INFO 均回 unknown
// command ⇒ 探测失败 ⇒ 原始错误包装上抛（真 Redis 无 RedisBloom 模块同形态）。
func TestBloomFilter_ModuleMissing_ErrorPropagates(t *testing.T) {
	c, _ := newTestClient(t, "")
	ctx := context.Background()

	bf, err := c.NewBloomFilter(ctx, "bf:nomodule", 0.01, 1000)
	if err == nil {
		t.Fatal("BF.RESERVE 失败（模块缺失）必须返回错误")
	}
	if bf != nil {
		t.Fatal("错误路径不得返回过滤器句柄")
	}
	if !strings.Contains(err.Error(), "create bloom filter") {
		t.Fatalf("错误必须带 create bloom filter 包装，got: %v", err)
	}
}
