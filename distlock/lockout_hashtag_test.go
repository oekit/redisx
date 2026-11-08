package distlock

import (
	"context"
	"testing"
	"time"
)

// crc16XMODEM 复刻 Redis Cluster 的 CRC16-XMODEM（CCITT 逆序变体，
// REDIS_CLUSTER_KEYSLOT 用的就是它）。
func crc16XMODEM(buf string) uint16 {
	crc := uint16(0)
	for i := 0; i < len(buf); i++ {
		crc ^= uint16(buf[i]) << 8
		for bit := 0; bit < 8; bit++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc = crc << 1
			}
		}
	}
	return crc
}

// clusterSlot 按官方 hash-tag 规则求 slot：取完整 key 里**首个** '{' 与其后
// 首个 '}' 之间的内容（不存在成对花括号则用完整 key），对 CRC16 取模 16384。
// 与 issue #119 的实测（lockout:user:42 → 5580 等三个键两两不同槽）同一算法。
func clusterSlot(key string) uint16 {
	if start := indexOfByte(key, '{'); start >= 0 {
		if end := indexOfByte(key[start+1:], '}'); end >= 0 {
			return crc16XMODEM(key[start+1:start+1+end]) % 16384
		}
	}
	return crc16XMODEM(key) % 16384
}

func indexOfByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// TestLockout_KeysShareClusterSlot 回归（#119）：Lockout 三键（失败窗口/
// 锁定标记/事件序号）由单个 Lua 脚本同时访问（KEYS[1..3]），无 hash-tag 时
// 三键按整 key 的 CRC16 两两不同槽——Redis Cluster 上 EVAL 按首键路由，
// 节点执行脚本访问异槽键即报 "Lua script attempted to access a non local
// key"，Fail() 每次失败，防爆破锁整体失效。修复后三键共享共同段
// {prefix+key} 的 hash-tag，必须同 slot。
func TestLockout_KeysShareClusterSlot(t *testing.T) {
	cases := []struct {
		prefix string
		key    string
	}{
		{"zeus:lockout:", "user:42"},
		{"zeus:lockout:", "k"},
		{"lockout:", "user:42"},       // 缺省前缀
		{"{app}:lockout:", "user:42"}, // KeyPrefix 自带 hash-tag：首个 {...} 仍三键一致
		{"a:b:c:", "X"},               // 无花括号干扰的普通段
	}
	for _, c := range cases {
		l := NewLockout(nil, LockoutConfig{
			KeyPrefix: c.prefix,
			Threshold: 1, Window: time.Minute, Penalty: time.Minute,
		})
		keys := l.keys(c.key)
		if len(keys) != 3 {
			t.Fatalf("%q%q: got %d keys, want 3", c.prefix, c.key, len(keys))
		}
		want := clusterSlot(keys[0])
		for i, k := range keys {
			if got := clusterSlot(k); got != want {
				t.Fatalf("%q%q: key #%d %q slot=%d differs from first key %q slot=%d (#119)",
					c.prefix, c.key, i, k, got, keys[0], want)
			}
		}
		// 钉住键名形态：共同段包 {...}，lock/seq 后缀在 tag 外。
		tagged := "{" + c.prefix + c.key + "}"
		if keys[0] != tagged || keys[1] != tagged+":lock" || keys[2] != tagged+":seq" {
			t.Fatalf("key layout changed: got %v, want [%s %s:lock %s:seq]", keys, tagged, tagged, tagged)
		}
	}
}

// TestLockout_TripleSlotMismatchPin 证明修复前三键确实异槽（防回归锚点的
// 有效性）：用无 tag 的旧键名形态计算 slot，三个值必须两两不同——否则本
// 文件的 slot 断言形同虚设。取 issue 实测键 lockout:user:42。
func TestLockout_TripleSlotMismatchPin(t *testing.T) {
	base := "lockout:user:42"
	old := []string{base, base + ":lock", base + ":seq"}
	seen := map[uint16]string{}
	for _, k := range old {
		s := clusterSlot(k)
		if prev, dup := seen[s]; dup {
			t.Fatalf("old untagged keys unexpectedly share slot %d (%q vs %q) — pin premise broken", s, prev, k)
		}
		seen[s] = k
	}
}

// TestLockout_HashtagKeysFunctional 新键名下的功能不回归：Fail 计数/锁定/
// Reset 清零走 {tagged}:lock/:seq 三键仍然成立（miniredis 单机语义）。
func TestLockout_HashtagKeysFunctional(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := &lockoutClock{nowFn: func() time.Time { return now }}
	l, mr := newLockoutTest(t, clock, nil)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := l.Fail(ctx, "user:42"); err != nil {
			t.Fatalf("fail %d: %v", i+1, err)
		}
	}
	locked, _, err := l.Check(ctx, "user:42")
	if err != nil || !locked {
		t.Fatalf("Check = (%v, %v), want locked", locked, err)
	}
	// 三键物理形态钉住：{zeus:lockout:user:42} / :lock / :seq
	tagged := "{zeus:lockout:user:42}"
	for _, k := range []string{tagged + ":lock", tagged + ":seq"} {
		if !mr.Exists(k) {
			t.Fatalf("expected key %q to exist, keys: %v", k, mr.Keys())
		}
	}
	if !mr.Exists(tagged) {
		t.Fatalf("expected window zset %q to exist, keys: %v", tagged, mr.Keys())
	}
	if err := l.Reset(ctx, "user:42"); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if locked, _, _ := l.Check(ctx, "user:42"); locked {
		t.Fatal("after Reset must be unlocked")
	}
	for _, k := range []string{tagged, tagged + ":lock", tagged + ":seq"} {
		if mr.Exists(k) {
			t.Fatalf("key %q must be deleted by Reset", k)
		}
	}
}
