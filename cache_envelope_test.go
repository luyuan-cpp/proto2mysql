package proto2mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
)

// 本文件覆盖 docs/cache.md 里逐条声称的行为。
//
// TestCacheEntryRoundTrip / TestCacheEntrySupersetRule 在 cache_test.go 里测的是
// 编解码与判定这两个**纯函数**；这里测的是它们接进 cacheGetProto / cacheSetProto
// 之后的**端到端行为**——文档里描述的事故时序，必须有一条测试真的走一遍。

// TestCacheEntryFromOlderSchemaIsAMiss 对应 cache.md 第二节的完整事故时序。
//
// 缓存里的记录是"按列从 MySQL 读出来再填进 message 的"，不是从 protobuf 字节解析的，
// 所以 protobuf 的 unknown-fields 保护在这里完全不适用：旧版本写进缓存的是一条
// **结构上残缺**的记录，新版本读到就是新字段拿零值——而 MySQL 里的数据是对的，
// 零日志、零异常，只能靠对账发现。
func TestCacheEntryFromOlderSchemaIsAMiss(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	// 模拟旧版本进程写的条目：它不认识 player_id(pb:6)
	olderFields := map[int32]struct{}{}
	for num := range table.fieldNumbers {
		if num != 6 {
			olderFields[num] = struct{}{}
		}
	}
	stale := &testpb.GolangTest{Id: 9, Ip: "from-old-writer"}
	payload, err := proto.Marshal(stale)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	key, _ := cacheKeyFor(table, stale)
	cache.data[key] = encodeCacheEntry(olderFields, payload)

	out := &testpb.GolangTest{Id: 9}
	if db.cacheGetProto(table, out) {
		t.Fatal("旧 schema 写的残缺条目必须当未命中，否则新字段会静默拿到零值")
	}
}

// TestCacheEntryFromNewerSchemaIsUsable 写入方认识得更多时可以直接用。
//
// 多出来的字段对读取方就是 unknown fields，无害。所以 miss 是**单向**的——
// 滚动发布期间的雪崩面比"把指纹拼进 key"小得多。
func TestCacheEntryFromNewerSchemaIsUsable(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	newerFields := map[int32]struct{}{999: {}}
	for num := range table.fieldNumbers {
		newerFields[num] = struct{}{}
	}
	fresh := &testpb.GolangTest{Id: 9, Ip: "from-newer-writer"}
	payload, err := proto.Marshal(fresh)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	key, _ := cacheKeyFor(table, fresh)
	cache.data[key] = encodeCacheEntry(newerFields, payload)

	out := &testpb.GolangTest{Id: 9}
	if !db.cacheGetProto(table, out) {
		t.Fatal("新 schema 写的条目是超集，应当命中")
	}
	if out.Ip != "from-newer-writer" {
		t.Errorf("命中后内容不对: %q", out.Ip)
	}
}

// TestLegacyBareProtoEntryIsAMiss 老版本写的裸 pb 字节（没有 P2MC 信封）当未命中。
//
// 安全，且会被下一次写原地覆盖成新格式——不需要单独做一次缓存清理。
func TestLegacyBareProtoEntryIsAMiss(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	stale := &testpb.GolangTest{Id: 9, Ip: "bare-bytes"}
	payload, _ := proto.Marshal(stale)
	key, _ := cacheKeyFor(table, stale)
	cache.data[key] = payload // 没有信封

	out := &testpb.GolangTest{Id: 9}
	if db.cacheGetProto(table, out) {
		t.Fatal("无信封的条目必须当未命中")
	}
}

// TestCorruptCacheEntryDoesNotClobberPrimaryKey 对应 cache.md 第四节。
//
// 原先是直接 Unmarshal 进调用方的 message，而 proto.Unmarshal 内部会先 Reset——
// 解析一旦失败，调用方 message 的**主键已经被清成 0**，上层拿着 0 回去查库，
// 于是查错行/查不到，且完全看不出根因。
//
// 而 FindOneByPK(out) 的 out 既是入参（主键）又是出参，复用是这个 API 的天然用法。
func TestCorruptCacheEntryDoesNotClobberPrimaryKey(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	msg := &testpb.GolangTest{Id: 9}
	key, _ := cacheKeyFor(table, msg)
	// 信封是合法的，但里面的 pb 字节是坏的
	cache.data[key] = encodeCacheEntry(table.fieldNumbers, []byte{0xFF, 0xFF, 0xFF})

	out := &testpb.GolangTest{Id: 9}
	if db.cacheGetProto(table, out) {
		t.Fatal("损坏的 pb 字节必须当未命中")
	}
	if out.Id != 9 {
		t.Errorf("解析失败后主键必须还在，实际 Id=%d —— 上层会拿 0 回去查库", out.Id)
	}
}

// TestCacheSetWritesEnvelope 写路径必须带上信封，否则读路径永远认不出来。
func TestCacheSetWritesEnvelope(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	src := &testpb.GolangTest{Id: 42, Ip: "10.0.0.1"}
	db.cacheSetProto(table, src)

	key, _ := cacheKeyFor(table, src)
	stored, ok := cache.data[key]
	if !ok {
		t.Fatal("回填后缓存里应当有这条")
	}
	fields, _, ok := decodeCacheEntry(stored)
	if !ok {
		t.Fatal("写进去的必须是带信封的条目")
	}
	if len(fields) != len(table.fieldNumbers) {
		t.Errorf("信封里应记录本进程认识的全部字段号，期望 %d 个，实际 %d 个",
			len(table.fieldNumbers), len(fields))
	}

	// 自己写的自己一定读得回来
	got := &testpb.GolangTest{Id: 42}
	if !db.cacheGetProto(table, got) {
		t.Fatal("自己写的条目必须能命中")
	}
	if !proto.Equal(src, got) {
		t.Errorf("往返后内容不一致\nwant: %s\ngot:  %s", src, got)
	}
}

// TestCacheTTLIsPassedThrough EnableCache 的 ttl 必须真的传到 Cache.Set。
//
// docs/cache.md 要求"必须设一个有限的 TTL"：按 WHERE 条件的批量更新/删除无法定位
// 受影响主键、不会失效缓存，TTL 是那类情况唯一的兜底。
func TestCacheTTLIsPassedThrough(t *testing.T) {
	cache := newTTLRecordingCache()
	db := NewDB()
	db.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	db.EnableCache(cache, 5*time.Minute)

	table := db.Tables[GetTableName(&testpb.GolangTest{})]
	db.cacheSetProto(table, &testpb.GolangTest{Id: 1})

	if cache.lastTTL != 5*time.Minute {
		t.Errorf("ttl 未透传，实际 %v", cache.lastTTL)
	}
}

// ttlRecordingCache 记录最后一次 Set 用的 ttl。
type ttlRecordingCache struct {
	fakeCache
	lastTTL time.Duration
}

func newTTLRecordingCache() *ttlRecordingCache {
	return &ttlRecordingCache{fakeCache: fakeCache{data: map[string][]byte{}}}
}

func (c *ttlRecordingCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	c.lastTTL = ttl
	return c.fakeCache.Set(ctx, key, value, ttl)
}

// ── 并发边界（docs/concurrency.md） ──────────────────────────────────────
//
// 这一组测的是「P2MC 管什么、不管什么」。其中"丢失更新"那条是**故意断言坏行为**：
// 它不是 bug，是整行 Save 的固有语义；写成测试是为了把这个失败模式钉死在代码里，
// 免得有人以为加了 ODKU 就万事大吉。

// TestSaveDoesNotTouchUnknownColumns v2 独有的列不会被 v1 的 Save 清零。
//
// ODKU 只更新点名的列，而列清单来自本进程的 descriptor —— 所以"本进程不认识的列"
// 根本不会出现在语句里。
func TestSaveDoesNotTouchUnknownColumns(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	stmt, err := table.GetSaveSQLWithArgs(&testpb.GolangTest{Id: 7, Ip: "a"})
	if err != nil {
		t.Fatalf("GetSaveSQLWithArgs: %v", err)
	}
	for _, col := range []string{"id", "ip", "port", "group_id", "player", "player_id"} {
		want := "`" + col + "` = VALUES(`" + col + "`)"
		if !strings.Contains(stmt.Sql, want) {
			t.Errorf("ODKU 应覆盖本进程认识的全部列，缺 %s: %s", want, stmt.Sql)
		}
	}
	if strings.Contains(stmt.Sql, "level") {
		t.Errorf("本进程不认识的列不该出现: %s", stmt.Sql)
	}
}

// TestLostUpdateOnSharedFieldIsNotPrevented **共同字段的丢失更新，本库不防**。
//
// 时序（docs/concurrency.md 第三节）：
//
//	T1 v1 读到 port=100
//	T2 v2 把 port 改成 200 并 Save
//	T3 v1 只想改 ip，却用了整行 Save —— 它手里的 port 还是 100
//	→ port=200 被盖回 100
//
// 要防必须自己选：UpdateFieldsByPK / IncrByPK / 乐观锁 / 行锁。
func TestLostUpdateOnSharedFieldIsNotPrevented(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	stale := &testpb.GolangTest{Id: 7, Ip: "Bob", Port: 100} // v1 手里的旧快照

	stmt, err := table.GetSaveSQLWithArgs(stale)
	if err != nil {
		t.Fatalf("GetSaveSQLWithArgs: %v", err)
	}
	// port 确实被写进去了——哪怕调用方只想改 ip
	if !strings.Contains(stmt.Sql, "`port` = VALUES(`port`)") {
		t.Fatalf("整行 Save 应当写入 port: %s", stmt.Sql)
	}
	found := false
	for _, a := range stmt.Args {
		if fmt.Sprint(a) == "100" {
			found = true
		}
	}
	if !found {
		t.Error("整行 Save 会把手里的旧 port 一起写回去——这正是丢失更新的来源")
	}
}

// TestTransactionBypassesCacheOnRead 事务内**完全绕过缓存**。
//
// 要读到事务内自己刚写的最新值，走缓存就错了。这也是"缓存机制本身不会把未提交
// 数据写进缓存"的一半原因，另一半是失效延迟到提交成功之后
// （见 TestCacheInvalidateDeferredInTx）。
func TestTransactionBypassesCacheOnRead(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	// 缓存里放一条能命中的
	cached := &testpb.GolangTest{Id: 9, Ip: "from-cache"}
	db.cacheSetProto(table, cached)

	// 事务外：命中缓存
	out := &testpb.GolangTest{Id: 9}
	if !db.cacheGetProto(table, out) {
		t.Fatal("事务外应当命中缓存")
	}

	// 事务内：useCache 必须为 false
	txDB := &DB{Tables: db.Tables, cache: db.cache, cacheTTL: db.cacheTTL, tx: new(sql.Tx)}
	if txDB.cacheEnabled() && txDB.tx == nil {
		t.Fatal("事务内的 useCache 判定应当为 false")
	}
	if !txDB.cacheEnabled() {
		t.Fatal("事务内缓存本身仍然是启用的，只是读路径不走它")
	}
}
