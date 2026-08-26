package proto2mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
)

// fakeCache 内存缓存实现，可注入错误模拟Redis故障
type fakeCache struct {
	mu        sync.Mutex
	data      map[string][]byte
	getErr    error
	setErr    error
	delErr    error
	deleted   []string
	delCtxErr error
}

func newFakeCache() *fakeCache {
	return &fakeCache{data: map[string][]byte{}}
}

func (f *fakeCache) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	if v, ok := f.data[key]; ok {
		return v, nil
	}
	return nil, ErrCacheMiss
}

func (f *fakeCache) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key] = value
	return nil
}

func (f *fakeCache) Del(ctx context.Context, keys ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delCtxErr = ctx.Err()
	if f.delErr != nil {
		return f.delErr
	}
	for _, k := range keys {
		delete(f.data, k)
		f.deleted = append(f.deleted, k)
	}
	return nil
}

func newCacheTestDB(cache Cache) *DB {
	db := NewDB()
	db.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	if cache != nil {
		db.EnableCache(cache, time.Minute)
	}
	return db
}

// TestCacheKeyFormat 缓存key为 pb:<表名>:<主键值...>
//
// schema 版本刻意**不进 key**：失效路径与读路径共用 cacheKeyFor，指纹进 key 会让
// v1 写库后删的是自己那个指纹的 key、v2 缓存的那条永远没人删，把"读到残缺数据"
// 升级成"跨版本永久脏读"。版本信息在 value 头部（见 encodeCacheEntry）。
func TestCacheKeyFormat(t *testing.T) {
	db := newCacheTestDB(newFakeCache())

	key, err := db.CacheKey(&testpb.GolangTest{Id: 42})
	if err != nil {
		t.Fatalf("CacheKey失败: %v", err)
	}
	want := "pb:" + GetTableName(&testpb.GolangTest{}) + ":42"
	if key != want {
		t.Errorf("key = %q, 预期 %q", key, want)
	}

	// 复合主键：所有主键值依次拼接
	db2 := NewDB()
	db2.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id", "group_id"))
	db2.EnableCache(newFakeCache(), time.Minute)
	key2, err := db2.CacheKey(&testpb.GolangTest{Id: 1, GroupId: 2})
	if err != nil {
		t.Fatalf("CacheKey失败: %v", err)
	}
	want2 := "pb:" + GetTableName(&testpb.GolangTest{}) + ":1:2"
	if key2 != want2 {
		t.Errorf("key = %q, 预期 %q", key2, want2)
	}
}

// TestCacheEntryRoundTrip 信封编解码；与 Python 版必须逐字节一致（可能共用同一个 Redis）。
func TestCacheEntryRoundTrip(t *testing.T) {
	payload := []byte{0x08, 0x2a}
	set := map[int32]struct{}{3: {}, 1: {}, 2: {}}
	blob := encodeCacheEntry(set, payload)

	if !bytes.HasPrefix(blob, append(append([]byte{}, cacheEntryMagic...), cacheEntryVersion)) {
		t.Fatalf("信封头不对: %x", blob)
	}
	fields, out, ok := decodeCacheEntry(blob)
	if !ok {
		t.Fatal("decodeCacheEntry 应当成功")
	}
	if len(fields) != 3 || !bytes.Equal(out, payload) {
		t.Errorf("拆包结果不对: fields=%v payload=%x", fields, out)
	}
	// 字段号必须升序写入，保证同一集合产出同一份字节
	same := encodeCacheEntry(map[int32]struct{}{1: {}, 2: {}, 3: {}}, payload)
	if !bytes.Equal(blob, same) {
		t.Errorf("同一集合产出的字节必须相同: %x vs %x", blob, same)
	}
	if _, _, ok := decodeCacheEntry([]byte("not-an-envelope")); ok {
		t.Error("无 magic 的条目必须判为非本库写入")
	}
	if _, _, ok := decodeCacheEntry(append(append([]byte{}, cacheEntryMagic...), 0x99)); ok {
		t.Error("版本不认的条目必须判为非本库写入")
	}
}

// TestCacheEntrySupersetRule 超集判定：写入方字段集 ⊇ 读取方才采用。
//
// 这正是"旧版本把残缺记录写进缓存、新版本读到零值"那条投毒路径的闸门，
// 而且 miss 是单向的——认识更多字段的一方读到旧条目才 miss。
func TestCacheEntrySupersetRule(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{})
	mine := table.fieldNumbers

	older := map[int32]struct{}{}
	for num := range mine {
		if num != 6 { // 旧版本不认识 player_id(pb:6)
			older[num] = struct{}{}
		}
	}
	if missing := missingFieldNumbers(older, mine); len(missing) != 1 || missing[0] != 6 {
		t.Errorf("旧写入方应缺 pb:6，实际 %v", missing)
	}

	newer := map[int32]struct{}{999: {}}
	for num := range mine {
		newer[num] = struct{}{}
	}
	if missing := missingFieldNumbers(newer, mine); len(missing) != 0 {
		t.Errorf("新写入方是超集，不该 miss，实际缺 %v", missing)
	}
}

// TestCacheHitAndBackfill 缓存命中直接反序列化；回填后可再次命中
func TestCacheHitAndBackfill(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	src := &testpb.GolangTest{Id: 42, Ip: "10.0.0.1", Port: 8888}

	// 回填
	db.cacheSetProto(table, src)

	// 命中：只带主键的消息应被填满
	got := &testpb.GolangTest{Id: 42}
	if !db.cacheGetProto(table, got) {
		t.Fatal("预期缓存命中")
	}
	if !proto.Equal(src, got) {
		t.Errorf("缓存数据不一致\nwant: %s\ngot:  %s", src, got)
	}

	// 未命中
	miss := &testpb.GolangTest{Id: 999}
	if db.cacheGetProto(table, miss) {
		t.Error("不存在的key不应命中")
	}
}

// TestCacheDegradation Redis故障时读写全部降级，不影响调用方
func TestCacheDegradation(t *testing.T) {
	cache := newFakeCache()
	cache.getErr = errors.New("redis: connection refused")
	cache.setErr = errors.New("redis: connection refused")
	cache.delErr = errors.New("redis: connection refused")

	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]
	msg := &testpb.GolangTest{Id: 1}

	// Get故障 → 未命中（降级读DB）
	if db.cacheGetProto(table, msg) {
		t.Error("缓存故障时应视为未命中")
	}
	// Set/Del故障 → 不panic不报错
	db.cacheSetProto(table, msg)
	db.invalidateMessages(table, msg)
	if err := db.InvalidateCache(msg); err == nil {
		t.Log("InvalidateCache返回了缓存错误（手动调用可感知），符合预期")
	}
}

// TestCacheCorruptedDataFallback 缓存内容损坏时降级读DB
func TestCacheCorruptedDataFallback(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	msg := &testpb.GolangTest{Id: 42}
	key, _ := cacheKeyFor(table, msg)
	cache.data[key] = []byte{0xFF, 0xFF, 0xFF} // 非法proto数据

	if db.cacheGetProto(table, msg) {
		t.Error("损坏的缓存数据应视为未命中")
	}
}

// TestCacheInvalidateImmediate 非事务写：立即删缓存
func TestCacheInvalidateImmediate(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	msg := &testpb.GolangTest{Id: 42, Port: 1}
	db.cacheSetProto(table, msg)

	db.invalidateMessages(table, msg)

	key, _ := cacheKeyFor(table, msg)
	if _, ok := cache.data[key]; ok {
		t.Error("非事务失效应立即删除缓存")
	}
	if len(db.pendingCacheDels) != 0 {
		t.Error("非事务失效不应暂存key")
	}
}

// TestCacheInvalidateDeferredInTx 事务内写：删缓存延迟暂存（提交后统一执行）
func TestCacheInvalidateDeferredInTx(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]

	msg := &testpb.GolangTest{Id: 42, Port: 1}
	db.cacheSetProto(table, msg)

	// 模拟事务上下文（只需tx非空，不执行真实SQL）
	txDB := &DB{
		Tables:   db.Tables,
		cache:    db.cache,
		cacheTTL: db.cacheTTL,
		tx:       new(sql.Tx),
	}
	txDB.invalidateMessages(table, msg)

	key, _ := cacheKeyFor(table, msg)
	if _, ok := cache.data[key]; !ok {
		t.Error("事务内不应立即删除缓存")
	}
	if len(txDB.pendingCacheDels) != 1 || txDB.pendingCacheDels[0] != key {
		t.Errorf("事务内应暂存待删key，实际: %v", txDB.pendingCacheDels)
	}

	// 模拟提交后清理
	db.cacheDelKeys(txDB.pendingCacheDels...)
	if _, ok := cache.data[key]; ok {
		t.Error("提交后应删除缓存")
	}
}

// TestCacheInvalidateDeferredAcrossWithContext 事务实例派生 WithContext 后仍必须把待删 key
// 记到同一个事务状态；否则提交路径只看原 txDB，派生实例记录的失效会永久漏掉。
func TestCacheInvalidateDeferredAcrossWithContext(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]
	msg := &testpb.GolangTest{Id: 77}

	txDB := &DB{Tables: db.Tables, cache: db.cache, cacheTTL: db.cacheTTL, tx: new(sql.Tx)}
	txDB.txOwner = txDB
	derived := txDB.WithContext(context.Background())
	derived.invalidateMessages(table, msg)

	key, _ := cacheKeyFor(table, msg)
	if len(txDB.pendingCacheDels) != 1 || txDB.pendingCacheDels[0] != key {
		t.Fatalf("WithContext 必须共享事务待删队列，root=%v derived=%v",
			txDB.pendingCacheDels, derived.pendingCacheDels)
	}
}

// TestCommittedWriteCacheCleanupSurvivesRequestCancellation 数据库写成功后，请求 ctx 可能
// 已在返回路上取消；缓存失效仍要用独立、有限时长的 ctx 尝试一次，否则陈旧值留到 TTL。
func TestCommittedWriteCacheCleanupSurvivesRequestCancellation(t *testing.T) {
	cache := newFakeCache()
	db := newCacheTestDB(cache)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db.WithContext(ctx).cacheDelKeys("pb:test:1")
	if cache.delCtxErr != nil {
		t.Fatalf("提交后缓存清理不应继承请求取消，实际 ctx err=%v", cache.delCtxErr)
	}
}

// TestCacheDisabledNoop 未启用缓存时所有缓存操作为空操作
func TestCacheDisabledNoop(t *testing.T) {
	db := newCacheTestDB(nil)
	table := db.Tables[GetTableName(&testpb.GolangTest{})]
	msg := &testpb.GolangTest{Id: 1}

	if db.cacheEnabled() {
		t.Error("未启用缓存时cacheEnabled应为false")
	}
	db.invalidateMessages(table, msg) // 不应panic
	if err := db.InvalidateCache(msg); err != nil {
		t.Errorf("未启用缓存时InvalidateCache应为空操作: %v", err)
	}
}
