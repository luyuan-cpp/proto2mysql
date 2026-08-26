package proto2mysql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// ErrCacheMiss 缓存未命中时Cache.Get应返回的错误
var ErrCacheMiss = errors.New("cache miss")

// Cache 缓存抽象（cache-aside模式）。库不直接依赖具体Redis客户端，
// 由业务方注入实现（如go-redis适配器），保持弱依赖：
// 任何缓存错误都不会影响数据库操作，只会降级为直读DB。
//
// go-redis适配示例：
//
//	type RedisCache struct{ C *redis.Client }
//	func (r *RedisCache) Get(ctx context.Context, key string) ([]byte, error) {
//	    b, err := r.C.Get(ctx, key).Bytes()
//	    if errors.Is(err, redis.Nil) { return nil, proto2mysql.ErrCacheMiss }
//	    return b, err
//	}
//	func (r *RedisCache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
//	    return r.C.Set(ctx, key, val, ttl).Err()
//	}
//	func (r *RedisCache) Del(ctx context.Context, keys ...string) error {
//	    return r.C.Del(ctx, keys...).Err()
//	}
type Cache interface {
	// Get 返回缓存值；未命中必须返回ErrCacheMiss
	Get(ctx context.Context, key string) ([]byte, error)
	// Set 写入缓存，ttl<=0表示不过期
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Del 删除一个或多个key
	Del(ctx context.Context, keys ...string) error
}

// EnableCache 启用cache-aside缓存（按主键的单行读写生效）。
// 语义：
//   - 读（FindOneByPK/FindOrCreate命中路径）：先查缓存，未命中读DB后回填；
//   - 写（Save/Update/UpdateFieldsByPK/UpdateKVByPK/UpdateIfVersion/Delete/IncrByPK等
//     按主键操作）：先写DB，成功后删缓存；
//   - 事务（RunInTransaction）：删缓存延迟到提交成功之后，避免回滚后缓存脏删；
//   - 降级：缓存Get/Set/Del出错仅记录日志，不影响DB结果（Redis弱依赖）。
//
// 注意：按WHERE条件的更新/删除（UpdateByWhereWithArgs/DeleteByWhereWithArgs/DeleteByKV）
// 无法定位受影响主键，不做缓存失效；缓存表请优先使用按主键的接口，
// 或调用InvalidateCache手动失效。
func (p *DB) EnableCache(cache Cache, ttl time.Duration) {
	p.cache = cache
	p.cacheTTL = ttl
}

// CacheKey 返回message对应的缓存key（pb:<表名>:<主键值...>），供业务方手动操作缓存
func (p *DB) CacheKey(message proto.Message) (string, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return "", err
	}
	return cacheKeyFor(table, message)
}

// InvalidateCache 手动删除一批消息对应的缓存（按WHERE批量写后可调用）。
//
// **事务内调用会延迟到提交成功之后**，与 invalidateMessages 同一套语义。原先这里
// 无条件立即 Del：在 RunInTransaction 里先删缓存、事务再回滚的话，删掉的是一条
// 本该继续有效的条目，下一次读会把**未回滚前的库值**重新灌回缓存——写路径特意
// 做的"先写库后删缓存 + 回滚不删"在这条手动入口上整个被绕过。
func (p *DB) InvalidateCache(messages ...proto.Message) error {
	if !p.cacheEnabled() || len(messages) == 0 {
		return nil
	}

	keys := make([]string, 0, len(messages))
	for _, msg := range messages {
		key, err := p.CacheKey(msg)
		if err != nil {
			return err
		}
		keys = append(keys, key)
	}
	if p.tx != nil {
		owner := p.transactionCacheOwner()
		owner.pendingCacheDels = append(owner.pendingCacheDels, keys...)
		return nil
	}
	return p.cache.Del(p.context(), keys...)
}

func (p *DB) cacheEnabled() bool {
	return p.cache != nil
}

// cacheKeyFor 生成缓存key：pb:<表名>:<主键值1>:<主键值2>...
//
// ⚠️ **schema 版本刻意不进 key，而是进 value 的头部**（见 encodeCacheEntry）。
//
// 看起来把指纹拼进 key 更省事——新旧版本天然不共享条目、投毒路径直接消失。
// 但失效路径与读路径共用本函数：v1 写库后去删的是**自己那个指纹**的 key，
// v2 缓存的那条永远没人删，于是「读到残缺数据」被升级成「跨版本永久脏读」，
// 一直脏到 TTL 到期。比原来的问题更糟。
//
// 放进 value 头部则：key 不变 → 失效跨版本照常生效；读时做超集判定 →
// 只有「认识更多字段的一方读到认识更少的一方写的条目」才 miss（单向），
// 雪崩面小，而且是原地覆写，不会把旧 key 空间搁浅占内存。
//
// ⚠️ 复合主键的分量必须**转义分隔符**再拼，否则两个不同的主键会得到同一个 key：
//
//	("x:y", "z")  →  pb:t:x:y:z
//	("x", "y:z")  →  pb:t:x:y:z     ← 同一个 key
//
// 命中时返回的是另一行的**整条 protobuf**，而且两边互相投毒——这是静默的跨行脏读，
// 库里数据自始至终是对的，零日志零异常。字符串主键（订单号、外部账号 ID、
// 复合业务键）里带冒号一点都不罕见。
//
// 表名与每个主键分量都使用最小侵入的百分号编码：只动 '%' 和 ':' 两个字符，其余原样。
// 于是不含这两个字符的表名/主键（整数、绝大多数字符串）**产出的 key 与旧版逐字节相同**，
// 升级不会让存量缓存整体失效；含分隔符的值必须让共用 Redis 的其它语言实现同步编码，
// 只有原本就在碰撞的那些值才会换 key，而那正是我们要它们分开。注意这部分 key 在
// 新旧进程混部时无法互相失效：涉及 '%' / ':' 的部署应在升级窗口禁用或 flush 缓存，
// 并同步切换所有语言实现，不能依赖自然滚动维持缓存一致性。
func cacheKeyFor(table *MessageTable, message proto.Message) (string, error) {
	values, err := table.primaryKeySerializedValues(message)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("pb:")
	// 表名与主键分量共用 ':' 分隔符，必须同样编码；否则表 "a:1" 的
	// 单主键 (2) 会与表 "a" 的复合主键 (1,2) 生成同一个 key。
	b.WriteString(escapeCacheKeyPart(table.tableName))
	for _, v := range values {
		b.WriteString(":")
		b.WriteString(escapeCacheKeyPart(fmt.Sprint(v)))
	}
	return b.String(), nil
}

// escapeCacheKeyPart 把表名/主键分量里的分隔符转义掉：'%' → "%25"、':' → "%3A"。
// 不含这两个字符时原样返回（连拷贝都不做），保证绝大多数 key 与旧版逐字节一致。
func escapeCacheKeyPart(s string) string {
	if !strings.ContainsAny(s, "%:") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '%':
			b.WriteString("%25")
		case ':':
			b.WriteString("%3A")
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// ── 缓存条目的信封 ──────────────────────────────────────────────────────
//
// 存进缓存的**不是**裸 pb 字节，而是「写入方认识哪些字段」+ pb 字节。
//
// 为什么必须记这个：缓存里的 message 是**按列从 MySQL 读出来再填进去的**，不是从
// pb 字节 Unmarshal 的——所以 protobuf 的 unknown-fields 保留机制在这里完全不适用。
// 旧版本进程的 message 里根本没有新字段，它写进缓存的是一条**残缺**记录。滚动发布时
// v1 写、v2 读，v2 就拿到新字段的零值，而 MySQL 里是有值的；v2 写完只 Del key，
// v1 下次读又投毒一次，表现为**读到的值随机闪烁**。最阴的是 MySQL 里的数据自始至终
// 是对的，零日志零异常，只能靠对账发现。
//
// 判定规则是**超集**：写入方的字段集 ⊇ 读取方的字段集时才采用。
//   - v1 读 v2 写的条目 → v2 认识得更多，采用（多出来的字段对 v1 就是 unknown fields）
//   - v2 读 v1 写的条目 → v1 认识得更少，**当未命中回源**
//
// 所以 miss 是单向的，滚动发布期间的雪崩面比"换 key"小得多。
//
// 布局（与 Python 版逐字节一致，可共用同一个 Redis）：
//
//	magic   4B   "P2MC"
//	version 1B   0x01
//	count   varint          字段号个数
//	fields  count 个 varint  字段号，**升序**
//	payload 剩余全部          pb 序列化字节
//
// 老条目（裸 pb 字节，没有 magic）读到就当未命中——安全，且会被下一次写覆盖。
const (
	cacheEntryVersion       = 1
	maxCacheEntryFieldCount = 1 << 16
)

var cacheEntryMagic = []byte("P2MC")

// CacheCleanupTimeout 是数据库写成功后的缓存失效预算。该清理不能继承已经取消的请求
// context（否则写已提交、旧缓存却没删），也不能无限阻塞返回路径。
const CacheCleanupTimeout = 2 * time.Second

// encodeCacheEntry 把「写入方认识的字段号集合」和 pb 字节打包成一条缓存条目。
func encodeCacheEntry(fieldNumbers map[int32]struct{}, payload []byte) []byte {
	nums := make([]int, 0, len(fieldNumbers))
	for num := range fieldNumbers {
		nums = append(nums, int(num))
	}
	// 必须排序：同一个集合要产出同一份字节，否则每次写入的内容都不同。
	slices.Sort(nums)

	out := make([]byte, 0, len(cacheEntryMagic)+1+binary.MaxVarintLen64*(len(nums)+1)+len(payload))
	out = append(out, cacheEntryMagic...)
	out = append(out, cacheEntryVersion)
	out = binary.AppendUvarint(out, uint64(len(nums)))
	for _, num := range nums {
		out = binary.AppendUvarint(out, uint64(num))
	}
	return append(out, payload...)
}

// decodeCacheEntry 拆包；不是本库写的条目（无 magic / 版本不认 / 截断）返回 ok=false。
func decodeCacheEntry(data []byte) (fields map[int32]struct{}, payload []byte, ok bool) {
	if !bytes.HasPrefix(data, cacheEntryMagic) {
		return nil, nil, false
	}
	pos := len(cacheEntryMagic)
	if pos >= len(data) || data[pos] != cacheEntryVersion {
		return nil, nil, false
	}
	pos++

	count, n := binary.Uvarint(data[pos:])
	if n <= 0 {
		return nil, nil, false
	}
	pos += n
	// count 来自外部缓存，不能在验证前直接拿去做 make 的容量提示。
	// 每个字段号至少占 1 字节，所以 count 大于剩余字节数一定是截断/恶意输入；
	// 另设一个远高于 MySQL 单表字段上限的硬阈值，避免大条目把 map 放大到无界。
	if count > maxCacheEntryFieldCount || count > uint64(len(data)-pos) {
		return nil, nil, false
	}

	fields = make(map[int32]struct{}, int(count))
	var previous uint64
	for i := uint64(0); i < count; i++ {
		num, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return nil, nil, false
		}
		pos += n
		// protobuf 字段号从 1 开始且有明确上限。信封格式还规定字段号升序；
		// 严格递增同时排除重复，避免 uint64 -> int32 截断或非规范集合绕过超集判定。
		if num == 0 || num > uint64(protowire.MaxValidNumber) || (i > 0 && num <= previous) {
			return nil, nil, false
		}
		previous = num
		fields[int32(num)] = struct{}{}
	}
	return fields, data[pos:], true
}

// missingFieldNumbers 返回 want 里而 have 里没有的字段号（升序），空则表示 have ⊇ want。
func missingFieldNumbers(have, want map[int32]struct{}) []int {
	var missing []int
	for num := range want {
		if _, ok := have[num]; !ok {
			missing = append(missing, int(num))
		}
	}
	slices.Sort(missing)
	return missing
}

// cacheGetProto 读缓存并反序列化到message；返回是否命中。任何错误都视为未命中（降级）。
func (p *DB) cacheGetProto(table *MessageTable, message proto.Message) bool {
	key, err := cacheKeyFor(table, message)
	if err != nil {
		return false
	}

	data, err := p.cache.Get(p.context(), key)
	if err != nil {
		if !errors.Is(err, ErrCacheMiss) {
			log.Printf("proto2mysql: cache get %s failed (fallback to db): %v", key, err)
		}
		return false
	}

	writerFields, payload, ok := decodeCacheEntry(data)
	if !ok {
		// 没有信封：要么是老版本写的裸 pb 字节，要么根本不是本库写的。
		// 一律当未命中回源——下一次写会把它原地覆盖成带信封的条目。
		log.Printf("proto2mysql: cache entry %s has no envelope (fallback to db)", key)
		return false
	}
	if missing := missingFieldNumbers(writerFields, table.fieldNumbers); len(missing) > 0 {
		// 写入方认识的字段比我少，这条记录对我来说是**残缺**的。直接用会让我新增的
		// 那些字段静默拿到零值，而库里其实是有值的。
		log.Printf("proto2mysql: cache entry %s was written by an older schema "+
			"(missing pb:%v); falling back to db", key, missing)
		return false
	}

	// 解析到临时对象再拷回去：原先是直接 Unmarshal 进调用方的 message，
	// 而 proto.Unmarshal 会先 Reset —— 解析一旦失败，调用方 message 的**主键已经被清成 0**，
	// 上层拿着 0 回去查库，于是查错行/查不到，且看不出根因。
	scratch := message.ProtoReflect().New().Interface()
	if err := proto.Unmarshal(payload, scratch); err != nil {
		log.Printf("proto2mysql: cache unmarshal %s failed (fallback to db): %v", key, err)
		return false
	}
	proto.Reset(message)
	proto.Merge(message, scratch)
	return true
}

// cacheSetProto 序列化message并回填缓存（尽力而为，失败仅记日志）
func (p *DB) cacheSetProto(table *MessageTable, message proto.Message) {
	key, err := cacheKeyFor(table, message)
	if err != nil {
		return
	}

	data, err := proto.Marshal(message)
	if err != nil {
		log.Printf("proto2mysql: cache marshal %s failed: %v", key, err)
		return
	}
	data = encodeCacheEntry(table.fieldNumbers, data)
	if err := p.cache.Set(p.context(), key, data, p.cacheTTL); err != nil {
		log.Printf("proto2mysql: cache set %s failed: %v", key, err)
	}
}

// cacheDelKeys 删除缓存key（尽力而为，失败仅记日志——存在短暂脏读风险，靠TTL兜底）
func (p *DB) cacheDelKeys(keys ...string) {
	if !p.cacheEnabled() || len(keys) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(p.context()), CacheCleanupTimeout)
	defer cancel()
	if err := p.cache.Del(ctx, keys...); err != nil {
		log.Printf("proto2mysql: cache del %v failed (stale until ttl): %v", keys, err)
	}
}

// invalidateMessages 写DB成功后失效缓存：
// 事务内先暂存key，提交成功后统一删除；非事务立即删除。
func (p *DB) invalidateMessages(table *MessageTable, messages ...proto.Message) {
	if !p.cacheEnabled() {
		return
	}

	keys := make([]string, 0, len(messages))
	for _, msg := range messages {
		key, err := cacheKeyFor(table, msg)
		if err != nil {
			continue // 无主键的表不参与缓存
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return
	}

	if p.tx != nil {
		owner := p.transactionCacheOwner()
		owner.pendingCacheDels = append(owner.pendingCacheDels, keys...)
		return
	}
	p.cacheDelKeys(keys...)
}
