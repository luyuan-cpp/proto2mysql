package proto2mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/luyuancpp/proto2mysql/pbconv"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// 常量定义
const (
	BatchInsertMaxSize = 1000 // 批量插入最大条数
	// MySQL关键字列表
	mysqlKeywordPattern = `^(SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|AND|OR|JOIN|ON|IN|NOT|NULL|PRIMARY|KEY|INDEX|UNIQUE|AUTO_INCREMENT|INT|VARCHAR|TEXT|BLOB|DATETIME|TIMESTAMP|FLOAT|DOUBLE|BOOL|TINYINT|BIGINT)$`
)

var (
	// keywordRegex 用于识别与MySQL关键字冲突的标识符
	keywordRegex = regexp.MustCompile(mysqlKeywordPattern)
	// timestampFullName 是google.protobuf.Timestamp的全名，用于字段类型判断
	timestampFullName = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().FullName()

	ErrTableNotFound      = errors.New("table not found")
	ErrNoRepeatedField    = errors.New("message has no repeated field")
	ErrMultipleRepeated   = errors.New("message has multiple repeated fields")
	ErrPrimaryKeyNotFound = errors.New("primary key not found")
	ErrFieldNotFound      = errors.New("field not found in message")
	ErrMultipleRowsFound  = errors.New("multiple rows found")
	ErrNoRowsFound        = errors.New("no rows found")
	ErrDuplicateKey       = errors.New("duplicate key")
	ErrBatchSizeExceeded  = fmt.Errorf("batch size exceeds maximum %d", BatchInsertMaxSize)

	// ErrFieldNumberReused 线上某列的 pb:N 与新字段号相同，但两者类型跨族不可转换。
	//
	// 这不是改名，是**字段号被复用**：proto 里删掉一个字段后，把它的编号让给了一个
	// 类型完全不同的新字段。库按字段号识别列，于是会生成 CHANGE COLUMN legacy foo
	// bigint —— MySQL 的隐式类型转换会把 mediumtext 里的内容整列吃掉，而本库
	// 「永不 DROP COLUMN」的保护在这里帮不上忙。
	//
	// 字段号是 protobuf 的身份，**永不复用**：删字段要用 reserved。
	ErrFieldNumberReused = errors.New("proto field number reused with an incompatible column type")

	// ErrExpandOnlyViolation 开了 ExpandOnly 之后，本次对齐产生了非「纯新增」的变更。
	//
	// 滚动 / 金丝雀发布时必须打开那个开关。本库没有 schema 版本概念，每个进程都把自己的
	// proto 当成唯一正确的目标结构，所以只要新旧两版同时在跑，MODIFY / CHANGE 就会被
	// 两边**来回改**——不需要写任何数据，一次重启就翻一次面。ADD COLUMN 没有这个问题：
	// 旧版本的 SQL 里根本不会出现新列名。
	ErrExpandOnlyViolation = errors.New("schema change is not expand-only")

	// ErrUnsupportedFieldKind proto 字段类型没有 MySQL 列类型映射。
	//
	// sint32 / sint64 / fixed32 / fixed64 / sfixed32 / sfixed64 都不支持——它们的
	// zigzag / 定长编码没有直接对应的 MySQL 列类型。改用 int32 / int64 / uint32 / uint64
	// 即可（取值范围完全一样，只是线上编码不同）。
	//
	// 早先这些类型静默回落成 TEXT：建表一路成功，跑到**第一次写入**才抛错，
	// 而那时列已经建出来了、可能还上了线。现在在生成 DDL 时就 fail-fast。
	ErrUnsupportedFieldKind = errors.New("field kind has no MySQL type mapping")
)

// SqlWithArgs 存储带?占位符的SQL和对应的参数列表
type SqlWithArgs struct {
	Sql  string        // 带占位符的SQL
	Args []interface{} // 与占位符一一对应的参数值
}

// MessageTable 存储Protobuf消息与MySQL表的映射关系及预生成的SQL片段
type MessageTable struct {
	tableName       string
	Descriptor      protoreflect.MessageDescriptor
	primaryKey      []string // 主键字段列表
	primaryKeyField protoreflect.FieldDescriptor
	indexes         []string // 普通索引（逗号分隔字段）
	uniqueKeys      string   // 唯一键（逗号分隔字段）
	autoIncreaseKey string   // 自增字段名
	nullableFields  []string // 允许为NULL的字段

	// TiDB 方言选项：以 /*T!*/ 扩展注释形式进入 DDL，MySQL 视为普通注释忽略，
	// 同一份建表语句在 MySQL 与 TiDB 上均可执行（详见 proto/proto2mysql_option.proto 注释）
	tidbNonclusteredPK  bool   // 主键追加 /*T![clustered_index] NONCLUSTERED */
	tidbShardRowIDBits  uint32 // >0 时表尾追加 SHARD_ROW_ID_BITS=N
	tidbPreSplitRegions uint32 // >0 时与 SHARD_ROW_ID_BITS 同块追加 PRE_SPLIT_REGIONS=N（无 shard 时忽略）
	tidbAutoIDCacheOne  bool   // 表尾追加 /*T![auto_id_cache] AUTO_ID_CACHE=1 */

	// 预生成的SQL片段（Init时构建，之后只读）
	fieldsListSQL                string
	selectFieldsSQL              string
	selectAllSQLWithSemicolon    string
	selectAllSQLWithoutSemicolon string
	insertSQLTemplate            string
	replaceSQLPrefix             string

	// fieldNumbers 本消息认识的全部 proto 字段号，用于缓存条目的超集判定。
	// 详见 cache.go 里 encodeCacheEntry 的注释。
	fieldNumbers map[int32]struct{}

	// fieldNameToDesc 缓存字段名到描述符的映射
	fieldNameToDesc map[string]protoreflect.FieldDescriptor
	// cachedColumns 缓存数据库中的表结构（字段名->类型）
	cachedColumns map[string]string
	columnsMu     sync.RWMutex // 保护cachedColumns的并发安全
}

func (m *MessageTable) isNullableField(fieldName string) bool {
	return slices.Contains(m.nullableFields, fieldName)
}

func (m *MessageTable) isAutoIncrementField(fieldName string) bool {
	return m.autoIncreaseKey == fieldName
}

func buildPlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?, ", count), ", ")
}

// getMySQLFieldType 获取字段对应的MySQL目标类型（支持Timestamp特殊处理）
func (m *MessageTable) getMySQLFieldType(fieldDesc protoreflect.FieldDescriptor) string {
	// 特殊处理Timestamp类型：
	//   - DATETIME(6)：不带小数秒精度会把毫秒/纳秒**静默**截断到整秒（不报错、无警告）。
	//     MySQL时间类型最高到微秒，纳秒位仍会丢，这是MySQL的硬上限。
	//   - 恒定可空：proto的message字段天然是"有/无"两态，未设置时唯一正确的表示是NULL
	//     （空串在STRICT模式下被拒，'0000-00-00'在NO_ZERO_DATE下非法）。
	//     声明成NOT NULL的话，凡是没赋值该字段的行都插不进去，等于把这列变成必填，
	//     所以这里不受nullable选项影响，一律允许NULL。
	if fieldDesc.Message() != nil && fieldDesc.Message().FullName() == timestampFullName {
		return "DATETIME(6)"
	}

	if fieldDesc.IsMap() || fieldDesc.IsList() {
		return "MEDIUMBLOB" // 集合类型统一用MEDIUMBLOB
	}

	fieldName := string(fieldDesc.Name())
	baseType, ok := MySQLFieldTypes[fieldDesc.Kind()]
	if !ok {
		baseType = "TEXT" // 默认类型
	}

	// string/bytes 主键列映射为 VARCHAR(191)/VARBINARY(191)，不用 MEDIUMTEXT/MEDIUMBLOB：
	// TEXT/BLOB 列做主键必须带前缀长度，而前缀主键只保证前 191 个字符唯一，
	// 前缀相同的两个长键会被判成重复。191 是 utf8mb4 下仍可建索引的长度；
	// 列变成 VARCHAR 后 indexColumn 不再补前缀，主键按完整列比较。
	// 只按完整字段名匹配主键（含复合主键），非主键列的存储不变。
	for _, primaryKey := range m.primaryKey {
		if primaryKey != fieldName {
			continue
		}
		switch fieldDesc.Kind() {
		case protoreflect.StringKind:
			baseType = "VARCHAR(191)"
		case protoreflect.BytesKind:
			baseType = "VARBINARY(191)"
		}
		break
	}

	// 处理 nullable 字段
	if m.isNullableField(fieldName) {
		baseType = strings.ReplaceAll(baseType, " NOT NULL", "")
	}

	// 处理自增字段：移除默认值（修复Error 1067）
	if m.isAutoIncrementField(fieldName) {
		// 移除DEFAULT 0（避免自增字段默认值冲突）
		baseType = strings.ReplaceAll(baseType, " DEFAULT 0", "")
		baseType += " AUTO_INCREMENT"
	}

	return baseType
}

// DB 管理所有表的数据库实例
type DB struct {
	Tables map[string]*MessageTable
	DB     *sql.DB
	DBName string
	// ExpandOnly 只允许「纯新增」的结构变更。默认 false（保持既有行为，改名照常保留数据）。
	//
	// **滚动 / 金丝雀发布必须打开**：本库没有 schema 版本概念，每个进程都把自己的 proto
	// 当成唯一正确的目标结构，新旧两版同时在跑时 MODIFY / CHANGE 会被两边来回执行——
	// 不写任何数据，一次重启就翻一次面。ADD COLUMN 没有这个问题（旧版本的 SQL 里根本
	// 不会出现新列名），所以放行。违反时返回 ErrExpandOnlyViolation 并列出具体语句。
	ExpandOnly bool
	// tx 非空时所有增删改查走事务（由RunInTransaction设置）
	tx *sql.Tx
	// cache 可选的cache-aside缓存（EnableCache注入）；nil时全部直读DB
	cache    Cache
	cacheTTL time.Duration
	// pendingCacheDels 事务内暂存待删除的缓存key，提交成功后统一删除
	pendingCacheDels []string
	// tableExistsCache 缓存表是否存在的查询结果
	tableExistsCache map[string]bool
	tableExistsMu    sync.RWMutex
	// ctx 由WithContext绑定，用于超时控制/trace传递；nil时用context.Background()
	ctx context.Context
}

// contextExecutor 统一*sql.DB与*sql.Tx的context执行接口
type contextExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// sqlExecutor 绑定context的执行器：所有内部SQL都经由它下发，
// 保证WithContext传入的超时/trace能作用到每条语句
type sqlExecutor struct {
	ctx context.Context
	db  contextExecutor
}

func (e sqlExecutor) Exec(query string, args ...interface{}) (sql.Result, error) {
	return e.db.ExecContext(e.ctx, query, args...)
}

func (e sqlExecutor) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return e.db.QueryContext(e.ctx, query, args...)
}

func (e sqlExecutor) QueryRow(query string, args ...interface{}) *sql.Row {
	return e.db.QueryRowContext(e.ctx, query, args...)
}

// conn 返回当前执行器：事务内返回tx，否则返回DB（均绑定当前context）
func (p *DB) conn() sqlExecutor {
	if p.tx != nil {
		return sqlExecutor{ctx: p.context(), db: p.tx}
	}
	return sqlExecutor{ctx: p.context(), db: p.DB}
}

// context 返回当前绑定的context，未绑定时返回Background
func (p *DB) context() context.Context {
	if p.ctx != nil {
		return p.ctx
	}
	return context.Background()
}

// WithContext 返回绑定ctx的新实例（共享Tables/DB/缓存配置），用于传递超时与trace：
//
//	pbDB.WithContext(ctx).FindOneByPK(msg)
//
// 注意：请在根实例上调用；RunInTransaction内请直接使用回调收到的tx实例
// （事务实例的延迟缓存失效记录不会跨实例传递）。
func (p *DB) WithContext(ctx context.Context) *DB {
	return &DB{
		Tables:           p.Tables,
		DB:               p.DB,
		DBName:           p.DBName,
		tx:               p.tx,
		cache:            p.cache,
		cacheTTL:         p.cacheTTL,
		tableExistsCache: make(map[string]bool),
		ctx:              ctx,
	}
}

// wrapExecErr 把MySQL 1062（唯一键冲突）包装成可errors.Is(err, ErrDuplicateKey)判断的哨兵错误
func wrapExecErr(err error) error {
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1062 {
		return fmt.Errorf("%w: %w", ErrDuplicateKey, err)
	}
	return err
}

// RunInTransaction 在事务中执行fn：fn收到的tx可直接使用全部增删改查接口，
// fn返回错误时自动回滚，否则提交。适合“扣货币+发道具”等需要原子性的游戏逻辑。
// 若启用了缓存，事务内的缓存失效会延迟到提交成功后执行（回滚不删缓存）。
func (p *DB) RunInTransaction(fn func(tx *DB) error) error {
	var txDB *DB
	err := p.Transaction(func(sqlTx *sql.Tx) error {
		txDB = &DB{
			Tables:           p.Tables,
			DB:               p.DB,
			DBName:           p.DBName,
			tx:               sqlTx,
			cache:            p.cache,
			cacheTTL:         p.cacheTTL,
			tableExistsCache: make(map[string]bool),
			ctx:              p.ctx,
		}
		return fn(txDB)
	})
	if err == nil && txDB != nil {
		// 提交成功后统一失效缓存（先写库后删缓存）
		p.cacheDelKeys(txDB.pendingCacheDels...)
	}
	return err
}

// OpenDB 打开数据库连接并切换数据库
func (p *DB) OpenDB(db *sql.DB, dbname string) error {
	p.DB = db
	p.DBName = dbname
	_, err := p.DB.ExecContext(p.context(), "USE "+escapeMySQLName(p.DBName))
	return err
}

// MySQLFieldTypes MySQL字段类型映射表
var MySQLFieldTypes = map[protoreflect.Kind]string{
	protoreflect.Int32Kind:   "int NOT NULL DEFAULT 0",
	protoreflect.Uint32Kind:  "int unsigned NOT NULL DEFAULT 0",
	protoreflect.FloatKind:   "float NOT NULL DEFAULT 0",
	protoreflect.StringKind:  "MEDIUMTEXT",
	protoreflect.Int64Kind:   "bigint NOT NULL DEFAULT 0",
	protoreflect.Uint64Kind:  "bigint unsigned NOT NULL DEFAULT 0",
	protoreflect.DoubleKind:  "double NOT NULL DEFAULT 0",
	protoreflect.BoolKind:    "tinyint(1) NOT NULL DEFAULT 0",
	protoreflect.EnumKind:    "int NOT NULL DEFAULT 0",
	protoreflect.BytesKind:   "MEDIUMBLOB",
	protoreflect.MessageKind: "MEDIUMBLOB",
}

// 解析MySQL类型信息
type mysqlTypeInfo struct {
	baseType string
	length   int
	decimal  int
	unsigned bool
}

// 解析MySQL类型字符串
func parseMySQLType(colType string) mysqlTypeInfo {
	info := mysqlTypeInfo{}
	parts := strings.Fields(strings.ToLower(colType))

	if len(parts) == 0 {
		return info
	}

	// 提取基础类型
	basePart := parts[0]
	if idx := strings.Index(basePart, "("); idx != -1 {
		info.baseType = basePart[:idx]
		// 提取长度和小数位
		params := strings.Trim(basePart[idx:], "()")
		if strings.Contains(params, ",") {
			parts := strings.Split(params, ",")
			if len(parts) >= 1 {
				info.length, _ = strconv.Atoi(strings.TrimSpace(parts[0]))
			}
			if len(parts) >= 2 {
				info.decimal, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
			}
		} else {
			info.length, _ = strconv.Atoi(params)
		}
	} else {
		info.baseType = basePart
	}

	// 检查是否为unsigned
	for _, part := range parts[1:] {
		if part == "unsigned" {
			info.unsigned = true
			break
		}
	}

	return info
}

// 判断两个MySQL类型是否匹配（增强版）
func isTypeMatch(currentType, targetType string) bool {
	current := parseMySQLType(currentType)
	target := parseMySQLType(targetType)

	currentBase := normalizeBaseType(current.baseType)
	targetBase := normalizeBaseType(target.baseType)

	// 基础类型不同：同族跨类型（int↔bigint、varchar↔mediumtext、float↔double）
	// 按容量阶梯判方向；跨族（比如 int↔mediumtext）没有可比性，一律判不兼容。
	if currentBase != targetBase {
		for _, family := range typeFamilies {
			currentRank, okCurrent := family.ranks[currentBase]
			targetRank, okTarget := family.ranks[targetBase]
			if !okCurrent || !okTarget {
				continue
			}
			if family.isInteger && current.unsigned != target.unsigned {
				// 有无符号决定的是值域方向而不是宽窄，两边都可能装不下对方，必须ALTER。
				return false
			}
			return currentRank >= targetRank
		}
		return false
	}

	// 同一基础类型，再比长度/精度/符号。口径同上：线上装得下目标就不动它。
	switch currentBase {
	case "varchar", "char":
		// 线上更宽时不动它（收窄会截断已有数据）。
		return current.length >= target.length
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		// 位宽相同，只剩有无符号要比。
		return current.unsigned == target.unsigned
	case "float", "double":
		// 小数位同理，线上精度更高时不降。
		return current.decimal >= target.decimal
	case "datetime":
		// 括号里的是小数秒精度(fsp)。线上精度低于目标时必须ALTER，
		// 否则老表停留在DATETIME(0)，写入的毫秒会被静默丢掉——精度修了等于没修。
		// 线上精度更高时不动它（降精度会丢数据）。
		return current.length >= target.length
	}

	return true
}

// baseTypeAliases 把等价写法折叠到同一个名字再比较。
var baseTypeAliases = map[string]string{
	"bool":       "tinyint",
	"integer":    "int",
	"mediumtext": "mediumtext",
	"text":       "text",
	"blob":       "blob",
	"mediumblob": "mediumblob",
	"datetime":   "datetime",
	"timestamp":  "datetime",
	"varchar":    "varchar",
	"char":       "char",
}

// normalizeBaseType 归一基础类型名；表里没有的原样返回。
func normalizeBaseType(baseType string) string {
	if mapped, ok := baseTypeAliases[baseType]; ok {
		return mapped
	}
	return baseType
}

// typeFamily 是一族可互相比较"谁更宽"的MySQL类型。
type typeFamily struct {
	ranks     map[string]int
	isInteger bool
}

// typeFamilies 同族容量阶梯。判定口径统一为「线上装得下目标就不动它」：
//
//	线上容量 >= 目标容量  → 兼容，不生成ALTER
//	线上容量 <  目标容量  → 必须ALTER拓宽
//
// 这条口径原先只有datetime分支做对了，另外两族是坏的：
//
//   - varchar/char 与 float/double 的方向**是反的**（写的是 target >= current）。
//     后果双向都错：线上varchar(50)、proto要varchar(100)时判「兼容」不拓宽，
//     写100字符报1406；线上varchar(100)、proto要varchar(50)时反而去ALTER收窄。
//   - 整数族**根本没有方向判断**。int与bigint是不同的baseType，在上面的
//     currentBase != targetBase 处就直接判不兼容了，于是**两个方向都ALTER**。
//
// 为什么这在滚动发布下是P0：本库没有schema版本概念，每个进程都把自己的proto当成
// 唯一正确的目标结构。v2把uint32拓宽成uint64之后，任何一个还在跑v1的副本**一重启**
// 就把列MODIFY回int unsigned——不需要写任何数据，schema就在新旧副本之间来回翻面。
// 文本族同理：2026-08-19实测撞到过线上mediumtext被varchar(255)的一侧重建，
// 同一条写入在宽列副本成功、窄列副本报1406，且不可复现。
//
// 收窄是有损操作，与本库「永不DROP COLUMN」的既有立场一致：确实要收窄请手写ALTER。
var typeFamilies = []typeFamily{
	{ranks: map[string]int{"tinyint": 1, "smallint": 2, "mediumint": 3, "int": 4, "bigint": 5}, isInteger: true},
	{ranks: map[string]int{"char": 1, "varchar": 2, "tinytext": 3, "text": 4, "mediumtext": 5, "longtext": 6}},
	{ranks: map[string]int{"binary": 1, "varbinary": 2, "tinyblob": 3, "blob": 4, "mediumblob": 5, "longblob": 6}},
	{ranks: map[string]int{"float": 1, "double": 2}},
}

// escapeMySQLName 将MySQL标识符整体转义，兼容包含点号的protobuf full name表名。
func escapeMySQLName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// TextIndexPrefixLength 在 TEXT/BLOB 列上建索引时使用的前缀长度。
//
// MySQL **不允许**对 TEXT/BLOB 列建不带前缀长度的索引，直接报
// Error 1170 BLOB/TEXT column used in key specification without a key length。
// 而本库把 string 映射成 MEDIUMTEXT，所以只要在 string 列上声明了 index / unique_key，
// 不补前缀产出的就是一条 MySQL 会拒绝执行的 DDL。
//
// 这个洞长期没暴露，是因为测试只比对 SQL 字符串、从不真的执行：拿本仓库自带的
// tools/proto2sql/testdata/account.proto 生成建表语句打到 MySQL 8.4 上就是 Error 1170。
//
// 191 是 utf8mb4 下的经典安全值（旧的 767 字节索引上限 ÷ 4）。
// 设为 0 表示不补前缀——产出的 DDL 建不了表，只在做历史输出比对时才有意义。
const TextIndexPrefixLength = 191

// validateFieldKinds 检查所有字段都有 MySQL 类型映射，没有就 fail-fast。
//
// 放在生成 DDL 的入口做，而不是等到第一次写入才由 pbconv 抛错——那时列已经
// 按 TEXT 建出来了，改回正确类型是跨族 MODIFY，会把数据吃成 0。
func (m *MessageTable) validateFieldKinds() error {
	fields := m.Descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		fieldDesc := fields.Get(i)
		if fieldDesc.IsMap() || fieldDesc.IsList() || fieldDesc.Kind() == protoreflect.MessageKind {
			continue // 这三类统一落 MEDIUMBLOB，不查映射表
		}
		if _, ok := MySQLFieldTypes[fieldDesc.Kind()]; !ok {
			return fmt.Errorf("%w: 表 %s 的字段 %s（%s）。"+
				"sint32/sint64/fixed32/fixed64/sfixed32/sfixed64 都不支持，"+
				"改用 int32/int64/uint32/uint64 即可（取值范围一样，只是线上编码不同）",
				ErrUnsupportedFieldKind, m.tableName, fieldDesc.Name(), fieldDesc.Kind())
		}
	}
	return nil
}

// needsIndexPrefix 该列建索引时是否必须带前缀长度（TEXT/BLOB 系列都要）。
func (m *MessageTable) needsIndexPrefix(col string) bool {
	if TextIndexPrefixLength <= 0 {
		return false
	}
	fieldDesc, ok := m.fieldNameToDesc[col]
	if !ok {
		return false
	}
	colType := strings.ToUpper(m.getMySQLFieldType(fieldDesc))
	return strings.Contains(colType, "TEXT") || strings.Contains(colType, "BLOB")
}

// indexColumn 索引里的一列，必要时补前缀长度。见 TextIndexPrefixLength 的注释。
func (m *MessageTable) indexColumn(col string) string {
	escaped := escapeMySQLName(col)
	if m.needsIndexPrefix(col) {
		return fmt.Sprintf("%s(%d)", escaped, TextIndexPrefixLength)
	}
	return escaped
}

// GetCreateTableSQL 生成创建表的SQL语句
func (m *MessageTable) GetCreateTableSQL() string {
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", escapeMySQLName(m.tableName))
	fields := []string{}
	indexes := []string{}

	desc := m.Descriptor
	for i := 0; i < desc.Fields().Len(); i++ {
		field := desc.Fields().Get(i)
		fieldName := string(field.Name())
		escapedName := escapeMySQLName(fieldName)

		fieldType := m.getMySQLFieldType(field)

		fields = append(fields, fmt.Sprintf("  %s %s%s", escapedName, fieldType, columnComment(field.Number())))
	}

	if len(m.primaryKey) > 0 {
		primaryKeys := make([]string, len(m.primaryKey))
		for i, pk := range m.primaryKey {
			primaryKeys[i] = m.indexColumn(pk)
		}
		pkClause := fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(primaryKeys, ","))
		if m.tidbNonclusteredPK {
			pkClause += tidbNonclusteredPKSQL
		}
		fields = append(fields, pkClause)
	}

	if len(m.indexes) > 0 {
		for idx, indexCols := range m.indexes {
			cols := strings.Split(indexCols, ",")
			quotedCols := make([]string, len(cols))
			for i, col := range cols {
				quotedCols[i] = m.indexColumn(strings.TrimSpace(col))
			}
			indexName := fmt.Sprintf("idx_%s_%d", m.tableName, idx)
			indexes = append(indexes, fmt.Sprintf("  INDEX %s (%s)", escapeMySQLName(indexName), strings.Join(quotedCols, ",")))
		}
	}

	if m.uniqueKeys != "" {
		uniqueCols := strings.Split(m.uniqueKeys, ",")
		quotedUniqueCols := make([]string, len(uniqueCols))
		for i, col := range uniqueCols {
			name := strings.TrimSpace(col)
			if m.needsIndexPrefix(name) {
				log.Printf("warning: unique key on TEXT/BLOB column %s in table %s only enforces "+
					"uniqueness over the first %d characters", name, m.tableName, TextIndexPrefixLength)
			}
			quotedUniqueCols[i] = m.indexColumn(name)
		}
		indexes = append(indexes, fmt.Sprintf("  UNIQUE KEY %s (%s)", escapeMySQLName("uk_"+m.tableName), strings.Join(quotedUniqueCols, ",")))
	}

	stmt += strings.Join(fields, ",\n")
	if len(indexes) > 0 {
		stmt += ",\n" + strings.Join(indexes, ",\n")
	}

	// 表注释简化为表名；TiDB 方言块按 TiDB 规范导出顺序放在 COMMENT 之前
	stmt += "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci" + m.tidbTableOptionsSQL() + " COMMENT='" + escapeMySQLComment(m.tableName) + "';"
	return stmt
}

// tidbNonclusteredPKSQL 主键的 TiDB 非聚簇声明（含前导空格）。
// TiDB 扩展注释语法：TiDB 解析注释内容，MySQL 视为普通注释忽略。
const tidbNonclusteredPKSQL = " /*T![clustered_index] NONCLUSTERED */"

// tidbTableOptionsSQL 生成表级 TiDB 方言片段（含前导空格），无任何 TiDB 选项时返回空串。
// 输出顺序与 TiDB SHOW CREATE TABLE 的规范导出一致（AUTO_ID_CACHE 块在前、SHARD 块在后、
// 整体位于 COMMENT 之前），便于与线上表结构做文本 diff。
// 两条 fail-safe（宁可忽略选项并告警，也不生成一条必然报错的 DDL）：
//   - SHARD_ROW_ID_BITS 只支持非聚簇主键/无主键表：有主键却未声明 NONCLUSTERED 时忽略 shard（连带 preSplit）；
//   - TiDB 要求 PRE_SPLIT_REGIONS ≤ SHARD_ROW_ID_BITS：超出时收敛到 shard 值。
func (m *MessageTable) tidbTableOptionsSQL() string {
	var sb strings.Builder
	if m.tidbAutoIDCacheOne {
		sb.WriteString(" /*T![auto_id_cache] AUTO_ID_CACHE=1 */")
	}
	shardBits := m.tidbShardRowIDBits
	if shardBits > 0 && len(m.primaryKey) > 0 && !m.tidbNonclusteredPK {
		log.Printf("proto2mysql: 表 %s 声明了 SHARD_ROW_ID_BITS 但主键未声明 NONCLUSTERED，TiDB 聚簇表不支持该选项，已忽略（请补 tidb_nonclustered_pk）", m.tableName)
		shardBits = 0
	}
	if shardBits > 0 {
		fmt.Fprintf(&sb, " /*T! SHARD_ROW_ID_BITS=%d", shardBits)
		if pre := m.tidbPreSplitRegions; pre > 0 {
			if pre > shardBits {
				log.Printf("proto2mysql: 表 %s 的 PRE_SPLIT_REGIONS=%d 超过 SHARD_ROW_ID_BITS=%d，已收敛到 %d（TiDB 要求前者 ≤ 后者）", m.tableName, pre, shardBits, shardBits)
				pre = shardBits
			}
			fmt.Fprintf(&sb, " PRE_SPLIT_REGIONS=%d", pre)
		}
		sb.WriteString(" */")
	}
	return sb.String()
}

// escapeMySQLComment 转义MySQL注释中的特殊字符（仅保留基础转义）
func escapeMySQLComment(comment string) string {
	return strings.ReplaceAll(strings.ReplaceAll(comment, "'", "\\'"), "\n", " ")
}

// columnCommentPrefix 列注释中记录 proto 字段号的前缀，形如 COMMENT 'pb:3'。
// 迁移时据此按字段号识别列，从而支持字段改名（CHANGE COLUMN）并保留原有数据。
const columnCommentPrefix = "pb:"

// columnComment 生成带 proto 字段号的列注释片段（含前导空格），如 " COMMENT 'pb:3'"。
func columnComment(num protoreflect.FieldNumber) string {
	return fmt.Sprintf(" COMMENT '%s%d'", columnCommentPrefix, num)
}

// parseFieldNumFromComment 从列注释解析 proto 字段号；无 pb:N 前缀或非法时返回 (0,false)。
func parseFieldNumFromComment(comment string) (protoreflect.FieldNumber, bool) {
	if !strings.HasPrefix(comment, columnCommentPrefix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(comment, columnCommentPrefix))
	if err != nil || n < int(protowire.MinValidNumber) || n > int(protowire.MaxValidNumber) {
		return 0, false
	}
	return protoreflect.FieldNumber(n), true
}

// columnMeta 线上单列的元信息：类型 + 从注释解析出的 proto 字段号（0 表示无字段号注释，
// 通常是旧版本创建的表）。
type columnMeta struct {
	colType  string
	fieldNum protoreflect.FieldNumber
}

// getTableColumns 获取表当前字段结构信息
func (p *DB) getTableColumns(tableName string) (map[string]string, error) {
	table, ok := p.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	table.columnsMu.RLock()
	if table.cachedColumns != nil {
		defer table.columnsMu.RUnlock()
		return table.cachedColumns, nil
	}
	table.columnsMu.RUnlock()

	query := `
		SELECT COLUMN_NAME, COLUMN_TYPE 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`
	rows, err := p.DB.QueryContext(p.context(), query, p.DBName, table.tableName)
	if err != nil {
		return nil, fmt.Errorf("query columns for table %s: %w", table.tableName, err)
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var colName, colType string
		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, fmt.Errorf("scan columns for table %s: %w", tableName, err)
		}
		columns[colName] = colType
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error for table %s columns: %w", tableName, err)
	}

	table.columnsMu.Lock()
	table.cachedColumns = columns
	table.columnsMu.Unlock()

	return columns, nil
}

// getTableColumnMeta 读取线上表每列的类型与 proto 字段号注释（COLUMN_COMMENT 中的 pb:N）。
// 用于迁移时按字段号识别列以支持改名保留数据。不使用 cachedColumns（迁移不频繁，且需要
// 注释信息），避免与 getTableColumns 的类型缓存混淆。
func (p *DB) getTableColumnMeta(tableName string) (map[string]columnMeta, error) {
	table, ok := p.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	query := `
		SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`
	rows, err := p.DB.QueryContext(p.context(), query, p.DBName, table.tableName)
	if err != nil {
		return nil, fmt.Errorf("query column meta for table %s: %w", table.tableName, err)
	}
	defer rows.Close()

	metas := make(map[string]columnMeta)
	for rows.Next() {
		var colName, colType, colComment string
		if err := rows.Scan(&colName, &colType, &colComment); err != nil {
			return nil, fmt.Errorf("scan column meta for table %s: %w", tableName, err)
		}
		meta := columnMeta{colType: colType}
		if num, ok := parseFieldNumFromComment(colComment); ok {
			meta.fieldNum = num
		}
		metas[colName] = meta
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error for table %s column meta: %w", tableName, err)
	}

	return metas, nil
}

// clearColumnCache 清除表字段缓存
func (p *DB) clearColumnCache(tableName string) {
	if table, ok := p.Tables[tableName]; ok {
		table.clearColumnCache()
	}
}

// clearColumnCache 清除本表的字段类型缓存（DDL 变更后必须调用）。
func (m *MessageTable) clearColumnCache() {
	m.columnsMu.Lock()
	m.cachedColumns = nil
	m.columnsMu.Unlock()
}

// CreateOrUpdateTable 创建表或同步已有表字段结构。
func (p *DB) CreateOrUpdateTable(m proto.Message) error {
	tableName := GetTableName(m)
	if _, ok := p.Tables[tableName]; !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}
	return p.UpdateTableField(m)
}

// buildAlterClauses 按 proto 定义与线上字段结构(currentCols)比对，生成 ALTER TABLE 的
// 子句列表。匹配优先级：
//  1. 列名精确匹配：类型不兼容或缺少字段号注释时 MODIFY COLUMN（顺带回填注释）；
//  2. 字段号匹配（列名不同但注释里的 proto 字段号与当前字段一致）：CHANGE COLUMN 改名并对齐
//     类型，原有数据保留（这是"根据 Field id 改名字/类型"的核心）；
//  3. 都无匹配：ADD COLUMN 新增字段。
//
// 所有生成的列均带 COMMENT 'pb:N'，以便后续迁移继续按字段号识别列。
// 不修改传入的 currentCols（内部拷贝一份）。
func (m *MessageTable) buildAlterClauses(currentCols map[string]columnMeta, expandOnly bool) ([]string, error) {
	remaining := make(map[string]columnMeta, len(currentCols))
	if err := m.validateFieldKinds(); err != nil {
		return nil, err
	}

	// byFieldNum 必须**确定性**构建。
	//
	// 早先是直接 `byFieldNum[meta.fieldNum] = name` 边遍历 currentCols 边写——而 Go 的
	// map 迭代顺序是随机化的。正常情况下一个 pb:N 只对应一列，看不出问题；但线上表出现
	// 两列带同一个 pb:N 时（DBA 照 SHOW CREATE TABLE 复制一个备份列就会），
	// **每次运行挑中的列都可能不同**，产出的 CHANGE COLUMN 也就不同——其中一种会去
	// 改那个备份列。同一个库、同一份 proto、同一个线上结构，跑两次得到两份 DDL，
	// Go 版连自己都不逐字节相同（Python 侧的 dict 推导是确定的，所以只有 Go 有这个问题）。
	//
	// 冲突时取**列名字典序最小**的那个：规则简单、可复现，且与"先建的列通常名字更短/更早"
	// 无关——重点是无论跑多少次都给出同一个答案。同时打一条告警，因为重复的 pb:N
	// 本身就是个需要人工处理的异常。
	byFieldNum := make(map[protoreflect.FieldNumber]string, len(currentCols))
	for name, meta := range currentCols {
		remaining[name] = meta
		if meta.fieldNum == 0 {
			continue
		}
		if prev, dup := byFieldNum[meta.fieldNum]; dup {
			log.Printf("warning: table %s 有多列带同一个 pb:%d 注释（%q 与 %q），"+
				"按列名字典序取较小者以保证可复现；请人工确认哪一列才是真正的数据列",
				m.tableName, meta.fieldNum, prev, name)
			if prev < name {
				continue
			}
		}
		byFieldNum[meta.fieldNum] = name
	}

	var alterSQLs []string
	desc := m.Descriptor
	for i := 0; i < desc.Fields().Len(); i++ {
		fieldDesc := desc.Fields().Get(i)
		fieldName := string(fieldDesc.Name())

		if keywordRegex.MatchString(strings.ToUpper(fieldName)) {
			log.Printf("warning: field %s in table %s conflicts with MySQL keyword", fieldName, m.tableName)
		}

		fieldNum := fieldDesc.Number()
		targetType := m.getMySQLFieldType(fieldDesc)
		comment := columnComment(fieldNum)

		// 1) 列名精确匹配
		if meta, exists := remaining[fieldName]; exists {
			// 类型不兼容，或旧表该列尚无字段号注释时，MODIFY 顺带回填注释
			if !isTypeMatch(meta.colType, targetType) || meta.fieldNum != fieldNum {
				alterSQLs = append(alterSQLs, fmt.Sprintf("MODIFY COLUMN %s %s%s", escapeMySQLName(fieldName), targetType, comment))
			}
			delete(remaining, fieldName)
			continue
		}

		// 2) 字段号匹配（改名场景）：找到注释字段号一致但列名不同的现有列
		if oldName, ok := byFieldNum[fieldNum]; ok {
			if oldMeta, still := remaining[oldName]; still {
				if !isRenameConvertible(oldMeta.colType, targetType) {
					// 类型跨族对不上，这不是改名，是字段号被复用了。照常生成 CHANGE 的话，
					// MySQL 的隐式转换会把旧列内容整列吃掉，而本库「永不 DROP COLUMN」
					// 的保护在这里完全帮不上忙。
					return nil, fmt.Errorf("%w: 表 %s 的列 %s（%s，pb:%d）与新字段 %s（%s，pb:%d）"+
						"类型跨族，无法当作改名处理。字段号是 protobuf 的身份，永不复用："+
						"删字段请用 reserved，新字段另取一个没用过的编号。"+
						"若确实要把这一列的数据转成新类型，请人工写 ALTER 并自行确认转换语义",
						ErrFieldNumberReused, m.tableName, oldName, oldMeta.colType, fieldNum,
						fieldName, targetType, fieldNum)
				}
				// 改名能保留数据，但库无法判断谁新谁旧：滚动发布时新旧两版会把这一列来回
				// 改名（v2 改成新名 → 任何一个 v1 副本重启又改回旧名 → v2 立刻 Error 1054）。
				// 所以这里必须留痕。
				log.Printf("warning: table %s: column %s -> %s by field number pb:%d. "+
					"滚动发布期间新旧副本会来回改名，正确做法是 expand→migrate→contract"+
					"（先加新列、双写回填、下个版本再删旧列）；或对同步调用打开 ExpandOnly",
					m.tableName, oldName, fieldName, fieldNum)
				alterSQLs = append(alterSQLs, fmt.Sprintf("CHANGE COLUMN %s %s %s%s",
					escapeMySQLName(oldName), escapeMySQLName(fieldName), targetType, comment))
				delete(remaining, oldName)
				continue
			}
		}

		// 3) 全新字段
		alterSQLs = append(alterSQLs, fmt.Sprintf("ADD COLUMN %s %s%s", escapeMySQLName(fieldName), targetType, comment))
	}

	if expandOnly {
		var offenders []string
		for _, clause := range alterSQLs {
			if !strings.HasPrefix(clause, "ADD COLUMN") {
				offenders = append(offenders, clause)
			}
		}
		if len(offenders) > 0 {
			return nil, fmt.Errorf("%w: 表 %s 的本次对齐含非「纯新增」变更，ExpandOnly 下拒绝执行：\n  %s\n"+
				"  这些语句在滚动发布下会被新旧副本来回执行。请改成 expand→migrate→contract 三步，"+
				"或人工审核后单独执行", ErrExpandOnlyViolation, m.tableName, strings.Join(offenders, "\n  "))
		}
	}

	return alterSQLs, nil
}

// isRenameConvertible 线上旧列的类型，能不能安全承接改名后的新类型。
//
// 改名保留数据靠的是 CHANGE COLUMN，而 MySQL 会对它做**隐式类型转换**。同基础类型、
// 或同族（int↔bigint、varchar↔mediumtext）都算能接；跨族（mediumtext↔bigint）一律
// 不能——那多半根本不是改名，而是 proto 里把一个已删字段的编号让给了类型完全不同的新字段。
func isRenameConvertible(currentType, targetType string) bool {
	currentBase := normalizeBaseType(parseMySQLType(currentType).baseType)
	targetBase := normalizeBaseType(parseMySQLType(targetType).baseType)
	if currentBase == targetBase {
		return true
	}
	for _, family := range typeFamilies {
		_, okCurrent := family.ranks[currentBase]
		_, okTarget := family.ranks[targetBase]
		if okCurrent && okTarget {
			return true
		}
	}
	return false
}

// UpdateTableField 同步表字段（表不存在则创建，存在则对齐字段类型）
func (p *DB) UpdateTableField(m proto.Message) error {
	tableName := GetTableName(m)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}
	return p.syncTableSchema(tableName, table)
}

// syncTableSchema 按 registryKey（proto full name）对应的 table 同步 MySQL 表结构：
// 表不存在则创建，存在则对齐字段类型。
func (p *DB) syncTableSchema(registryKey string, table *MessageTable) error {
	exists, err := p.IsTableExists(table.tableName)
	if err != nil {
		return fmt.Errorf("检查表 %s 存在性: %w", table.tableName, err)
	}

	// 表不存在就先建。注意这里**刻意不 return**，建完继续往下走列对齐。
	//
	// 建表语句是 CREATE TABLE IF NOT EXISTS，在并发下可能整条是 no-op：两个版本的
	// 进程同时冷启动到空库时，先到的那个按自己的 proto 建表，后到的这条 CREATE 只会
	// 拿到一条 Warning 1050——不报错、不改结构。早先这里直接 return nil，于是后到进程
	// 独有的新列**从未被添加**，而它自己启动成功、零异常，一直到第一条 SELECT 才报
	// Error 1054 Unknown column；且表存在性缓存已置 true，重启也不重新对齐，**不自愈**。
	//
	// 落到对齐路径上就没有这个问题：getTableColumnMeta 直读 INFORMATION_SCHEMA
	// （不走列类型缓存），拿到的是真实建成的结构，缺什么补什么。自己建成功的那条路径
	// 上 buildAlterClauses 返回空，只多一次元信息查询，代价可以忽略。
	if !exists {
		createSQL := table.GetCreateTableSQL()
		if _, err := p.DB.ExecContext(p.context(), createSQL); err != nil {
			return fmt.Errorf("创建表 %s 失败: %w, SQL: %s", table.tableName, err, createSQL)
		}
		p.updateTableExistsCache(table.tableName, true)
	}

	// 表已存在，同步字段结构（读取列类型 + 字段号注释，支持按 Field id 改名保留数据）
	currentCols, err := p.getTableColumnMeta(registryKey)
	if err != nil {
		return fmt.Errorf("获取表 %s 字段: %w", registryKey, err)
	}

	alterSQLs, err := table.buildAlterClauses(currentCols, p.ExpandOnly)
	if err != nil {
		return err
	}

	// 补齐缺失的主键。
	//
	// buildAlterClauses 只对齐列（ADD/MODIFY/CHANGE COLUMN），从不看主键。
	// 主键必须与列变更放进同一条 ALTER：主键列常同时带 AUTO_INCREMENT，若先单独
	// MODIFY 成 AUTO_INCREMENT、再 ADD PRIMARY KEY，MySQL 会在第一条语句就报
	// Error 1075（auto column must be defined as a key），永远到不了补主键那一步。
	// 同一条 ALTER 也保证列对齐与补主键原子成功或失败。
	//
	// 这里只补“从无到有”，绝不自动 DROP/改写已有主键。ADD PRIMARY KEY 在已有
	// 重复行时会失败；这是预期的 fail-closed 行为，调用方必须先人工去重再重试。
	missingPrimaryKey := false
	if len(table.primaryKey) > 0 {
		hasPK, err := p.tableHasPrimaryKey(table.tableName)
		if err != nil {
			return fmt.Errorf("检查表 %s 主键存在性: %w", table.tableName, err)
		}
		if !hasPK {
			missingPrimaryKey = true
			pkCols := make([]string, len(table.primaryKey))
			for i, pk := range table.primaryKey {
				pkCols[i] = escapeMySQLName(pk)
			}
			pkClause := fmt.Sprintf("ADD PRIMARY KEY (%s)", strings.Join(pkCols, ","))
			if table.tidbNonclusteredPK {
				// TiDB 补主键只能是非聚簇（省略关键字时默认即非聚簇），显式注释仅为语义自文档化；MySQL 视为注释忽略
				pkClause += tidbNonclusteredPKSQL
			}
			alterSQLs = append(alterSQLs, pkClause)
			log.Printf("table %s is missing its primary key; adding %s", table.tableName, strings.Join(pkCols, ","))
		}
	}

	// 补齐 proto 里声明了、但线上还没有的索引。
	//
	// 早先索引只出现在 CREATE TABLE 分支：表一旦建成，之后在 .proto 里新加
	// index / unique_key **完全不生效**，而且零提示——查询照常能跑，只是走全表扫描，
	// 数据量上来才表现为"莫名其妙变慢"，谁也想不到是建表选项没落地。
	indexClauses, err := p.missingIndexClauses(table)
	if err != nil {
		return err
	}
	alterSQLs = append(alterSQLs, indexClauses...)

	// 执行ALTER TABLE（如果有需要修改的列或需要补主键）
	if len(alterSQLs) > 0 {
		alterSQL := fmt.Sprintf("ALTER TABLE %s %s", escapeMySQLName(table.tableName), strings.Join(alterSQLs, ", "))
		_, err := p.DB.ExecContext(p.context(), alterSQL)
		if err != nil {
			if missingPrimaryKey {
				return fmt.Errorf("更新表 %s 结构并补齐主键失败(可能存在重复行，需先去重再重试): %w, SQL: %s",
					table.tableName, err, alterSQL)
			}
			return fmt.Errorf("更新表 %s 结构失败: %w, SQL: %s", table.tableName, err, alterSQL)
		}
		p.clearColumnCache(registryKey) // 清除缓存，下次查询时重新加载字段
		p.awaitSchemaVisible(registryKey, table, alterSQLs)
	}

	return nil
}

const (
	// SchemaSettleTimeout 等 DDL 在所有节点生效的最长时间。
	SchemaSettleTimeout = 60 * time.Second
	// SchemaSettleInterval 就绪探测的轮询间隔。
	SchemaSettleInterval = 200 * time.Millisecond
)

// addedColumnNames 从 ALTER 子句里挑出本次**新增**的列名。
//
// 只等这些列：MODIFY / CHANGE 改的是已有列，回读时本来就看得见，
// 等它们既没意义又会把探测拖长。
func addedColumnNames(alterSQLs []string) map[string]struct{} {
	const prefix = "ADD COLUMN `"
	names := make(map[string]struct{})
	for _, clause := range alterSQLs {
		if !strings.HasPrefix(clause, prefix) {
			continue
		}
		rest := clause[len(prefix):]
		if end := strings.Index(rest, "`"); end > 0 {
			names[strings.ReplaceAll(rest[:end], "``", "`")] = struct{}{}
		}
	}
	return names
}

// awaitSchemaVisible 等到 ALTER 的结果**真的能被看见**，再放行后续 SQL。
//
// MySQL 单机上这一步立刻就过（DDL 返回即生效），几乎零开销。
//
// 但 **TiDB 的 DDL 是异步 online 的**：ALTER 语句返回时，schema 变更只是进了 DDL 队列，
// 各个 TiDB 节点要按 lease（默认 45s）分批加载新的 schema 版本。库执行完 ALTER 立刻按
// 新 proto 发 INSERT/SELECT，这段窗口里连到**还没加载新 schema 的节点**就会报
// Unknown column——启动日志漂亮，第一批请求全挂。原先这里没有任何等待或重试。
//
// 实现上刻意**不去检测"后端是不是 TiDB"**：版本号/变量嗅探很容易被兼容层骗过，
// 而"回读 information_schema 直到结构真的对上"是纯行为判定，对 MySQL、TiDB、
// 以及任何自称兼容的实现都成立。
func (p *DB) awaitSchemaVisible(registryKey string, table *MessageTable, alterSQLs []string) {
	want := addedColumnNames(alterSQLs)
	if len(want) == 0 {
		return // 本次没有新增列（只有 MODIFY / CHANGE / 索引），无需等待
	}

	deadline := time.Now().Add(SchemaSettleTimeout)
	for {
		live, err := p.getTableColumnMeta(registryKey)
		if err != nil {
			log.Printf("warning: 回读表 %s 结构失败，跳过就绪探测：%v", table.tableName, err)
			return
		}
		if len(live) == 0 {
			// 一列都读不到 = 根本看不见这张表（权限/库名不对/驱动不给结果）。
			// 真表不可能零列，所以这不是"还没生效"，继续轮询只会白等到超时。
			return
		}

		var missing []string
		for name := range want {
			if _, ok := live[name]; !ok {
				missing = append(missing, name)
			}
		}
		if len(missing) == 0 {
			return
		}
		if time.Now().After(deadline) {
			// 只告警不返回错误：结构可能确实还在后台排队。
			// 报错会把"慢"升级成"起不来"。
			slices.Sort(missing)
			log.Printf("warning: 表 %s 的结构变更在 %v 内没有全部可见（仍缺 %v）。"+
				"TiDB 的 DDL 是异步的，可能仍在后台排队；"+
				"若后续 SQL 报 Unknown column，等一个 schema lease 再重试",
				table.tableName, SchemaSettleTimeout, missing)
			return
		}
		time.Sleep(SchemaSettleInterval)
	}
}

// existingIndexNames 线上这张表已有的索引名（不含 PRIMARY）。
func (p *DB) existingIndexNames(tableName string) (map[string]struct{}, error) {
	rows, err := p.DB.QueryContext(p.context(),
		"SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?", p.DBName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "" && name != "PRIMARY" {
			names[name] = struct{}{}
		}
	}
	return names, rows.Err()
}

// missingIndexClauses proto 里声明了、线上却没有的索引，生成 ADD INDEX / ADD UNIQUE KEY。
//
// **只加不删**：线上多出来的索引一律不动（可能是 DBA 按查询模式手工加的，库没有立场去删）。
// 与「永不 DROP COLUMN」是同一个立场。
//
// 索引名与 CREATE TABLE 分支保持一致（idx_<表名>_<序号> / uk_<表名>），否则同一份 proto
// 在"新建表"和"老表补索引"两条路径上会产出不同的索引名。
func (p *DB) missingIndexClauses(table *MessageTable) ([]string, error) {
	if len(table.indexes) == 0 && table.uniqueKeys == "" {
		return nil, nil // proto 里一个索引都没声明，不必去查 information_schema
	}

	existing, err := p.existingIndexNames(table.tableName)
	if err != nil {
		// 查不到就跳过，不阻断结构同步
		log.Printf("warning: 读取表 %s 的既有索引失败，本次跳过索引补齐：%v", table.tableName, err)
		return nil, nil
	}

	var clauses []string
	for idx, indexCols := range table.indexes {
		name := fmt.Sprintf("idx_%s_%d", table.tableName, idx)
		if _, ok := existing[name]; ok {
			continue
		}
		cols := strings.Split(indexCols, ",")
		quoted := make([]string, len(cols))
		for i, col := range cols {
			quoted[i] = table.indexColumn(strings.TrimSpace(col))
		}
		clauses = append(clauses, fmt.Sprintf("ADD INDEX %s (%s)",
			escapeMySQLName(name), strings.Join(quoted, ",")))
		log.Printf("table %s 补索引 %s (%s)", table.tableName, name, strings.Join(quoted, ","))
	}

	if table.uniqueKeys != "" {
		name := "uk_" + table.tableName
		if _, ok := existing[name]; !ok {
			cols := strings.Split(table.uniqueKeys, ",")
			quoted := make([]string, len(cols))
			for i, col := range cols {
				quoted[i] = table.indexColumn(strings.TrimSpace(col))
			}
			clauses = append(clauses, fmt.Sprintf("ADD UNIQUE KEY %s (%s)",
				escapeMySQLName(name), strings.Join(quoted, ",")))
			log.Printf("warning: table %s 补唯一键 %s (%s)：线上若已有重复行，这条 ALTER 会失败，"+
				"需先人工去重再重试（fail-closed，不会静默跳过）",
				table.tableName, name, strings.Join(quoted, ","))
		}
	}
	return clauses, nil
}

// tableHasPrimaryKey 检查线上表当前是否已存在主键约束。
// 用 TABLE_CONSTRAINTS 判定，不看具体列——本包只补"从无到有"的主键，
// 不做主键列的比对/改写（那需要 DROP+ADD，属破坏性操作）。
func (p *DB) tableHasPrimaryKey(tableName string) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_TYPE = 'PRIMARY KEY'
	`
	var count int
	if err := p.DB.QueryRowContext(p.context(), query, p.DBName, tableName).Scan(&count); err != nil {
		return false, fmt.Errorf("query primary key constraint for table %s: %w", tableName, err)
	}
	return count > 0, nil
}

// IsTableExists 检查表是否存在
func (p *DB) IsTableExists(tableName string) (bool, error) {
	p.tableExistsMu.RLock()
	if exists, ok := p.tableExistsCache[tableName]; ok {
		p.tableExistsMu.RUnlock()
		return exists, nil
	}
	p.tableExistsMu.RUnlock()

	query := `
		SELECT COUNT(*) 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`
	var count int
	err := p.DB.QueryRowContext(p.context(), query, p.DBName, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("query table %s exists: %w", tableName, err)
	}
	exists := count > 0

	p.tableExistsMu.Lock()
	p.tableExistsCache[tableName] = exists
	p.tableExistsMu.Unlock()

	return exists, nil
}

// updateTableExistsCache 更新表存在缓存
func (p *DB) updateTableExistsCache(tableName string, exists bool) {
	p.tableExistsMu.Lock()
	p.tableExistsCache[tableName] = exists
	p.tableExistsMu.Unlock()
}

// GetInsertSQLWithArgs 生成参数化的INSERT语句
func (m *MessageTable) GetInsertSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	if err := m.validateMessageDescriptor(message); err != nil {
		return nil, err
	}

	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		val, err := pbconv.SerializeFieldValue(message, fieldDesc)
		if err != nil {
			return nil, fmt.Errorf("serialize field %s: %w", fieldDesc.Name(), err)
		}
		args = append(args, val)
	}
	return &SqlWithArgs{Sql: m.insertSQLTemplate, Args: args}, nil
}

// GetBatchInsertSQLWithArgs 生成批量INSERT语句
func (m *MessageTable) GetBatchInsertSQLWithArgs(messages []proto.Message) (*SqlWithArgs, error) {
	if len(messages) == 0 {
		return nil, errors.New("no messages to insert")
	}
	if len(messages) > BatchInsertMaxSize {
		return nil, ErrBatchSizeExceeded
	}

	for _, msg := range messages {
		if err := m.validateMessageDescriptor(msg); err != nil {
			return nil, err
		}
	}

	var allArgs []interface{}
	var valueGroups []string
	fieldCount := m.Descriptor.Fields().Len()

	for _, msg := range messages {
		args := make([]interface{}, 0, fieldCount)
		for i := 0; i < fieldCount; i++ {
			fieldDesc := m.Descriptor.Fields().Get(i)
			val, err := pbconv.SerializeFieldValue(msg, fieldDesc)
			if err != nil {
				return nil, fmt.Errorf("serialize field %s: %w", fieldDesc.Name(), err)
			}
			args = append(args, val)
		}
		allArgs = append(allArgs, args...)
		valueGroups = append(valueGroups, buildPlaceholders(fieldCount))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		escapeMySQLName(m.tableName),
		m.fieldsListSQL,
		strings.Join(valueGroups, "), ("))
	return &SqlWithArgs{Sql: sql, Args: allArgs}, nil
}

// GetBatchReplaceSQLWithArgs 生成批量REPLACE语句
func (m *MessageTable) GetBatchReplaceSQLWithArgs(messages []proto.Message) (*SqlWithArgs, error) {
	insertSQL, err := m.GetBatchInsertSQLWithArgs(messages)
	if err != nil {
		return nil, err
	}
	return &SqlWithArgs{
		Sql:  "REPLACE" + strings.TrimPrefix(insertSQL.Sql, "INSERT"),
		Args: insertSQL.Args,
	}, nil
}

// GetInsertOnDupUpdateSQLWithArgs 生成参数化的INSERT...ON DUPLICATE KEY UPDATE语句
func (m *MessageTable) GetInsertOnDupUpdateSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	insertSQL, err := m.GetInsertSQLWithArgs(message)
	if insertSQL == nil || err != nil {
		return nil, err
	}

	var updateClauses []string
	var updateArgs []interface{}
	reflection := message.ProtoReflect()

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		if !reflection.Has(fieldDesc) {
			continue
		}
		val, err := pbconv.SerializeFieldValue(message, fieldDesc)
		if err != nil {
			return nil, fmt.Errorf("serialize update field %s: %w", fieldDesc.Name(), err)
		}
		updateClauses = append(updateClauses, fmt.Sprintf("%s = ?", escapeMySQLName(string(fieldDesc.Name()))))
		updateArgs = append(updateArgs, val)
	}

	if len(updateClauses) == 0 {
		return insertSQL, nil
	}

	updateClauseStr := strings.Join(updateClauses, ", ")
	fullSQL := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSQL.Sql, updateClauseStr)
	fullArgs := append(insertSQL.Args, updateArgs...)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}, nil
}

// GetInsertOnDupKeyForPrimaryKeyWithArgs 生成参数化的INSERT...更新主键语句
func (m *MessageTable) GetInsertOnDupKeyForPrimaryKeyWithArgs(message proto.Message) (*SqlWithArgs, error) {
	if m.primaryKeyField == nil {
		return nil, ErrPrimaryKeyNotFound
	}

	insertSQL, err := m.GetInsertSQLWithArgs(message)
	if insertSQL == nil || err != nil {
		return nil, err
	}

	primaryKeyName := string(m.primaryKeyField.Name())
	primaryKeyValue, err := pbconv.SerializeFieldValue(message, m.primaryKeyField)
	if err != nil {
		return nil, fmt.Errorf("serialize primary key: %w", err)
	}
	updateClause := fmt.Sprintf("%s = ?", escapeMySQLName(primaryKeyName))
	fullSQL := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSQL.Sql, updateClause)
	fullArgs := append(insertSQL.Args, primaryKeyValue)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}, nil
}

// Insert 执行参数化的INSERT操作（直接用DB，无Tx）
func (p *DB) Insert(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs, err := table.GetInsertSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate insert SQL for table %s: %w", tableName, err)
	}

	_, err = p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec insert for table %s: sql=%s, args=%v, err=%w",
			tableName, sqlWithArgs.Sql, sqlWithArgs.Args, wrapExecErr(err))
	}
	return nil
}

// BatchInsert 执行批量INSERT操作（直接用DB，无Tx）
func (p *DB) BatchInsert(messages []proto.Message) error {
	if len(messages) == 0 {
		return errors.New("no messages to insert")
	}

	// 分批处理大批量数据
	for i := 0; i < len(messages); i += BatchInsertMaxSize {
		end := i + BatchInsertMaxSize
		if end > len(messages) {
			end = len(messages)
		}
		batch := messages[i:end]

		tableName := GetTableName(batch[0])
		table, ok := p.Tables[tableName]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
		}

		sqlWithArgs, err := table.GetBatchInsertSQLWithArgs(batch)
		if sqlWithArgs == nil || err != nil {
			return fmt.Errorf("generate batch insert SQL for table %s: %w", tableName, err)
		}

		_, err = p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
		if err != nil {
			return fmt.Errorf("exec batch insert for table %s: sql=%s, args len=%d, err=%w",
				tableName, sqlWithArgs.Sql, len(sqlWithArgs.Args), wrapExecErr(err))
		}
	}

	return nil
}

// InsertIgnore 幂等插入（INSERT IGNORE）：主键/唯一键冲突时跳过不报错，
// 补数据/防重复发奖常用。返回是否实际插入了新行。
func (p *DB) InsertIgnore(message proto.Message) (bool, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return false, err
	}

	insertSQL, err := table.GetInsertSQLWithArgs(message)
	if insertSQL == nil || err != nil {
		return false, fmt.Errorf("generate insert SQL for table %s: %w", table.tableName, err)
	}

	sqlStmt := "INSERT IGNORE" + strings.TrimPrefix(insertSQL.Sql, "INSERT")
	result, err := p.conn().Exec(sqlStmt, insertSQL.Args...)
	if err != nil {
		return false, fmt.Errorf("exec insert ignore for table %s: %w", table.tableName, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

// InsertReturningID 插入并返回自增主键ID（LAST_INSERT_ID），自增主键表建议用此接口
func (p *DB) InsertReturningID(message proto.Message) (int64, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return 0, err
	}

	sqlWithArgs, err := table.GetInsertSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return 0, fmt.Errorf("generate insert SQL for table %s: %w", table.tableName, err)
	}

	result, err := p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return 0, fmt.Errorf("exec insert for table %s: %w", table.tableName, wrapExecErr(err))
	}
	return result.LastInsertId()
}

// InsertOnDupUpdate 执行参数化的INSERT...ON DUPLICATE KEY UPDATE操作（直接用DB，无Tx）
func (p *DB) InsertOnDupUpdate(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs, err := table.GetInsertOnDupUpdateSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate insert on dup update SQL for table %s: %w", tableName, err)
	}

	_, err = p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec insert on dup update for table %s: sql=%s, args=%v, err=%w",
			tableName, sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}
	p.invalidateMessages(table, message)
	return nil
}

// GetSelectSQLByKVWithArgs 生成参数化的KV查询语句
func (m *MessageTable) GetSelectSQLByKVWithArgs(whereKey, whereVal string) (*SqlWithArgs, error) {
	if _, ok := m.fieldNameToDesc[whereKey]; !ok {
		return nil, fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, whereKey, m.tableName)
	}
	sql := fmt.Sprintf("%s WHERE %s = ?;", m.selectFieldsSQL, escapeMySQLName(whereKey))
	return &SqlWithArgs{Sql: sql, Args: []interface{}{whereVal}}, nil
}

// GetSelectSQLByWhereWithArgs 生成参数化的自定义WHERE查询语句
func (m *MessageTable) GetSelectSQLByWhereWithArgs(whereClause string, whereArgs []interface{}) *SqlWithArgs {
	sql := fmt.Sprintf("%s WHERE %s;", m.selectFieldsSQL, whereClause)
	return &SqlWithArgs{Sql: sql, Args: whereArgs}
}

// GetSelectSQL 合并版：生成查询语句
func (m *MessageTable) GetSelectSQL(includeSemicolon bool) string {
	if includeSemicolon {
		return m.selectAllSQLWithSemicolon
	}
	return m.selectAllSQLWithoutSemicolon
}

// GetDeleteSQLWithArgs 生成参数化的按主键删除语句
func (m *MessageTable) GetDeleteSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	whereClause, whereArgs, err := m.primaryKeyWhere(message)
	if err != nil {
		return nil, err
	}
	return &SqlWithArgs{
		Sql:  fmt.Sprintf("DELETE FROM %s WHERE %s", escapeMySQLName(m.tableName), whereClause),
		Args: whereArgs,
	}, nil
}

// GetDeleteSQLByWhereWithArgs 生成参数化的自定义WHERE删除语句
func (m *MessageTable) GetDeleteSQLByWhereWithArgs(whereClause string, whereArgs []interface{}) *SqlWithArgs {
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s", escapeMySQLName(m.tableName), whereClause)
	return &SqlWithArgs{Sql: sql, Args: whereArgs}
}

// Delete 执行参数化的按主键删除操作（直接用DB，无Tx）
func (p *DB) Delete(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs, err := table.GetDeleteSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate delete SQL for table %s: %w", tableName, err)
	}

	_, err = p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec delete for table %s: sql=%s, args=%v, err=%w",
			tableName, sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}
	p.invalidateMessages(table, message)
	return nil
}

// DeleteByWhereWithArgs 执行参数化的自定义WHERE删除操作
func (p *DB) DeleteByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	sqlWithArgs := table.GetDeleteSQLByWhereWithArgs(whereClause, whereArgs)
	if _, err := p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...); err != nil {
		return fmt.Errorf("exec delete by where for table %s: %w", table.tableName, err)
	}
	return nil
}

// DeleteByKV 按单个字段等值条件删除
func (p *DB) DeleteByKV(message proto.Message, key string, value interface{}) error {
	return p.DeleteByWhereWithArgs(message, escapeMySQLName(key)+" = ?", []interface{}{value})
}

// BatchDelete 按主键批量删除（DELETE ... WHERE pk IN (...)，自动分批）
func (p *DB) BatchDelete(messages []proto.Message) error {
	if len(messages) == 0 {
		return nil
	}

	table, err := p.tableForMessage(messages[0])
	if err != nil {
		return err
	}
	if table.primaryKeyField == nil {
		return ErrPrimaryKeyNotFound
	}
	for _, msg := range messages {
		if err := table.validateMessageDescriptor(msg); err != nil {
			return err
		}
	}

	pkNames := make([]string, len(table.primaryKey))
	for i, primaryKey := range table.primaryKey {
		pkNames[i] = escapeMySQLName(primaryKey)
	}

	for i := 0; i < len(messages); i += BatchInsertMaxSize {
		end := i + BatchInsertMaxSize
		if end > len(messages) {
			end = len(messages)
		}
		batch := messages[i:end]

		var args []interface{}
		tuples := make([]string, 0, len(batch))
		for _, msg := range batch {
			values, err := table.primaryKeyValues(msg)
			if err != nil {
				return err
			}
			args = append(args, values...)
			tuples = append(tuples, "("+buildPlaceholders(len(table.primaryKey))+")")
		}

		where := fmt.Sprintf("(%s) IN (%s)", strings.Join(pkNames, ", "), strings.Join(tuples, ", "))
		if err := p.DeleteByWhereWithArgs(messages[0], where, args); err != nil {
			return err
		}
	}
	p.invalidateMessages(table, messages...)
	return nil
}

// Update 按主键更新消息中已设置的字段（UPDATE ... WHERE pk = ?）
func (p *DB) Update(message proto.Message) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	sqlWithArgs, err := table.GetUpdateSQLWithArgs(message)
	if err != nil {
		return fmt.Errorf("generate update SQL for table %s: %w", table.tableName, err)
	}
	if _, err := p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...); err != nil {
		return fmt.Errorf("exec update for table %s: %w", table.tableName, err)
	}
	p.invalidateMessages(table, message)
	return nil
}

// UpdateByWhereWithArgs 按自定义WHERE条件更新消息中已设置的字段
func (p *DB) UpdateByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	sqlWithArgs, err := table.GetUpdateSQLByWhereWithArgs(message, whereClause, whereArgs)
	if err != nil {
		return fmt.Errorf("generate update SQL for table %s: %w", table.tableName, err)
	}
	if _, err := p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...); err != nil {
		return fmt.Errorf("exec update by where for table %s: %w", table.tableName, err)
	}
	return nil
}

// UpdateFieldsByPK 按主键只更新指定字段（部分更新），避免Update全字段覆盖
// 冲掉其他地方的并发写入（如改名操作把别处刚加的金币覆盖回去）
func (p *DB) UpdateFieldsByPK(message proto.Message, fields ...string) error {
	if len(fields) == 0 {
		return errors.New("no fields to update")
	}
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	clauses := make([]string, 0, len(fields))
	args := make([]interface{}, 0, len(fields))
	for _, field := range fields {
		desc, ok := table.fieldNameToDesc[field]
		if !ok {
			return fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, field, table.tableName)
		}
		val, err := pbconv.SerializeFieldValue(message, desc)
		if err != nil {
			return fmt.Errorf("serialize update field %s: %w", field, err)
		}
		clauses = append(clauses, escapeMySQLName(field)+" = ?")
		args = append(args, val)
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}

	sqlStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		escapeMySQLName(table.tableName), strings.Join(clauses, ", "), whereClause)
	if _, err := p.conn().Exec(sqlStmt, append(args, whereArgs...)...); err != nil {
		return fmt.Errorf("exec update fields for table %s: %w", table.tableName, err)
	}
	p.invalidateMessages(table, message)
	return nil
}

// UpdateKVByPK 按主键设置单个字段的值（如改状态、封号）
func (p *DB) UpdateKVByPK(message proto.Message, field string, value interface{}) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}
	if _, ok := table.fieldNameToDesc[field]; !ok {
		return fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, field, table.tableName)
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}

	sqlStmt := fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s",
		escapeMySQLName(table.tableName), escapeMySQLName(field), whereClause)
	if _, err := p.conn().Exec(sqlStmt, append([]interface{}{value}, whereArgs...)...); err != nil {
		return fmt.Errorf("exec update kv for table %s: %w", table.tableName, err)
	}
	p.invalidateMessages(table, message)
	return nil
}

// UpdateIfVersion 乐观锁CAS更新：按主键更新消息中已设置的字段（versionField自动+1），
// 仅当数据库中versionField等于message当前值时生效。返回false表示版本冲突（被其他
// 写入抢先），调用方应重读后重试。不想用行锁时的轻量并发控制。
func (p *DB) UpdateIfVersion(message proto.Message, versionField string) (bool, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return false, err
	}
	versionDesc, ok := table.fieldNameToDesc[versionField]
	if !ok {
		return false, fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, versionField, table.tableName)
	}

	curVersion, err := pbconv.SerializeFieldValue(message, versionDesc)
	if err != nil {
		return false, fmt.Errorf("serialize version field %s: %w", versionField, err)
	}

	pkSet := make(map[string]bool, len(table.primaryKey))
	for _, pk := range table.primaryKey {
		pkSet[pk] = true
	}

	reflection := message.ProtoReflect()
	var clauses []string
	var args []interface{}
	for i := 0; i < table.Descriptor.Fields().Len(); i++ {
		field := table.Descriptor.Fields().Get(i)
		name := string(field.Name())
		if name == versionField || pkSet[name] || !reflection.Has(field) {
			continue
		}
		val, err := pbconv.SerializeFieldValue(message, field)
		if err != nil {
			return false, fmt.Errorf("serialize update field %s: %w", name, err)
		}
		clauses = append(clauses, escapeMySQLName(name)+" = ?")
		args = append(args, val)
	}
	if len(clauses) == 0 {
		return false, errors.New("no fields to update")
	}

	escapedVersion := escapeMySQLName(versionField)
	clauses = append(clauses, fmt.Sprintf("%s = %s + 1", escapedVersion, escapedVersion))

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return false, err
	}

	sqlStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s AND %s = ?",
		escapeMySQLName(table.tableName), strings.Join(clauses, ", "), whereClause, escapedVersion)
	args = append(args, whereArgs...)
	args = append(args, curVersion)

	result, err := p.conn().Exec(sqlStmt, args...)
	if err != nil {
		return false, fmt.Errorf("exec update if version for table %s: %w", table.tableName, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected > 0 {
		p.invalidateMessages(table, message)
	}
	return affected > 0, nil
}

// UpdateFieldsIfVersion 乐观锁CAS+显式字段列表：
//
//	UPDATE t SET f1=?,..., ver=ver+1 WHERE pk=? AND ver=?
//
// 与UpdateIfVersion的区别：不用Has()自动挑字段，显式列出要写的列，
// 规避proto3隐式presence下零值字段（空bytes/0/""）被跳过的坑。
// 返回false=版本冲突，调用方重读重试。
func (p *DB) UpdateFieldsIfVersion(message proto.Message, versionField string, fields ...string) (bool, error) {
	if len(fields) == 0 {
		return false, errors.New("no fields to update")
	}
	table, err := p.tableForMessage(message)
	if err != nil {
		return false, err
	}
	versionDesc, ok := table.fieldNameToDesc[versionField]
	if !ok {
		return false, fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, versionField, table.tableName)
	}
	curVersion, err := pbconv.SerializeFieldValue(message, versionDesc)
	if err != nil {
		return false, fmt.Errorf("serialize version field %s: %w", versionField, err)
	}

	clauses := make([]string, 0, len(fields)+1)
	args := make([]interface{}, 0, len(fields)+2)
	for _, name := range fields {
		if name == versionField {
			continue // version 由下面统一 +1
		}
		desc, ok := table.fieldNameToDesc[name]
		if !ok {
			return false, fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, name, table.tableName)
		}
		val, err := pbconv.SerializeFieldValue(message, desc)
		if err != nil {
			return false, fmt.Errorf("serialize update field %s: %w", name, err)
		}
		clauses = append(clauses, escapeMySQLName(name)+" = ?")
		args = append(args, val)
	}
	escapedVersion := escapeMySQLName(versionField)
	clauses = append(clauses, fmt.Sprintf("%s = %s + 1", escapedVersion, escapedVersion))

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return false, err
	}
	sqlStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s AND %s = ?",
		escapeMySQLName(table.tableName), strings.Join(clauses, ", "), whereClause, escapedVersion)
	args = append(args, whereArgs...)
	args = append(args, curVersion)

	result, err := p.conn().Exec(sqlStmt, args...)
	if err != nil {
		return false, fmt.Errorf("exec update fields if version for table %s: %w", table.tableName, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected > 0 {
		p.invalidateMessages(table, message)
	}
	return affected > 0, nil
}

// valuesUpdateClause 生成 `col = VALUES(col)` 列表，覆盖本进程认识的**全部**列。
//
// ON DUPLICATE KEY UPDATE 只动子句里点名的列，本进程不认识的列原样保留——
// 这正是它比 REPLACE 安全的地方。
//
// 注意：VALUES() 在 MySQL 8.0.20 起被标记 deprecated（官方建议改 AS new 行别名），
// 至今仍可用，本库沿用它以兼容 5.7（与 sqlbuilder 里的 SetNew 等一致）。
func (m *MessageTable) valuesUpdateClause() string {
	parts := make([]string, 0, m.Descriptor.Fields().Len())
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		name := escapeMySQLName(string(m.Descriptor.Fields().Get(i).Name()))
		parts = append(parts, name+" = VALUES("+name+")")
	}
	return strings.Join(parts, ", ")
}

// GetSaveSQLWithArgs 整行落库：INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col), ...
//
// 取代 REPLACE INTO 作为 Save 的实现——同样是「有则更新、无则插入」，
// 但**不会清掉本进程不认识的列**。
func (m *MessageTable) GetSaveSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	stmt, err := m.GetInsertSQLWithArgs(message)
	if err != nil {
		return nil, err
	}
	stmt.Sql += " ON DUPLICATE KEY UPDATE " + m.valuesUpdateClause()
	return stmt, nil
}

// GetBatchSaveSQLWithArgs 批量整行落库，语义同 GetSaveSQLWithArgs。
func (m *MessageTable) GetBatchSaveSQLWithArgs(messages []proto.Message) (*SqlWithArgs, error) {
	stmt, err := m.GetBatchInsertSQLWithArgs(messages)
	if err != nil {
		return nil, err
	}
	stmt.Sql += " ON DUPLICATE KEY UPDATE " + m.valuesUpdateClause()
	return stmt, nil
}

// GetReplaceSQLWithArgs 生成参数化的REPLACE语句。
//
// ⚠️ REPLACE 的语义是「先 DELETE 再 INSERT」，语句里没提到的列不是"保持原值"，
// 是**回到列默认值**。而列清单来自本进程的 descriptor，所以滚动发布时旧版本进程
// 执行一次，新版本刚写进去的列就没了；本库「永不 DROP COLUMN」的保护在这里帮不上忙，
// 因为丢的是数据不是列。还会触发外键级联删除。
//
// DB.Save 已经改走 GetSaveSQLWithArgs（ON DUPLICATE KEY UPDATE，只覆盖本进程认识的列）。
// 本方法保留为**显式逃生口**：确实需要「整行推倒重来、未提及列一律归位」时才用。
func (m *MessageTable) GetReplaceSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	if err := m.validateMessageDescriptor(message); err != nil {
		return nil, err
	}

	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		val, err := pbconv.SerializeFieldValue(message, fieldDesc)
		if err != nil {
			return nil, fmt.Errorf("serialize field %s: %w", fieldDesc.Name(), err)
		}
		args = append(args, val)
	}

	placeholders := buildPlaceholders(len(args))
	sqlStmt := fmt.Sprintf("%s%s)", m.replaceSQLPrefix, placeholders)

	return &SqlWithArgs{Sql: sqlStmt, Args: args}, nil
}

// GetUpdateSetWithArgs 生成参数化的SET子句和参数（仅包含已设置的字段）
func (m *MessageTable) GetUpdateSetWithArgs(message proto.Message) (string, []interface{}, error) {
	if err := m.validateMessageDescriptor(message); err != nil {
		return "", nil, err
	}

	reflection := message.ProtoReflect()
	var clauses []string
	var args []interface{}

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		if !reflection.Has(field) {
			continue
		}

		val, err := pbconv.SerializeFieldValue(message, field)
		if err != nil {
			return "", nil, fmt.Errorf("serialize update field %s: %w", field.Name(), err)
		}

		clauses = append(clauses, escapeMySQLName(string(field.Name()))+" = ?")
		args = append(args, val)
	}

	return strings.Join(clauses, ", "), args, nil
}

// GetUpdateSQLWithArgs 生成参数化的按主键更新语句
func (m *MessageTable) GetUpdateSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	setClause, setArgs, err := m.GetUpdateSetWithArgs(message)
	if err != nil {
		return nil, err
	}
	if setClause == "" {
		return nil, errors.New("no fields to update")
	}

	whereClause, whereArgs, err := m.primaryKeyWhere(message)
	if err != nil {
		return nil, err
	}

	fullSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s", escapeMySQLName(m.tableName), setClause, whereClause)
	return &SqlWithArgs{Sql: fullSQL, Args: append(setArgs, whereArgs...)}, nil
}

// GetUpdateSQLByWhereWithArgs 生成参数化的自定义WHERE更新语句
func (m *MessageTable) GetUpdateSQLByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) (*SqlWithArgs, error) {
	setClause, setArgs, err := m.GetUpdateSetWithArgs(message)
	if err != nil {
		return nil, err
	}
	if setClause == "" {
		return nil, errors.New("no fields to update")
	}

	fullSQL := fmt.Sprintf("UPDATE %s SET %s WHERE %s", escapeMySQLName(m.tableName), setClause, whereClause)
	fullArgs := append(setArgs, whereArgs...)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}, nil
}

// Init 预生成MessageTable的SQL片段（注册表时调用一次）
func (m *MessageTable) Init() {
	desc := m.Descriptor
	fieldCount := desc.Fields().Len()

	m.fieldNameToDesc = make(map[string]protoreflect.FieldDescriptor, fieldCount)
	names := make([]string, 0, fieldCount)
	for i := 0; i < fieldCount; i++ {
		field := desc.Fields().Get(i)
		fieldName := string(field.Name())
		m.fieldNameToDesc[fieldName] = field
		names = append(names, escapeMySQLName(fieldName))
	}
	m.fieldsListSQL = strings.Join(names, ", ")

	escapedTable := escapeMySQLName(m.tableName)
	m.selectFieldsSQL = "SELECT " + m.fieldsListSQL + " FROM " + escapedTable
	m.selectAllSQLWithSemicolon = m.selectFieldsSQL + ";"
	m.selectAllSQLWithoutSemicolon = m.selectFieldsSQL + " "
	m.insertSQLTemplate = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		escapedTable, m.fieldsListSQL, buildPlaceholders(fieldCount))
	m.replaceSQLPrefix = "REPLACE INTO " + escapedTable + " (" + m.fieldsListSQL + ") VALUES ("

	if len(m.primaryKey) > 0 {
		m.primaryKeyField = desc.Fields().ByName(protoreflect.Name(m.primaryKey[0]))
	}

	m.fieldNumbers = make(map[int32]struct{}, fieldCount)
	for i := 0; i < fieldCount; i++ {
		m.fieldNumbers[int32(desc.Fields().Get(i).Number())] = struct{}{}
	}

	if m.tableName == string(desc.FullName()) {
		// 没有 table_name 选项，表名退化成 proto full name（含 package）。
		//
		// 这在 package 一改就出事：v2 把 package 从 game.v1 改成 game.v2，表名跟着变
		// → 建出一张**全新的空表**，v1 的数据留在旧表里，而**两边都不报错**——
		// 服务照常起来，玩家数据"凭空消失"。
		//
		// RegisterAllTables 强制要求 table_name（走那条路是安全的），
		// 但手工 RegisterTable / newMessageTable 不受保护，所以这里告警。
		log.Printf("warning: 表 %s 没有声明 table_name 选项，表名退化为 proto full name。"+
			"proto 的 package 一改表名就跟着变，会建出一张空表而旧数据留在旧表里，"+
			"且两边都不报错。建议在 .proto 里显式写 option (proto2mysql.table_name)",
			desc.FullName())
	}
}

// NewDB 创建新的数据库实例
func NewDB() *DB {
	return &DB{
		Tables:           make(map[string]*MessageTable),
		tableExistsCache: make(map[string]bool),
	}
}

// GetTableName 获取Protobuf对应的表名
func GetTableName(m proto.Message) string {
	return string(m.ProtoReflect().Descriptor().FullName())
}

// GetDescriptor 获取Protobuf的消息描述符
func GetDescriptor(m proto.Message) protoreflect.MessageDescriptor {
	return m.ProtoReflect().Descriptor()
}

// GetCreateTableSQL 获取创建表的SQL（对外接口）
func (p *DB) GetCreateTableSQL(message proto.Message) string {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return ""
	}
	return table.GetCreateTableSQL()
}

// Save 整行落库：有则更新、无则插入（直接用DB，无Tx）。
//
// 走 INSERT ... ON DUPLICATE KEY UPDATE，**不是** REPLACE INTO。REPLACE 的语义是
// 「先 DELETE 再 INSERT」，语句里没提到的列会**回到默认值**——而列清单来自本进程的
// descriptor，所以滚动发布时旧版本进程 Save 一次，新版本刚写进去的列就没了，且零报错。
// ODKU 只动子句里点名的列，本进程不认识的列原样保留。
//
// 需要「整行推倒重来」的旧语义时，显式用 GetReplaceSQLWithArgs。
// （顺带：GormDB.Save 一直用的就是 clause.OnConflict{UpdateAll}，本次改动让两者语义一致。）
func (p *DB) Save(message proto.Message) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs, err := table.GetSaveSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate save SQL for table %s: %w", tableName, err)
	}

	_, err = p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec save for table %s: sql=%s, args=%v, err=%w",
			tableName, sqlWithArgs.Sql, sqlWithArgs.Args, err)
	}
	p.invalidateMessages(table, message)
	return nil
}

// BatchSave 批量整行落库（自动分批），语义同 Save。
func (p *DB) BatchSave(messages []proto.Message) error {
	if len(messages) == 0 {
		return nil
	}

	table, err := p.tableForMessage(messages[0])
	if err != nil {
		return err
	}
	for _, msg := range messages {
		if err := table.validateMessageDescriptor(msg); err != nil {
			return err
		}
	}

	for i := 0; i < len(messages); i += BatchInsertMaxSize {
		end := i + BatchInsertMaxSize
		if end > len(messages) {
			end = len(messages)
		}

		sqlWithArgs, err := table.GetBatchSaveSQLWithArgs(messages[i:end])
		if err != nil {
			return fmt.Errorf("generate batch save SQL for table %s: %w", table.tableName, err)
		}
		if _, err := p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...); err != nil {
			return fmt.Errorf("exec batch replace for table %s: args len=%d, err=%w",
				table.tableName, len(sqlWithArgs.Args), err)
		}
	}
	p.invalidateMessages(table, messages...)
	return nil
}

// FindOneByPK 按消息中的主键值查询单条数据（查到后覆盖message其余字段）。
// 启用缓存时为cache-aside读路径：先查缓存，未命中读DB后回填。
func (p *DB) FindOneByPK(message proto.Message) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	// 事务内不走缓存（需要读到事务内未提交的最新值）
	useCache := p.cacheEnabled() && p.tx == nil
	if useCache && p.cacheGetProto(table, message) {
		return nil
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}
	if err := p.FindOneByWhereWithArgs(message, whereClause, whereArgs); err != nil {
		return err
	}

	if useCache {
		p.cacheSetProto(table, message)
	}
	return nil
}

// FindOneByPKForUpdate 按主键查询并加行锁（SELECT ... FOR UPDATE），
// 仅在RunInTransaction内有意义，用于防止并发修改同一玩家数据
func (p *DB) FindOneByPKForUpdate(message proto.Message) error {
	if p.tx == nil {
		return errors.New("FindOneByPKForUpdate must be called inside RunInTransaction")
	}

	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}

	sqlStmt := fmt.Sprintf("%s WHERE %s FOR UPDATE;", table.selectFieldsSQL, whereClause)
	rows, err := p.conn().Query(sqlStmt, whereArgs...)
	if err != nil {
		return fmt.Errorf("exec select for update for table %s: %w", table.tableName, err)
	}
	defer rows.Close()

	if err := scanOneProtoRow(rows, message); err != nil {
		return fmt.Errorf("table %s: %w", table.tableName, err)
	}
	return nil
}

// FindOrCreate 按主键查询，不存在则用message当前值插入（玩家首次登录常用）。
// 返回created表示是否新建了记录。
func (p *DB) FindOrCreate(message proto.Message) (created bool, err error) {
	err = p.FindOneByPK(message)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, ErrNoRowsFound) {
		return false, err
	}

	if err := p.Insert(message); err != nil {
		return false, err
	}
	return true, nil
}

// FindAllByPKIn 按主键批量查询，返回列表（类似Redis MGET：给一批主键，返回命中的行，
// 不存在的主键自动跳过）
func (p *DB) FindAllByPKIn(list proto.Message, pkValues []interface{}) error {
	table, listField, err := resolveListTable(p.Tables, list)
	if err != nil {
		return err
	}

	if len(pkValues) == 0 {
		list.ProtoReflect().Mutable(listField).List().Truncate(0)
		return nil
	}
	if table.primaryKeyField == nil {
		return ErrPrimaryKeyNotFound
	}

	pkName := escapeMySQLName(string(table.primaryKeyField.Name()))
	where := fmt.Sprintf("%s IN (%s)", pkName, buildPlaceholders(len(pkValues)))
	return p.FindAllByWhereWithArgs(list, where, pkValues)
}

// IncrByPK 按主键对数值字段原子加减（UPDATE ... SET f = f + delta），
// 适合货币/经验等计数器，避免“读-改-写”竞态
func (p *DB) IncrByPK(message proto.Message, field string, delta int64) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}
	if _, ok := table.fieldNameToDesc[field]; !ok {
		return fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, field, table.tableName)
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}

	escapedField := escapeMySQLName(field)
	sqlStmt := fmt.Sprintf("UPDATE %s SET %s = %s + ? WHERE %s",
		escapeMySQLName(table.tableName), escapedField, escapedField, whereClause)
	if _, err := p.conn().Exec(sqlStmt, append([]interface{}{delta}, whereArgs...)...); err != nil {
		return fmt.Errorf("exec incr for table %s: %w", table.tableName, err)
	}
	p.invalidateMessages(table, message)
	return nil
}

// DecrByPKIfEnough 按主键原子扣减数值字段，余额不足时不扣并返回false
// （UPDATE ... SET f = f - ? WHERE pk = ? AND f >= ?，防止负数余额，扣钱/扣道具常用）
func (p *DB) DecrByPKIfEnough(message proto.Message, field string, delta int64) (bool, error) {
	if delta < 0 {
		return false, fmt.Errorf("delta must be non-negative, got %d", delta)
	}

	table, err := p.tableForMessage(message)
	if err != nil {
		return false, err
	}
	if _, ok := table.fieldNameToDesc[field]; !ok {
		return false, fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, field, table.tableName)
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return false, err
	}

	escapedField := escapeMySQLName(field)
	sqlStmt := fmt.Sprintf("UPDATE %s SET %s = %s - ? WHERE %s AND %s >= ?",
		escapeMySQLName(table.tableName), escapedField, escapedField, whereClause, escapedField)
	args := append([]interface{}{delta}, whereArgs...)
	args = append(args, delta)

	result, err := p.conn().Exec(sqlStmt, args...)
	if err != nil {
		return false, fmt.Errorf("exec decr for table %s: %w", table.tableName, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected > 0 {
		p.invalidateMessages(table, message)
	}
	return affected > 0, nil
}

// FindOneByKV 按单个字段等值条件查询单条数据
func (p *DB) FindOneByKV(message proto.Message, whereKey string, whereVal string) error {
	return p.FindOneByWhereWithArgs(message, escapeMySQLName(whereKey)+" = ?", []interface{}{whereVal})
}

// FindOneByWhereWithArgs 执行参数化的自定义WHERE查询（单条数据）
func (p *DB) FindOneByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	sqlWithArgs := table.GetSelectSQLByWhereWithArgs(whereClause, whereArgs)
	rows, err := p.conn().Query(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec select for table %s: %w", tableName, err)
	}
	defer rows.Close()

	if err := scanOneProtoRow(rows, message); err != nil {
		return fmt.Errorf("table %s: %w", tableName, err)
	}
	return nil
}

// FindOneByWhereClause 按条件查询单条数据（whereClause为纯条件，无需带WHERE；空串查全表）
func (p *DB) FindOneByWhereClause(message proto.Message, whereClause string) error {
	return p.FindOneByWhereWithArgs(message, normalizeWhereClause(whereClause), nil)
}

// FindAll 查询全表数据到列表消息（包含单个repeated字段的消息）
func (p *DB) FindAll(message proto.Message) error {
	return p.FindAllByWhereWithArgs(message, "1=1", nil)
}

// FindAllByWhereWithArgs 执行参数化的自定义WHERE查询（批量数据）
func (p *DB) FindAllByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	table, listField, err := resolveListTable(p.Tables, message)
	if err != nil {
		return err
	}

	sqlWithArgs := table.GetSelectSQLByWhereWithArgs(whereClause, whereArgs)
	rows, err := p.conn().Query(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec select all for table %s: %w", table.tableName, err)
	}
	defer rows.Close()

	listValue := message.ProtoReflect().Mutable(listField).List()
	if err := scanProtoRowsToList(rows, listValue); err != nil {
		return fmt.Errorf("table %s: %w", table.tableName, err)
	}
	return nil
}

// FindAllByWhereClause 按条件查询批量数据（whereClause为纯条件，无需带WHERE；空串查全表）
func (p *DB) FindAllByWhereClause(message proto.Message, whereClause string) error {
	return p.FindAllByWhereWithArgs(message, normalizeWhereClause(whereClause), nil)
}

// FindMultiByWhereWithArgs 与FindAllByWhereWithArgs等价，保留以兼容旧接口
func (p *DB) FindMultiByWhereWithArgs(list proto.Message, whereClause string, args []interface{}) error {
	return p.FindAllByWhereWithArgs(list, whereClause, args)
}

// FindMultiByKV 按单个字段等值条件查询批量数据
func (p *DB) FindMultiByKV(list proto.Message, key string, value interface{}) error {
	return p.FindAllByWhereWithArgs(list, escapeMySQLName(key)+" = ?", []interface{}{value})
}

// FindAllByKVIn 按单个字段的IN条件查询批量数据（WHERE key IN (...)）
func (p *DB) FindAllByKVIn(list proto.Message, key string, values []interface{}) error {
	if len(values) == 0 {
		_, listField, err := resolveListTable(p.Tables, list)
		if err != nil {
			return err
		}
		list.ProtoReflect().Mutable(listField).List().Truncate(0)
		return nil
	}

	where := fmt.Sprintf("%s IN (%s)", escapeMySQLName(key), buildPlaceholders(len(values)))
	return p.FindAllByWhereWithArgs(list, where, values)
}

// FindMultiByWhereClause 与FindAllByWhereClause等价，保留以兼容旧接口
func (p *DB) FindMultiByWhereClause(message proto.Message, whereClause string) error {
	return p.FindAllByWhereClause(message, whereClause)
}

// QueryOptions 查询修饰选项，对应MySQL的ORDER BY / LIMIT / OFFSET / FOR UPDATE
type QueryOptions struct {
	OrderBy string // 排序表达式，如 "id DESC"（直接拼入SQL，勿传入不可信输入）
	Limit   int    // 返回行数上限，<=0表示不限制
	Offset  int    // 跳过的行数，仅在Limit>0时生效
	// ForUpdate 追加FOR UPDATE行锁（只在事务内有意义，事务外单句自动提交，锁即刻释放）。
	// 用于“先锁后改”：读到加锁后的最新值，防止并发读-改-写丢更新
	ForUpdate bool
}

// sqlSuffix 生成ORDER BY/LIMIT/OFFSET/FOR UPDATE后缀（以空格开头，可能为空串）
func (o QueryOptions) sqlSuffix() string {
	var b strings.Builder
	if o.OrderBy != "" {
		b.WriteString(" ORDER BY ")
		b.WriteString(o.OrderBy)
	}
	if o.Limit > 0 {
		b.WriteString(" LIMIT ")
		b.WriteString(strconv.Itoa(o.Limit))
		if o.Offset > 0 {
			b.WriteString(" OFFSET ")
			b.WriteString(strconv.Itoa(o.Offset))
		}
	}
	if o.ForUpdate {
		b.WriteString(" FOR UPDATE")
	}
	return b.String()
}

// FindAllWithOptions 按条件查询批量数据，支持ORDER BY / LIMIT / OFFSET
func (p *DB) FindAllWithOptions(list proto.Message, whereClause string, whereArgs []interface{}, opts QueryOptions) error {
	table, listField, err := resolveListTable(p.Tables, list)
	if err != nil {
		return err
	}

	sqlStmt := fmt.Sprintf("%s WHERE %s%s;", table.selectFieldsSQL, normalizeWhereClause(whereClause), opts.sqlSuffix())
	rows, err := p.conn().Query(sqlStmt, whereArgs...)
	if err != nil {
		return fmt.Errorf("exec select for table %s: %w", table.tableName, err)
	}
	defer rows.Close()

	listValue := list.ProtoReflect().Mutable(listField).List()
	if err := scanProtoRowsToList(rows, listValue); err != nil {
		return fmt.Errorf("table %s: %w", table.tableName, err)
	}
	return nil
}

// FindPage 分页查询批量数据（pageIndex从1开始）
func (p *DB) FindPage(list proto.Message, whereClause string, whereArgs []interface{}, pageIndex, pageSize int) error {
	if pageIndex < 1 || pageSize < 1 {
		return fmt.Errorf("invalid page params: pageIndex=%d, pageSize=%d", pageIndex, pageSize)
	}
	return p.FindAllWithOptions(list, whereClause, whereArgs, QueryOptions{
		Limit:  pageSize,
		Offset: (pageIndex - 1) * pageSize,
	})
}

// FindOneWithOptions 按条件+排序取一条数据（如排行第一名、最新一条记录）。
// 自动追加LIMIT 1，多行匹配时取排序后的第一条
func (p *DB) FindOneWithOptions(message proto.Message, whereClause string, whereArgs []interface{}, opts QueryOptions) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	opts.Limit = 1
	opts.Offset = 0
	sqlStmt := fmt.Sprintf("%s WHERE %s%s;", table.selectFieldsSQL, normalizeWhereClause(whereClause), opts.sqlSuffix())
	rows, err := p.conn().Query(sqlStmt, whereArgs...)
	if err != nil {
		return fmt.Errorf("exec select one for table %s: %w", table.tableName, err)
	}
	defer rows.Close()

	if err := scanOneProtoRow(rows, message); err != nil {
		return fmt.Errorf("table %s: %w", table.tableName, err)
	}
	return nil
}

// FindPageByCursor 游标分页（keyset pagination）：按cursorField升序返回cursorVal之后的pageSize条，
// 深分页时性能远好于OFFSET，适合流水/邮件列表。首页传cursorVal=nil，
// 下一页传上一页最后一条的cursorField值。cursorField应有索引且唯一（如自增id）。
func (p *DB) FindPageByCursor(list proto.Message, whereClause string, whereArgs []interface{}, cursorField string, cursorVal interface{}, pageSize int) error {
	if pageSize < 1 {
		return fmt.Errorf("invalid pageSize: %d", pageSize)
	}
	table, _, err := resolveListTable(p.Tables, list)
	if err != nil {
		return err
	}
	if _, ok := table.fieldNameToDesc[cursorField]; !ok {
		return fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, cursorField, table.tableName)
	}

	where := normalizeWhereClause(whereClause)
	args := append([]interface{}{}, whereArgs...)
	if cursorVal != nil {
		where = fmt.Sprintf("(%s) AND %s > ?", where, escapeMySQLName(cursorField))
		args = append(args, cursorVal)
	}

	return p.FindAllWithOptions(list, where, args, QueryOptions{
		OrderBy: escapeMySQLName(cursorField) + " ASC",
		Limit:   pageSize,
	})
}

// Count 统计全表行数（message可为行消息或列表消息）
func (p *DB) Count(message proto.Message) (int64, error) {
	return p.CountByWhereWithArgs(message, "", nil)
}

// CountByWhereWithArgs 按条件统计行数（SELECT COUNT(*)），message可为行消息或列表消息
func (p *DB) CountByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) (int64, error) {
	table, err := resolveAnyTable(p.Tables, message)
	if err != nil {
		return 0, err
	}

	sqlStmt := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s;",
		escapeMySQLName(table.tableName), normalizeWhereClause(whereClause))
	var count int64
	if err := p.conn().QueryRow(sqlStmt, whereArgs...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count table %s: %w", table.tableName, err)
	}
	return count, nil
}

// Exists 判断是否存在满足条件的行（SELECT 1 ... LIMIT 1），message可为行消息或列表消息
func (p *DB) Exists(message proto.Message, whereClause string, whereArgs []interface{}) (bool, error) {
	table, err := resolveAnyTable(p.Tables, message)
	if err != nil {
		return false, err
	}

	sqlStmt := fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1;",
		escapeMySQLName(table.tableName), normalizeWhereClause(whereClause))
	var one int
	err = p.conn().QueryRow(sqlStmt, whereArgs...).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("exists check for table %s: %w", table.tableName, err)
	}
	return true, nil
}

// ExistsByPK 按消息中的主键值判断行是否存在
func (p *DB) ExistsByPK(message proto.Message) (bool, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return false, err
	}
	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return false, err
	}
	return p.Exists(message, whereClause, whereArgs)
}

// Transaction 在事务中执行fn：fn返回错误时回滚，否则提交（需要原生*sql.Tx时使用，
// 否则推荐RunInTransaction）
func (p *DB) Transaction(fn func(tx *sql.Tx) error) error {
	if p.tx != nil {
		return errors.New("nested transaction is not supported")
	}
	tx, err := p.DB.BeginTx(p.context(), nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", err, rbErr)
		}
		return err
	}
	return tx.Commit()
}

// tableForMessage 解析行消息对应的已注册表
func (p *DB) tableForMessage(message proto.Message) (*MessageTable, error) {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}
	return table, nil
}

// resolveAnyTable 解析行消息或列表消息（包含单个repeated字段）对应的已注册表
func resolveAnyTable(tables map[string]*MessageTable, message proto.Message) (*MessageTable, error) {
	tableName := GetTableName(message)
	if table, ok := tables[tableName]; ok {
		return table, nil
	}
	if table, _, err := resolveListTable(tables, message); err == nil {
		return table, nil
	}
	return nil, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
}

// normalizeWhereClause 空条件时返回恒真条件，兼容无条件查询
func normalizeWhereClause(whereClause string) string {
	if whereClause == "" {
		return "1=1"
	}
	return whereClause
}

// resolveListTable 从包含单个repeated字段的列表消息中解析出已注册的表和该字段
func resolveListTable(tables map[string]*MessageTable, list proto.Message) (*MessageTable, protoreflect.FieldDescriptor, error) {
	listField, err := getSingleRepeatedField(list)
	if err != nil {
		return nil, nil, err
	}

	tableName := string(listField.Message().FullName())
	table, ok := tables[tableName]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}
	return table, listField, nil
}

// scanProtoRowsToList 把结果集逐行反序列化并追加到repeated字段（先清空旧数据）
func scanProtoRowsToList(rows *sql.Rows, listValue protoreflect.List) error {
	listValue.Truncate(0)

	for rows.Next() {
		row, err := scanRowStrings(rows)
		if err != nil {
			return err
		}

		element := listValue.NewElement()
		if err := pbconv.ParseFromString(element.Message().Interface(), row); err != nil {
			return err
		}
		listValue.Append(element)
	}
	return rows.Err()
}

func getSingleRepeatedField(list proto.Message) (protoreflect.FieldDescriptor, error) {
	if list == nil {
		return nil, errors.New("list message cannot be nil")
	}

	fields := list.ProtoReflect().Descriptor().Fields()
	var repeatedField protoreflect.FieldDescriptor

	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !fd.IsList() {
			continue
		}
		if repeatedField != nil {
			return nil, ErrMultipleRepeated
		}
		repeatedField = fd
	}

	if repeatedField == nil {
		return nil, ErrNoRepeatedField
	}

	if repeatedField.Message() == nil {
		return nil, fmt.Errorf("repeated field %s is not a message type", repeatedField.Name())
	}

	return repeatedField, nil
}

// GetElementTableName 获取列表消息中repeated元素类型对应的表名
func GetElementTableName(list proto.Message) (string, error) {
	elementField, err := getSingleRepeatedField(list)
	if err != nil {
		return "", err
	}

	return string(elementField.Message().FullName()), nil
}

// MultiQuery 单张表的查询参数
type MultiQuery struct {
	Message     proto.Message // 用于接收查询结果的消息体
	WhereClause string        // 查询条件（如 "id = ?"）
	WhereArgs   []interface{} // 条件中的参数（与?对应）
}

// FindMultiByWhereClauses 一次查询多张无关表，每张表返回一条结果（依赖MultiStatements）
func (p *DB) FindMultiByWhereClauses(queries []MultiQuery) error {
	if len(queries) == 0 {
		return errors.New("no queries provided")
	}

	// 收集每张表的查询SQL（分号分隔）与参数
	sqlParts := make([]string, 0, len(queries))
	var allArgs []interface{}
	for _, q := range queries {
		tableName := GetTableName(q.Message)
		table, ok := p.Tables[tableName]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
		}
		sqlParts = append(sqlParts, table.GetSelectSQL(false)+" WHERE "+q.WhereClause)
		allArgs = append(allArgs, q.WhereArgs...)
	}

	sqlStmt := strings.Join(sqlParts, "; ")
	rows, err := p.DB.QueryContext(p.context(), sqlStmt, allArgs...)
	if err != nil {
		return fmt.Errorf("exec multi select: %w, SQL: %s, args: %v", err, sqlStmt, allArgs)
	}
	defer rows.Close()

	// 依次处理每个结果集（与queries顺序一致）
	for idx, q := range queries {
		if err := scanOneProtoRow(rows, q.Message); err != nil {
			return fmt.Errorf("%w: %s", err, GetTableName(q.Message))
		}
		if idx < len(queries)-1 && !rows.NextResultSet() {
			return fmt.Errorf("missing result set for table %s", GetTableName(queries[idx+1].Message))
		}
	}
	return nil
}

// newMessageTable 构建消息-表映射并预生成SQL片段。
// 先应用proto描述符里声明的表选项（message/field option，见options.go），
// 再应用代码传入的opts（优先级更高，可覆盖proto声明）。
func newMessageTable(m proto.Message, opts ...TableOption) *MessageTable {
	table := &MessageTable{
		tableName:  GetTableName(m),
		Descriptor: GetDescriptor(m),
	}
	for _, opt := range TableOptionsFromDescriptor(table.Descriptor) {
		opt(table)
	}
	for _, opt := range opts {
		opt(table)
	}
	table.Init()
	return table
}

// RegisterTable 注册Protobuf与表的映射关系。
// 表配置（表名/主键/自增/索引/唯一键/可空字段）优先从proto的message option、
// field option中读取（见proto/proto2mysql_option.proto），调用方通常无需传任何TableOption；
// 显式传入的opts可覆盖proto里的声明。
// 注册键固定为proto full name（查找路径统一按消息FullName解析）；
// table.tableName仅决定生成SQL中的表名。
func (p *DB) RegisterTable(m proto.Message, opts ...TableOption) {
	table := newMessageTable(m, opts...)
	p.Tables[GetTableName(m)] = table
}

// RegisterAllTables 扫描全局 proto 注册表（protoregistry.GlobalFiles），
// 自动注册项目中所有“用于建表”的消息，返回被注册的表（按 proto full name）。
//
// 一个消息只有同时满足以下两个条件才会被注册：
//  1. 其所在 .proto 文件声明了文件级选项 option (proto2mysql.db) = true;
//  2. 该 message 自身声明了 option (proto2mysql.table_name) = "...";
//
// 即：db 文件选项圈定“哪些文件参与建表”，table_name 决定“文件里哪些 message 建表”。
// 前提是这些 .proto 生成的 Go 代码已被链接进当前二进制（有 import，触发 init 注册到全局表）。
func (p *DB) RegisterAllTables() []string {
	var registered []string
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if FileHasDBOption(fd) {
			registered = append(registered, p.registerTablesInMessages(fd.Messages())...)
		}
		return true
	})
	return registered
}

// registerTablesInMessages 递归遍历消息（含嵌套消息），注册声明了 table_name 的表。
func (p *DB) registerTablesInMessages(msgs protoreflect.MessageDescriptors) []string {
	var out []string
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)
		if _, ok := TableNameFromDescriptor(md); ok {
			p.registerTableFromDescriptor(md)
			out = append(out, string(md.FullName()))
		}
		out = append(out, p.registerTablesInMessages(md.Messages())...)
	}
	return out
}

// registerTableFromDescriptor 从消息描述符构建并注册表（表配置自动从 message/field option 读取）。
func (p *DB) registerTableFromDescriptor(md protoreflect.MessageDescriptor) {
	table := &MessageTable{
		tableName:  string(md.FullName()),
		Descriptor: md,
	}
	for _, opt := range TableOptionsFromDescriptor(md) {
		opt(table)
	}
	table.Init()
	p.Tables[string(md.FullName())] = table
}

// SyncAllTables 对当前已注册的所有表执行建表/字段对齐：
// 表不存在则创建，存在则对齐字段类型（等价于对每张表调用 UpdateTableField）。
// 常与 RegisterAllTables 搭配：先自动注册，再一次性建/更新全部 MySQL 表。
func (p *DB) SyncAllTables() error {
	acquired := p.acquireSyncLock()
	if acquired {
		defer p.releaseSyncLock()
	}

	// 表名排序后再遍历。Go 的 map 迭代是随机化的，不排序的话多个副本会以不同顺序
	// 抢同一批表的元数据锁，可能互相等待；而且失败时"改到第几张表"每次都不一样，
	// 排障时对不上。
	keys := make([]string, 0, len(p.Tables))
	for key := range p.Tables {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	for _, key := range keys {
		if err := p.syncTableSchema(key, p.Tables[key]); err != nil {
			return err
		}
	}
	return nil
}

const (
	// SyncLockName DDL 咨询锁的名字（同一个库内全局）。
	SyncLockName = "proto2mysql:sync"
	// SyncLockTimeoutSeconds 抢 DDL 咨询锁的等待秒数。
	SyncLockTimeoutSeconds = 30
)

// acquireSyncLock 抢 DDL 咨询锁。拿到返回 true；超时 / 不支持 / 出错都返回 false 并降级。
//
// 没有这把锁时：N 个副本同时冷启动 → 一个 ALTER 成功、其余全部撞
// Error 1060 Duplicate column name → 返回错误 → 启动失败。下一次重启会成功
// （列已经存在，对齐结果为空），所以它是**"自愈式蒙对"**——日志里留下一串启动失败、
// 服务最终起来了，很容易被当成偶发 flake 忽略，直到某次重启风暴把它放大。
//
// 拿不到锁**不阻断**：GET_LOCK 超时返回 0、连接异常返回 NULL，而 TiDB 等兼容实现
// 不一定支持这个函数。所以拿不到只降级为"无锁执行 + 一条告警"——把可用性看得比
// "锁一定要拿到"更重：拿不到锁最坏是撞 1060、重启自愈；因为拿不到锁就拒绝启动，
// 是把一个并发问题升级成可用性事故。
func (p *DB) acquireSyncLock() bool {
	var got sql.NullInt64
	err := p.conn().QueryRow(
		"SELECT GET_LOCK(?, ?)", SyncLockName, SyncLockTimeoutSeconds).Scan(&got)
	if err != nil {
		log.Printf("warning: 拿不到 DDL 咨询锁（%s），本次结构同步无锁执行：%v。"+
			"多副本同时启动时可能撞 Error 1060，重启即可自愈", SyncLockName, err)
		return false
	}
	if !got.Valid || got.Int64 != 1 {
		// 返回 0 = 等超时了（别人正在改）；NULL = 连接出错。两种都只告警不阻断。
		log.Printf("warning: DDL 咨询锁 %s 未取得（valid=%v value=%d），本次结构同步无锁执行",
			SyncLockName, got.Valid, got.Int64)
		return false
	}
	return true
}

func (p *DB) releaseSyncLock() {
	var released sql.NullInt64
	if err := p.conn().QueryRow("SELECT RELEASE_LOCK(?)", SyncLockName).Scan(&released); err != nil {
		// 连接断开时锁会被服务端自动释放，这里失败不影响正确性。
		log.Printf("warning: 释放 DDL 咨询锁 %s 失败（连接断开时会自动释放）：%v", SyncLockName, err)
	}
}

// TableOption 表选项函数
type TableOption func(*MessageTable)

// WithTableName 自定义SQL表名（默认=proto full name）。
// 用于对接已有表/迁移脚本管理的表名（如 player_data）。
// 注意：注册与查找仍按proto full name进行，此选项只影响生成的SQL。
func WithTableName(name string) TableOption {
	return func(t *MessageTable) { t.tableName = name }
}

// WithPrimaryKey 设置主键
func WithPrimaryKey(keys ...string) TableOption {
	return func(t *MessageTable) {
		t.primaryKey = keys
	}
}

// WithIndexes 设置普通索引
func WithIndexes(indexes ...string) TableOption {
	return func(t *MessageTable) {
		t.indexes = indexes
	}
}

// WithUniqueKey 设置唯一键
func WithUniqueKey(uniqueKey string) TableOption {
	return func(t *MessageTable) {
		t.uniqueKeys = uniqueKey
	}
}

// WithAutoIncrementKey 设置自增字段
func WithAutoIncrementKey(key string) TableOption {
	return func(t *MessageTable) {
		t.autoIncreaseKey = key
	}
}

// WithNullableFields 设置允许为NULL的字段
func WithNullableFields(fields ...string) TableOption {
	return func(t *MessageTable) {
		t.nullableFields = fields
	}
}

// WithTiDBNonclusteredPK 主键追加 /*T![clustered_index] NONCLUSTERED */（TiDB 方言，MySQL 忽略）。
// 业务自赋值的单调主键（如 Snowflake）在 TiDB 默认聚簇表下有写热点，须配合
// WithTiDBShardRowIDBits 打散；代价是主键点查多一次索引回表。
func WithTiDBNonclusteredPK() TableOption {
	return func(t *MessageTable) { t.tidbNonclusteredPK = true }
}

// WithTiDBShardRowIDBits 追加 /*T! SHARD_ROW_ID_BITS=bits */（TiDB 方言，MySQL 忽略）。
// 仅对非聚簇主键/无主键表生效：表有主键却未设置 WithTiDBNonclusteredPK 时本选项被忽略并告警
// （TiDB 聚簇表不支持，生成了也必然建表失败）。bits 建议取 log2(TiKV 节点数) 数量级。
func WithTiDBShardRowIDBits(bits uint32) TableOption {
	return func(t *MessageTable) { t.tidbShardRowIDBits = bits }
}

// WithTiDBPreSplitRegions 建表即预切 region（TiDB 方言，MySQL 忽略）。
// 依赖 WithTiDBShardRowIDBits，未设置时本选项被忽略；
// TiDB 要求 n ≤ SHARD_ROW_ID_BITS，超出时收敛到 shard 值并告警。
func WithTiDBPreSplitRegions(n uint32) TableOption {
	return func(t *MessageTable) { t.tidbPreSplitRegions = n }
}

// WithTiDBAutoIDCacheOne 表尾追加 /*T![auto_id_cache] AUTO_ID_CACHE=1 */（TiDB 方言，MySQL 忽略）。
// 自增 ID 走集中分配（v6.4+），近似连续；不设置时 TiDB 默认按批缓存（3 万/批，重启跳号）。
func WithTiDBAutoIDCacheOne() TableOption {
	return func(t *MessageTable) { t.tidbAutoIDCacheOne = true }
}

// Close 关闭数据库连接
func (p *DB) Close() error {
	if p.DB == nil {
		return nil
	}
	return p.DB.Close()
}
