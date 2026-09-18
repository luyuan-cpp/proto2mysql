package proto2mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"regexp"
	"slices"
	"sort"
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

	ErrTableNotFound           = errors.New("table not found")
	ErrNoRepeatedField         = errors.New("message has no repeated field")
	ErrMultipleRepeated        = errors.New("message has multiple repeated fields")
	ErrPrimaryKeyNotFound      = errors.New("primary key not found")
	ErrFieldNotFound           = errors.New("field not found in message")
	ErrMultipleRowsFound       = errors.New("multiple rows found")
	ErrNoRowsFound             = errors.New("no rows found")
	ErrDuplicateKey            = errors.New("duplicate key")
	ErrBatchSizeExceeded       = fmt.Errorf("batch size exceeds maximum %d", BatchInsertMaxSize)
	ErrUnsafeSchemaConversion  = errors.New("unsafe schema conversion requires manual migration")
	ErrSchemaDrift             = errors.New("existing schema does not match the protobuf declaration")
	ErrInvalidTableOption      = errors.New("invalid table option")
	ErrSchemaSyncInTransaction = errors.New("schema sync cannot run inside a transaction")

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

	// ErrInvalidKeyValue 主键/唯一键里 string/bytes 列的值放不进目标列：超过 max_length，
	// 或 string 不是合法 UTF-8。
	//
	// 必须在发出任何 SQL 之前拦下：严格模式下 MySQL 报 1406/1366，而非严格模式会把超长值
	// **静默截断**——两个只在尾部不同的第三方 ID 截断后落成同一个键，要么撞 1062，要么被当成
	// 同一个账号。WHERE 主键条件和缓存 key 用的也是这份值，超长主键在库里本就不可能存在。
	ErrInvalidKeyValue = errors.New("invalid key column value")

	// ErrLegacyKeyColumn 线上主键/唯一键里的 string/bytes 列不是 VARCHAR（utf8mb4_0900_bin）/
	// VARBINARY 形态，与写入路径的唯一性语义不一致。总是与 ErrSchemaDrift 一起返回。
	//
	// MEDIUMTEXT/MEDIUMBLOB 前缀索引只保证前 191 个字符/字节唯一；*_ci 排序规则把 'AbC' 与 'abc'
	// 判为同一个键；utf8mb4_bin 等 PAD SPACE 规则把 'abc' 与 'abc ' 判为同一个键。
	// 不自动迁移：需要先处理 NULL 与可能的重复、重建索引，TiDB 聚簇主键还不允许原地修改，
	// 这些都必须由人确认数据后执行。
	ErrLegacyKeyColumn = errors.New("legacy string/bytes key column")
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
	// maxLengths 主键/唯一键里 string/bytes 字段的列宽（max_length 选项），未声明的字段取 DefaultKeyColumnLength
	maxLengths map[string]uint32

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

// isKeyColumnName 字段是否出现在主键或唯一键里。唯一键必须与建表处同样按逗号拆分并 TrimSpace，
// 否则 "provider, provider_id" 这种带空格的声明会让列类型与建出来的唯一键对不上。
func (m *MessageTable) isKeyColumnName(name string) bool {
	if slices.Contains(m.primaryKey, name) {
		return true
	}
	if m.uniqueKeys == "" {
		return false
	}
	for _, col := range strings.Split(m.uniqueKeys, ",") {
		if strings.TrimSpace(col) == name {
			return true
		}
	}
	return false
}

// keyColumnKind 字段是主键/唯一键里的非 list/map string 或 bytes 时返回其 Kind。
// 列类型是列的属性：这类列在任何索引里（包括普通索引）都是 VARCHAR/VARBINARY 整列。
func (m *MessageTable) keyColumnKind(fieldDesc protoreflect.FieldDescriptor) (protoreflect.Kind, bool) {
	if fieldDesc == nil || fieldDesc.IsList() || fieldDesc.IsMap() {
		return 0, false
	}
	kind := fieldDesc.Kind()
	if kind != protoreflect.StringKind && kind != protoreflect.BytesKind {
		return 0, false
	}
	return kind, m.isKeyColumnName(string(fieldDesc.Name()))
}

// primaryKeyHasKeyColumn 主键里有没有 string/bytes 键列。这类主键的线上形态可能是旧形态，
// 而旧形态主键只能按影子表重建，重建要用到线上全部二级索引。
func (m *MessageTable) primaryKeyHasKeyColumn() bool {
	for _, name := range m.primaryKey {
		if _, ok := m.keyColumnKind(m.Descriptor.Fields().ByName(protoreflect.Name(name))); ok {
			return true
		}
	}
	return false
}

// keyColumnLength 键列的 N：string 按字符、bytes 按字节。
func (m *MessageTable) keyColumnLength(fieldName string) uint32 {
	if n, ok := m.maxLengths[fieldName]; ok {
		return n
	}
	return DefaultKeyColumnLength
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

	// 主键/唯一键里的 string/bytes 必须建整列索引：MEDIUMTEXT/MEDIUMBLOB 只能建 191 前缀索引，
	// 唯一性只覆盖前 191 个字符/字节。string 用 KeyStringCollation（区分大小写、NO PAD），
	// 否则 'AbC'/'abc'、'abc'/'abc ' 会被判为同一个键。
	// 固定 NOT NULL DEFAULT ''：写入路径从不给 string/bytes 写 NULL，未赋值一律是 ''。
	// 刻意不查 MySQLFieldTypes：那是可被调用方改写的全局表，键列类型不能跟着漂移。
	if kind, ok := m.keyColumnKind(fieldDesc); ok {
		length := m.keyColumnLength(string(fieldDesc.Name()))
		if kind == protoreflect.StringKind {
			return fmt.Sprintf("VARCHAR(%d) CHARACTER SET utf8mb4 COLLATE %s NOT NULL DEFAULT ''", length, KeyStringCollation)
		}
		return fmt.Sprintf("VARBINARY(%d) NOT NULL DEFAULT ''", length)
	}

	fieldName := string(fieldDesc.Name())
	baseType, ok := MySQLFieldTypes[fieldDesc.Kind()]
	if !ok {
		baseType = "TEXT" // 默认类型
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
	// txOwner 指向拥有 pendingCacheDels 的事务根实例。WithContext 派生出的实例必须
	// 共享它，否则 tx.WithContext(ctx).Save 会把待删 key 写进一个提交路径看不到的切片。
	txOwner *DB
	// tableExistsCache 缓存表是否存在的查询结果
	tableExistsCache map[string]bool
	tableExistsMu    sync.RWMutex
	// ctx 由WithContext绑定，用于超时控制/trace传递；nil时用context.Background()
	ctx context.Context
	// pinned 非空时，结构同步的全部语句（DDL + information_schema）都钉在这条连接上。
	// 由 SyncAllTables 在拿到 DDL 咨询锁后设置，见 meta() 的注释。
	pinned contextExecutor
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

// meta 结构同步专用执行器：DDL 与 information_schema 查询都走它。
//
// 平时就是连接池。但 SyncAllTables 拿到 DDL 咨询锁时会把**持锁的那条连接**钉进来
// （见 withPinnedConn），此后整轮同步都在同一个 session 上跑。
//
// ⚠️ 这不是"顺手统一一下"，是必须的：MySQL 的用户锁属于 session，持锁必须钉住连接；
// 而一旦钉住，剩下的语句要是还去连接池另借一条，在 SetMaxOpenConns(1) 的连接池上
// 就是**死锁**——池里唯一那条连接正被锁持有者攥着，DDL 永远借不到，两边都不超时。
// 生产上按副本给一条连接是常见配置，本仓库的 concurrency_integration_test 也正是这么设的。
func (p *DB) meta() contextExecutor {
	if p.pinned != nil {
		return p.pinned
	}
	return p.DB
}

// withPinnedConn 派生一个把所有结构同步语句都钉在 conn 上执行的实例。
// 不带 tx：DDL 不走事务。表存在性缓存另起一份（与 WithContext 同样的取舍）。
func (p *DB) withPinnedConn(conn *sql.Conn) *DB {
	return &DB{
		Tables:           p.Tables,
		DB:               p.DB,
		DBName:           p.DBName,
		ExpandOnly:       p.ExpandOnly,
		cache:            p.cache,
		cacheTTL:         p.cacheTTL,
		tableExistsCache: make(map[string]bool),
		ctx:              p.ctx,
		pinned:           conn,
	}
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
//
// ⚠️ 派生实例必须把 **ExpandOnly 一起带过来**。它是个安全闸而不是普通配置：
// 漏拷一次，`pbDB.WithContext(ctx).SyncAllTables()` 就会在一个自以为开着
// ExpandOnly 的进程里照常执行 MODIFY / CHANGE COLUMN——**闸门关着，但不生效**，
// 且没有任何提示。凡是新增 DB 字段，都要回来问一句"它漏拷了会不会静默降级"。
func (p *DB) WithContext(ctx context.Context) *DB {
	txOwner := p.txOwner
	if p.tx != nil && txOwner == nil {
		txOwner = p
	}
	return &DB{
		Tables:           p.Tables,
		DB:               p.DB,
		DBName:           p.DBName,
		ExpandOnly:       p.ExpandOnly,
		tx:               p.tx,
		txOwner:          txOwner,
		cache:            p.cache,
		cacheTTL:         p.cacheTTL,
		tableExistsCache: make(map[string]bool),
		ctx:              ctx,
		pinned:           p.pinned,
	}
}

func (p *DB) transactionCacheOwner() *DB {
	if p.txOwner != nil {
		return p.txOwner
	}
	return p
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
			ExpandOnly:       p.ExpandOnly, // 安全闸必须跟着走，理由见 WithContext
			tx:               sqlTx,
			cache:            p.cache,
			cacheTTL:         p.cacheTTL,
			tableExistsCache: make(map[string]bool),
			ctx:              p.ctx,
		}
		txDB.txOwner = txDB
		return fn(txDB)
	})
	if err == nil && txDB != nil {
		// 提交成功后统一失效缓存（先写库后删缓存）
		p.cacheDelKeys(txDB.transactionCacheOwner().pendingCacheDels...)
	}
	return err
}

// OpenDB 绑定连接池并校验它确实指向 dbname。
//
// ⚠️ 原先这里执行的是 `USE <dbname>`，而 **USE 是 session 级的**：*sql.DB 是连接池，
// 这条语句只切换了它当场借到的**那一条**连接。池子里其余连接（以及之后新建的连接）
// 用的仍是 DSN 里的库。于是"切库"的效果取决于哪条连接被复用：
// 冷启动后池里通常只有一条，看起来完全正常；并发一上来就开始随机在错误的库上跑，
// 表现为**时好时坏的 Error 1146 Table doesn't exist**，或者更糟——
// 在同名的另一个库上读写成功。
//
// database/sql 没有"给整个池设默认库"的接口，所以这里改成**校验而不是修改**：
// 库必须由 DSN 指定（mysql.Config.DBName / DSN 里的 /dbname），
// 对不上就 fail-closed，给出可直接照做的修法。
func (p *DB) OpenDB(db *sql.DB, dbname string) error {
	p.DB = db

	var current sql.NullString
	if err := p.DB.QueryRowContext(p.context(), "SELECT DATABASE()").Scan(&current); err != nil {
		return fmt.Errorf("读取当前数据库失败: %w", err)
	}
	var lowerCaseTableNames int
	if err := p.DB.QueryRowContext(p.context(), "SELECT @@lower_case_table_names").Scan(&lowerCaseTableNames); err != nil {
		return fmt.Errorf("读取 lower_case_table_names 失败: %w", err)
	}
	matches := current.Valid && current.String == dbname
	if lowerCaseTableNames != 0 {
		matches = current.Valid && strings.EqualFold(current.String, dbname)
	}
	if !matches {
		return fmt.Errorf("连接池当前库是 %q，与要求的 %q 不一致。"+
			"本库不再执行 USE 切库（USE 只作用于池里的一条连接，并发下会随机落到错误的库）——"+
			"请把库名写进 DSN，例如 user:pass@tcp(host:3306)/%s",
			current.String, dbname, dbname)
	}
	// 后续 INFORMATION_SCHEMA 过滤与 DDL 锁命名都使用服务端返回的规范名字。
	p.DBName = current.String
	return nil
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
	case "varchar", "char", "varbinary", "binary":
		// 线上更宽时不动它（收窄会截断已有数据）；更窄时必须拓宽，否则调大 max_length 的键列写入报 1406。
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

// unsafeIntegerSignednessChange 判断两侧是否都是整数族、但 signedness 不同。
// signed 与 unsigned 的值域互不包含，任何方向的自动 ALTER 都可能丢数据。
func unsafeIntegerSignednessChange(currentType, targetType string) bool {
	current := parseMySQLType(currentType)
	target := parseMySQLType(targetType)
	currentBase := normalizeBaseType(current.baseType)
	targetBase := normalizeBaseType(target.baseType)
	for _, family := range typeFamilies {
		if !family.isInteger {
			continue
		}
		_, currentIsInteger := family.ranks[currentBase]
		_, targetIsInteger := family.ranks[targetBase]
		return currentIsInteger && targetIsInteger && current.unsigned != target.unsigned
	}
	return false
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
// 而本库把不在主键/唯一键里的 string 映射成 MEDIUMTEXT（repeated/map/message 映射成 MEDIUMBLOB），
// 只要在这类列上声明了索引，不补前缀产出的就是一条 MySQL 会拒绝执行的 DDL。
// 主键/唯一键里的标量 string/bytes 是 VARCHAR/VARBINARY 键列，建整列索引，不用前缀。
//
// 这个洞长期没暴露，是因为测试只比对 SQL 字符串、从不真的执行：拿本仓库自带的
// tools/proto2sql/testdata/account.proto 生成建表语句打到 MySQL 8.4 上就是 Error 1170。
//
// 191 是 utf8mb4 下的经典安全值（旧的 767 字节索引上限 ÷ 4）。
// 设为 0 表示不补前缀——产出的 DDL 建不了表，只在做历史输出比对时才有意义。
const TextIndexPrefixLength = 191

// 主键/唯一键里 string/bytes 列（VARCHAR/VARBINARY 整列索引）的长度与排序规则约束。
const (
	// DefaultKeyColumnLength 未声明 max_length 时键列的 N（string 按字符、bytes 按字节）。
	// 与 TextIndexPrefixLength 同为 191：旧的 767 字节索引上限 ÷ utf8mb4 的 4 字节，
	// 三个默认长度的 string 列组成的联合键（2292 字节）仍在 MaxIndexKeyBytes 之内。
	DefaultKeyColumnLength = 191
	// MaxKeyStringLength string 键列 max_length 的上限（字符）：MaxIndexKeyBytes ÷ utf8mb4 每字符 4 字节。
	// 实测单列 VARCHAR(768) utf8mb4 可做主键，VARCHAR(769) 报 Error 1071。
	MaxKeyStringLength = 768
	// MaxKeyBytesLength bytes 键列 max_length 的上限（字节）：VARBINARY 每字节计 1，等于 MaxIndexKeyBytes。
	MaxKeyBytesLength = 3072
	// MaxIndexKeyBytes 单个索引（主键、唯一键、每个普通索引各自计算）所有列合计的字节上限。
	// InnoDB DYNAMIC/COMPRESSED 行格式超出报 Error 1071（实测 VARCHAR(500)+VARCHAR(500) 联合主键即报）；
	// TiDB 的 max-index-length 默认同为 3072。
	MaxIndexKeyBytes = 3072
	// KeyStringCollation string 键列的排序规则：按码点比较、区分大小写、NO PAD（尾部空格参与比较）。
	// 第三方账号 ID（如 Google sub）区分大小写且须逐字符精确匹配：utf8mb4_unicode_ci 会把 'AbC' 与 'abc'
	// 判为重复，utf8mb4_bin 是 PAD SPACE、会把 'abc' 与 'abc ' 判为重复。
	// MySQL 8.0.17 起提供；TiDB 需要新排序规则框架（new_collations_enabled_on_first_bootstrap，新集群默认开启）。
	KeyStringCollation = "utf8mb4_0900_bin"
)

// validateFieldKinds 检查这个 message 能不能安全地映射成一张表，不能就 fail-fast。
//
// 放在**任何 DDL 之前**做（建表和对齐都要先过这一关）：等到第一次写入才由 pbconv
// 抛错的话，列已经按 TEXT 建出来了，改回正确类型是跨族 MODIFY，会把数据吃成 0。
func (m *MessageTable) validateFieldKinds() error {
	if strings.TrimSpace(m.tableName) == "" {
		return fmt.Errorf("%w: table_name 不能为空或只含空白", ErrInvalidTableOption)
	}
	// 表名先过一遍长度：MySQL 标识符上限 64 字符，超了是 Error 1059。
	// 没声明 table_name 时表名退化成 proto full name（含 package），很容易就超。
	if n := len([]rune(m.tableName)); n > MySQLMaxIdentifierLength {
		return fmt.Errorf("%w: 表名 %q 有 %d 个字符，超过 MySQL 的 %d 上限。"+
			"没声明 table_name 时表名会退化成 proto full name（含 package），"+
			"请在 .proto 里显式写 option (proto2mysql.table_name)",
			ErrUnsupportedFieldKind, m.tableName, n, MySQLMaxIdentifierLength)
	}

	fields := m.Descriptor.Fields()
	if fields.Len() == 0 {
		return fmt.Errorf("%w: 表 %s 的 protobuf message 没有字段，无法生成合法的 MySQL 表",
			ErrUnsupportedFieldKind, m.tableName)
	}
	for i := 0; i < fields.Len(); i++ {
		fieldDesc := fields.Get(i)
		if n := len([]rune(fieldDesc.Name())); n > MySQLMaxIdentifierLength {
			return fmt.Errorf("%w: 表 %s 的字段名 %q 有 %d 个字符，超过 MySQL 标识符的 %d 上限",
				ErrUnsupportedFieldKind, m.tableName, fieldDesc.Name(), n, MySQLMaxIdentifierLength)
		}

		// 真实 oneof 一律拒绝——它在本库的列模型里**无法正确往返**，而且是静默的。
		//
		// 本库把每个字段各映射成一列，列是 NOT NULL 的，没有"这个成员没被选中"的表示：
		//   写：SerializeFieldAsString 对未选中的标量成员照样 reflection.Get，
		//       拿到的是零值，于是每个成员列都被写成 0 / ""，看不出谁才是被选中的那个；
		//   读：ParseFromString 按**声明顺序**逐个 setFieldFromString，而对 oneof 成员
		//       Set（包括 setScalarDefault 里那次 Set 默认值）本身就会改变"当前选中的是谁"
		//       —— 于是**最后声明的那个成员恒胜**，真实选择被抹掉。
		//
		// 要正确往返就得让成员列可空、且只写选中的那一列，那是一次不兼容的结构改动，
		// 不是能靠调参绕开的。在这里 fail-closed，好过让人以为 oneof 能用。
		//
		// proto3 的 optional 字段在描述符里也表现为 oneof（synthetic），那个只是
		// "带 has 位的普通字段"，往返完全正常，放行。
		if od := fieldDesc.ContainingOneof(); od != nil && !od.IsSynthetic() {
			return fmt.Errorf("%w: 表 %s 的字段 %s 属于 oneof %q。"+
				"本库把每个字段各映射成一列，没有\"未选中\"的表示：写入时未选中的成员按零值落列，"+
				"读取时按声明顺序逐个 Set 又会让**最后声明的成员恒胜**，oneof 的真实选择被静默抹掉。"+
				"请把 oneof 拆成普通字段（另加一个 enum 字段标记当前用哪个），"+
				"或把整个 oneof 包进一个子 message 字段（子 message 整体序列化进一列，往返正常）",
				ErrUnsupportedFieldKind, m.tableName, fieldDesc.Name(), od.Name())
		}

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

// validateSchemaDefinition 校验所有会影响 DDL 的消息与表选项。
// validateFieldKinds 也用于 DML 编解码；TableOption 的约束只属于 schema，不能让一个
// 只读 SQLBuilder 因未使用的 auto_increment 选项组合而失效，所以二者保持分层。
func (m *MessageTable) validateSchemaDefinition() error {
	if err := m.validateFieldKinds(); err != nil {
		return err
	}
	return m.validateTableOptions()
}

// validateTableOptions 在任何 DDL 产出前拒绝必然生成无效 SQL 的选项组合。
func (m *MessageTable) validateTableOptions() error {
	fields := m.Descriptor.Fields()
	lookup := func(option, name string) (protoreflect.FieldDescriptor, error) {
		if name == "" || strings.TrimSpace(name) != name {
			return nil, fmt.Errorf("%w: 表 %s 的 %s 含空字段名或首尾空白 %q",
				ErrInvalidTableOption, m.tableName, option, name)
		}
		field := fields.ByName(protoreflect.Name(name))
		if field == nil {
			return nil, fmt.Errorf("%w: 表 %s 的 %s 引用了不存在的字段 %q",
				ErrInvalidTableOption, m.tableName, option, name)
		}
		return field, nil
	}
	validateKeyColumns := func(option string, rawColumns []string, trim bool) error {
		if len(rawColumns) == 0 {
			return fmt.Errorf("%w: 表 %s 的 %s 没有字段", ErrInvalidTableOption, m.tableName, option)
		}
		seen := make(map[string]struct{}, len(rawColumns))
		for _, rawName := range rawColumns {
			name := rawName
			if trim {
				name = strings.TrimSpace(name)
			}
			if name == "" {
				return fmt.Errorf("%w: 表 %s 的 %s 含空字段分量",
					ErrInvalidTableOption, m.tableName, option)
			}
			if _, duplicate := seen[name]; duplicate {
				return fmt.Errorf("%w: 表 %s 的 %s 重复引用字段 %q",
					ErrInvalidTableOption, m.tableName, option, name)
			}
			seen[name] = struct{}{}
			if _, err := lookup(option, name); err != nil {
				return err
			}
		}
		return nil
	}

	if len(m.primaryKey) > 0 {
		if err := validateKeyColumns("primary_key", m.primaryKey, false); err != nil {
			return err
		}
		for _, name := range m.primaryKey {
			field := fields.ByName(protoreflect.Name(name))
			targetType := m.getMySQLFieldType(field)
			if field.Kind() == protoreflect.FloatKind || field.Kind() == protoreflect.DoubleKind {
				return fmt.Errorf("%w: 表 %s 的主键字段 %q 是 %s；浮点值不能作为稳定身份，"+
					"其十进制、二进制与数据库比较语义可能不一致；请改用整数或枚举主键",
					ErrInvalidTableOption, m.tableName, name, field.Kind())
			}
			if m.needsIndexPrefix(name) {
				return fmt.Errorf("%w: 表 %s 的主键字段 %q 映射为 %s，只能建立前缀索引，"+
					"不能保证完整主键唯一性。标量 string/bytes 主键会映射为 VARCHAR/VARBINARY 整列索引，"+
					"但 repeated/map/message 这类整体序列化进 BLOB 的字段不行；请改用整数/枚举或标量 string/bytes 字段",
					ErrInvalidTableOption, m.tableName, name, targetType)
			}
			if !strings.Contains(strings.ToUpper(targetType), "NOT NULL") {
				return fmt.Errorf("%w: 表 %s 的主键字段 %q 目标类型 %s 可为 NULL；"+
					"MySQL 会静默强制成 NOT NULL 并造成永久 schema drift",
					ErrInvalidTableOption, m.tableName, name, targetType)
			}
		}
	}
	for i, spec := range m.indexes {
		// 这里必须保留 strings.Split 产生的空分量：生成路径同样逐分量输出，
		// 如果先用 splitTrimmed 吞掉空项，"id,,port" 会预检通过却生成 ` `` ` 列。
		if err := validateKeyColumns(fmt.Sprintf("index[%d]", i), strings.Split(spec, ","), true); err != nil {
			return err
		}
	}
	if m.uniqueKeys != "" {
		if err := validateKeyColumns("unique_key", strings.Split(m.uniqueKeys, ","), true); err != nil {
			return err
		}
	}
	for _, name := range m.nullableFields {
		field, err := lookup("nullable", name)
		if err != nil {
			return err
		}
		if kind, ok := m.keyColumnKind(field); ok {
			return fmt.Errorf("%w: 表 %s 的 %s 字段 %q 在主键/唯一键里，不能声明 nullable："+
				"写入路径从不为 string/bytes 写 NULL（未赋值一律写 ''），nullable 无法让未赋值的行不参与唯一性，"+
				"只会让列定义与写入语义不一致",
				ErrInvalidTableOption, m.tableName, kind, name)
		}
		if slices.Contains(m.primaryKey, name) {
			return fmt.Errorf("%w: 表 %s 的主键字段 %q 不能同时声明 nullable",
				ErrInvalidTableOption, m.tableName, name)
		}
	}
	if err := m.validateMaxLengths(lookup); err != nil {
		return err
	}
	if err := m.validateIndexKeyBytes(); err != nil {
		return err
	}

	if m.autoIncreaseKey == "" {
		return nil
	}
	autoField, err := lookup("auto_increment_key", m.autoIncreaseKey)
	if err != nil {
		return err
	}
	if autoField.IsList() || autoField.IsMap() || !isAutoIncrementKind(autoField.Kind()) {
		return fmt.Errorf("%w: 表 %s 的 auto_increment_key %q 必须是 int32/int64/uint32/uint64 标量字段，实际为 %s",
			ErrInvalidTableOption, m.tableName, m.autoIncreaseKey, autoField.Kind())
	}
	if m.isNullableField(m.autoIncreaseKey) {
		return fmt.Errorf("%w: 表 %s 的 auto_increment_key %q 不能同时声明 nullable",
			ErrInvalidTableOption, m.tableName, m.autoIncreaseKey)
	}
	if !m.autoIncrementIsFirstKeyColumn() {
		return fmt.Errorf("%w: 表 %s 的 auto_increment_key %q 必须是某个主键/普通索引/唯一键的第一列",
			ErrInvalidTableOption, m.tableName, m.autoIncreaseKey)
	}
	return nil
}

func isAutoIncrementKind(kind protoreflect.Kind) bool {
	switch kind {
	case protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		return true
	default:
		return false
	}
}

func (m *MessageTable) autoIncrementIsFirstKeyColumn() bool {
	if len(m.primaryKey) > 0 && m.primaryKey[0] == m.autoIncreaseKey {
		return true
	}
	for _, spec := range m.indexes {
		columns := splitTrimmed(spec)
		if len(columns) > 0 && columns[0] == m.autoIncreaseKey {
			return true
		}
	}
	columns := splitTrimmed(m.uniqueKeys)
	return len(columns) > 0 && columns[0] == m.autoIncreaseKey
}

// ValidateTableMessage 检查一个 message 能否安全映射成表（字段类型有映射、无真实 oneof、
// 表名不超长）。供**离线/生成期**调用方（如 tools/proto2sql）在产出 DDL 之前把关——
// 运行时路径已经在 syncTableSchema / CreateOrUpdateTable / GenerateMigrationSQL 里
// 自动做了这一步。
//
// 历史 GetCreateTableSQL 签名没有 error 位，校验失败时只能返回空串；需要错误原因的
// 调用方应直接用本函数或 GenerateCreateTableSQLChecked。
func ValidateTableMessage(m proto.Message, opts ...TableOption) error {
	return newMessageTable(m, opts...).validateSchemaDefinition()
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

// MySQLMaxIdentifierLength MySQL 标识符（表名 / 列名 / 索引名）的硬上限，单位是**字符**。
// 超了直接 Error 1059 Identifier name is too long。
const MySQLMaxIdentifierLength = 64

// indexNameFor / uniqueKeyName 索引名的**唯一**生成处。
//
// 建表分支和"老表补索引"分支必须叫出同一个名字，否则 missingIndexClauses 拿名字去
// 比对既有索引会永远比不中，每次启动都试着再加一遍（然后撞 Error 1061）。
// 所以两边都只能经由这两个函数取名。
func (m *MessageTable) indexNameFor(idx int) string {
	return truncateIdentifier(fmt.Sprintf("idx_%s_%d", m.tableName, idx))
}

func (m *MessageTable) uniqueKeyName() string {
	return truncateIdentifier("uk_" + m.tableName)
}

// truncateIdentifier 把超长标识符压回 MySQL 的 64 字符上限内。
//
// 表名本身是合法的（≤64），但 "idx_" + 表名 + "_0" 之后就可能不是了——一个 62 字符的
// 合法表名，产出的索引名是 68 字符，建表直接 Error 1059。这不是构造出来的边界：
// 表名退化成 proto full name（含 package）时轻易就到这个长度。
//
// 截断必须**确定性**：同一个输入永远得到同一个输出，否则上面那条"两个分支叫同一个
// 名字"的前提就没了。所以是「保留前缀 + 8 位 FNV-1a 指纹」，而不是随便切一刀——
// 只切前缀的话，两个长表名很容易切出同一个名字，第二张表建索引时撞 Error 1061。
//
// 按 rune 而不是 byte 切：MySQL 数的是字符，而且按字节切会把一个 UTF-8 字符劈成两半。
func truncateIdentifier(name string) string {
	runes := []rune(name)
	if len(runes) <= MySQLMaxIdentifierLength {
		return name
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	suffix := fmt.Sprintf("_%08x", h.Sum32())
	return string(runes[:MySQLMaxIdentifierLength-len(suffix)]) + suffix
}

// GetCreateTableSQL 生成创建表的SQL语句。
// 历史签名没有 error 返回位；遇到不支持字段、真实 oneof 或非法表名时返回空串，
// 需要错误原因的调用方应使用 GenerateCreateTableSQLChecked / ValidateTableMessage。
func (m *MessageTable) GetCreateTableSQL() string {
	if err := m.validateSchemaDefinition(); err != nil {
		return ""
	}
	return m.buildCreateTableSQL(createTableSpec{ifNotExists: true}) + ";"
}

// defaultTableCollation 建表语句里的表级排序规则。影子表重建时要拿它判断"线上列的排序规则
// 是不是表默认值"，所以不能再写成字面量散在两处。
const defaultTableCollation = "utf8mb4_unicode_ci"

// createTableSpec 建表语句里会随场景变化的部分，零值即 GetCreateTableSQL 的形态。
// 影子表重建（见 legacyKeyRebuildSQL）要换表名、单行输出、按线上形态覆盖列类型并追加线上
// 未声明的索引；那些都必须与建表走同一份生成逻辑，否则索引名、TiDB 方言块、表 COMMENT、
// 字符集会在两处各写一遍并慢慢分叉。
type createTableSpec struct {
	tableName    string                                    // 目标表名；空则用 m.tableName（表 COMMENT 始终是 m.tableName）
	ifNotExists  bool                                      // 加 IF NOT EXISTS
	inline       bool                                      // 单行输出：迁移 SQL 块里一条语句占一行
	columnType   func(protoreflect.FieldDescriptor) string // 列类型覆盖；nil 表示 getMySQLFieldType
	extraIndexes []string                                  // 追加在声明索引之后的索引子句，如 "UNIQUE KEY `x` (`a`)"
}

func (m *MessageTable) buildCreateTableSQL(spec createTableSpec) string {
	tableName := spec.tableName
	if tableName == "" {
		tableName = m.tableName
	}
	columnType := spec.columnType
	if columnType == nil {
		columnType = m.getMySQLFieldType
	}

	desc := m.Descriptor
	entries := make([]string, 0, desc.Fields().Len()+len(m.indexes)+len(spec.extraIndexes)+2)
	for i := 0; i < desc.Fields().Len(); i++ {
		field := desc.Fields().Get(i)
		entries = append(entries, fmt.Sprintf("%s %s%s",
			escapeMySQLName(string(field.Name())), columnType(field), columnComment(field.Number())))
	}

	if len(m.primaryKey) > 0 {
		primaryKeys := make([]string, len(m.primaryKey))
		for i, pk := range m.primaryKey {
			primaryKeys[i] = m.indexColumn(pk)
		}
		pkClause := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ","))
		if m.tidbNonclusteredPK {
			pkClause += tidbNonclusteredPKSQL
		}
		entries = append(entries, pkClause)
	}

	for idx, indexCols := range m.indexes {
		entries = append(entries, fmt.Sprintf("INDEX %s (%s)",
			escapeMySQLName(m.indexNameFor(idx)), m.indexColumnsSQL(indexCols)))
	}

	if m.uniqueKeys != "" {
		for _, col := range splitTrimmed(m.uniqueKeys) {
			if m.needsIndexPrefix(col) {
				log.Printf("warning: unique key on TEXT/BLOB column %s in table %s only enforces "+
					"uniqueness over the first %d characters", col, m.tableName, TextIndexPrefixLength)
			}
		}
		entries = append(entries, fmt.Sprintf("UNIQUE KEY %s (%s)",
			escapeMySQLName(m.uniqueKeyName()), m.indexColumnsSQL(m.uniqueKeys)))
	}
	entries = append(entries, spec.extraIndexes...)

	open, glue, closing := " (\n  ", ",\n  ", "\n)"
	if spec.inline {
		open, glue, closing = " (", ", ", ")"
	}
	stmt := "CREATE TABLE "
	if spec.ifNotExists {
		stmt += "IF NOT EXISTS "
	}
	// 表注释简化为表名；TiDB 方言块按 TiDB 规范导出顺序放在 COMMENT 之前
	return stmt + escapeMySQLName(tableName) + open + strings.Join(entries, glue) + closing +
		" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=" + defaultTableCollation +
		m.tidbTableOptionsSQL() + " COMMENT='" + escapeMySQLComment(m.tableName) + "'"
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

// escapeMySQLComment 转义 MySQL 字符串字面量里的特殊字符，供 COMMENT '...' 使用。
//
// 原先只处理 `'` 和换行；默认 SQL mode 下，输入里的反斜杠会参与转义，可能改变
// 后续引号的边界。现在先双写反斜杠，再按 SQL 标准把单引号倍写成 `”`：前者保护
// 默认模式，后者在默认模式和 NO_BACKSLASH_ESCAPES 下都成立。
//
//	table_name = `evil\`     →  COMMENT 'evil\'   ← \' 被 MySQL 读成转义的引号，
//	                                                 字符串没结束，后面的 SQL 全被吃进去
//
// 表名不是绑定参数、是直接拼进 DDL 的，而连接默认开着 MultiStatements
// （见 NewMysqlConfig，FindMultiByWhereClauses 依赖它），所以这条缝隙足以往
// 建表语句里追加任意语句。表名来自 .proto 的 table_name 选项——多数仓库里它是可信的，
// 但"可信输入"不是转义可以省略的理由。
func escapeMySQLComment(comment string) string {
	return strings.NewReplacer(
		"\\", "\\\\",
		"'", "''",
		"\n", " ",
		"\r", " ",
	).Replace(comment)
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

// columnMeta 线上单列的元信息。
//
// ⚠️ 早先这里**只有类型和字段号注释**，于是"结构已对齐"这个判断是瞎的：
// 一列在线上是 NULL 而 proto 要 NOT NULL、或者自增标记掉了，本库一律判为"没差异"，
// 什么都不做、什么都不说。等到运行时才表现为写入被拒（Error 1048）或
// 主键不再自增（拿到一堆 id=0 撞 1062），而结构同步的日志里干干净净。
//
// 现在把 IS_NULLABLE / EXTRA / COLUMN_DEFAULT 一起读回来，用于 fail-closed 校验漂移。
// 刻意不据此自动 MODIFY：那正是 ExpandOnly 要禁的那类语句——滚动发布时新旧副本
// 对同一列的期望不同，会一次重启翻一次面；而且给已存在的列加 AUTO_INCREMENT
// 在 TiDB 上直接是 Error 8200。这里的定位是"把哑的变成响的"，不是自动修。
type columnMeta struct {
	colType          string
	fieldNum         protoreflect.FieldNumber
	nullable         bool           // IS_NULLABLE = 'YES'
	defaultValue     sql.NullString // COLUMN_DEFAULT；Valid=false 表示 SQL NULL
	extra            string         // EXTRA（小写），含 "auto_increment" 时该列是自增列
	collation        string         // COLLATION_NAME；非字符列（数值/二进制/时间）为空串
	metadataComplete bool           // nullable/default/extra/collation 都来自真实元数据时才校验属性漂移
}

// isAutoIncrement 线上这一列当前带不带 AUTO_INCREMENT。
func (c columnMeta) isAutoIncrement() bool {
	return strings.Contains(c.extra, "auto_increment")
}

// lookupColumnMeta 按 MySQL 的列名语义查找线上列。
//
// MySQL 列名始终不区分大小写；若这里只做 map 的精确下标，存量列 `ID` 与 proto
// 字段 `id` 会被误判为两列，并生成必然撞 Error 1060 的 ADD COLUMN。另一方面，测试
// 快照或异常元数据里可能同时出现多个大小写折叠后相同的名字；这种身份已经歧义，
// 不能为了确定性随便挑一个，必须 fail-closed。
func (m *MessageTable) lookupColumnMeta(columns map[string]columnMeta, name string) (string, columnMeta, bool, error) {
	candidates := make([]string, 0, 1)
	for actualName := range columns {
		if strings.EqualFold(actualName, name) {
			candidates = append(candidates, actualName)
		}
	}
	if len(candidates) == 0 {
		return "", columnMeta{}, false, nil
	}
	sort.Strings(candidates)
	if len(candidates) > 1 {
		return "", columnMeta{}, false, fmt.Errorf(
			"%w: 表 %s 的线上列 %v 按 MySQL 大小写不敏感语义都匹配 proto 字段 %q；列身份歧义，拒绝自动同步",
			ErrSchemaDrift, m.tableName, candidates, name)
	}
	actualName := candidates[0]
	return actualName, columns[actualName], true, nil
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
	rows, err := p.meta().QueryContext(p.context(), query, p.DBName, table.tableName)
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
		SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLLATION_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`
	rows, err := p.meta().QueryContext(p.context(), query, p.DBName, table.tableName)
	if err != nil {
		return nil, fmt.Errorf("query column meta for table %s: %w", table.tableName, err)
	}
	defer rows.Close()

	metas := make(map[string]columnMeta)
	for rows.Next() {
		var colName, colType, colComment, isNullable, extra string
		var defaultValue, collation sql.NullString
		if err := rows.Scan(&colName, &colType, &colComment, &isNullable, &defaultValue, &extra, &collation); err != nil {
			return nil, fmt.Errorf("scan column meta for table %s: %w", tableName, err)
		}
		meta := columnMeta{
			colType:          colType,
			nullable:         strings.EqualFold(isNullable, "YES"),
			defaultValue:     defaultValue,
			extra:            strings.ToLower(extra),
			collation:        collation.String,
			metadataComplete: true,
		}
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
	clauses, err := m.buildColumnClauses(currentCols, expandOnly)
	if err != nil {
		return nil, err
	}
	return alterClauseSQLs(clauses), nil
}

// alterClause 一条列变更子句，连同后面要用到的元信息一起带着。
//
// 为什么不只存 SQL 字符串：syncTableSchema 需要把「主键列自己的那条列变更」挪到
// ADD PRIMARY KEY 同一条 ALTER 里去（MySQL 要求自增列必须是键，否则 Error 1075）。
// 只有字符串的话，判据只能是 strings.HasPrefix(clause, "MODIFY COLUMN `id` ")——
// 于是 MODIFY 猜中了，而这两种形态**全被漏掉**：
//
//	ADD COLUMN `id` bigint NOT NULL AUTO_INCREMENT ...        ← 新建自增主键列
//	CHANGE COLUMN `old_id` `id` bigint ... AUTO_INCREMENT ... ← 改名成自增主键列
//
// 它们留在第一条 ALTER 里，在主键还不存在时就带着 AUTO_INCREMENT 下发，Error 1075，
// 整条 ALTER 连同所有 ADD COLUMN 一起失败。把列名和自增标记显式记下来，判据就不用猜了。
type alterClause struct {
	sql           string // 完整子句，如 "ADD COLUMN `id` bigint NOT NULL COMMENT 'pb:1'"
	column        string // 这条子句作用后的列名（CHANGE 取新名）
	autoIncrement bool   // 子句里带 AUTO_INCREMENT
}

func alterClauseSQLs(clauses []alterClause) []string {
	if len(clauses) == 0 {
		return nil
	}
	out := make([]string, len(clauses))
	for i, c := range clauses {
		out[i] = c.sql
	}
	return out
}

// buildColumnClauses 是 buildAlterClauses 的实现本体，多返回每条子句的列名/自增标记。
func (m *MessageTable) buildColumnClauses(currentCols map[string]columnMeta, expandOnly bool) ([]alterClause, error) {
	remaining := make(map[string]columnMeta, len(currentCols))
	if err := m.validateSchemaDefinition(); err != nil {
		return nil, err
	}

	// byFieldNum 必须在生成任何 DDL 前确认身份唯一。
	//
	// 早先是直接 `byFieldNum[meta.fieldNum] = name` 边遍历 currentCols 边写——而 Go 的
	// map 迭代顺序是随机化的。正常情况下一个 pb:N 只对应一列，看不出问题；但线上表出现
	// 两列带同一个 pb:N 时（DBA 照 SHOW CREATE TABLE 复制一个备份列就会），
	// **每次运行挑中的列都可能不同**，产出的 CHANGE COLUMN 也就不同——其中一种会去
	// 改那个备份列。即使按字典序稳定挑一个，也无法证明那列是真实数据；确定性不等于正确性。
	//
	// 因此先按列名排序只为让错误文本稳定；一旦发现重复 pb:N 就以 ErrSchemaDrift
	// fail-closed，要求人工确认数据列并修正 COMMENT 后再迁移。
	byFieldNum := make(map[protoreflect.FieldNumber]string, len(currentCols))
	columnNames := make([]string, 0, len(currentCols))
	for name := range currentCols {
		columnNames = append(columnNames, name)
	}
	sort.Strings(columnNames)
	for _, name := range columnNames {
		meta := currentCols[name]
		remaining[name] = meta
		if meta.fieldNum == 0 {
			continue
		}
		if prev, dup := byFieldNum[meta.fieldNum]; dup {
			return nil, fmt.Errorf("%w: 表 %s 的线上列 %q 与 %q 都声明 COMMENT 'pb:%d'；"+
				"字段号身份已经歧义，无法安全判断哪列是真实数据。请先人工修正重复注释再同步",
				ErrSchemaDrift, m.tableName, prev, name, meta.fieldNum)
		}
		byFieldNum[meta.fieldNum] = name
	}

	var alterSQLs []alterClause
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

		// 1) 按 MySQL 大小写不敏感的列名语义匹配。
		onlineName, meta, exists, err := m.lookupColumnMeta(remaining, fieldName)
		if err != nil {
			return nil, err
		}
		if exists {
			// 类型不兼容，或旧表该列尚无字段号注释时，MODIFY 顺带回填注释
			if !isTypeMatch(meta.colType, targetType) || meta.fieldNum != fieldNum {
				if !isRenameConvertible(meta.colType, targetType) {
					return nil, fmt.Errorf("%w: 表 %s 的列 %s 线上是 %s，proto 要 %s。"+
						"跨类型族自动 MODIFY 会依赖 MySQL 隐式转换并可能把整列数据改成 0/空值；"+
						"请人工迁移数据后再调整 proto",
						ErrUnsafeSchemaConversion, m.tableName, fieldName, meta.colType, targetType)
				}
				if unsafeIntegerSignednessChange(meta.colType, targetType) {
					return nil, fmt.Errorf("%w: 表 %s 的列 %s 线上是 %s，proto 要 %s。"+
						"signed 与 unsigned 的值域互不包含，自动转换可能截断负数或大正数；"+
						"请先核对存量范围并人工执行 ALTER",
						ErrUnsafeSchemaConversion, m.tableName, fieldName, meta.colType, targetType)
				}
				// ⚠️ 这里**不能**无脑用 targetType。走到这一分支有两个原因，
				// 而第二个（fieldNum 对不上，需要回填 pb:N 注释）与类型兼容性无关：
				// 线上是 bigint、proto 要 int，isTypeMatch 判"兼容、不用动"，
				// 但只要这列还没有 pb:N 注释，就会因为回填注释顺带把列 MODIFY 成 int——
				// "永不收窄"被一条注释回填绕过去了，bigint 里超出 int 的值当场被吃掉。
				// 老表恰恰**普遍没有** pb:N 注释（那是后加的机制），所以这条路不是边界情况。
				alignedType := alignedColumnType(meta.colType, targetType)
				if alignedType != targetType {
					log.Printf("table %s: 列 %s 需要回填字段号注释，但 proto 的目标类型 %s 比线上的 %s 窄——"+
						"本次按线上类型下 MODIFY，只补注释不收窄。确实要收窄请人工写 ALTER",
						m.tableName, fieldName, targetType, meta.colType)
				}
				alterSQLs = append(alterSQLs, newAlterClause(
					fmt.Sprintf("MODIFY COLUMN %s %s%s", escapeMySQLName(fieldName), alignedType, comment),
					fieldName, alignedType))
			} else if narrowingSuppressed(meta.colType, targetType) {
				// 什么都不做，但必须留痕：否则有人把 bigint 改回 int、期待列变窄，
				// 结果什么也没发生，也没有任何线索告诉他为什么。
				log.Printf("table %s: 列 %s 线上是 %s、proto 要 %s——**保持线上的不动**。"+
					"收窄会丢数据，且在滚动发布期间会被新旧副本来回改。确实要收窄请人工写 ALTER",
					m.tableName, fieldName, meta.colType, targetType)
			}
			delete(remaining, onlineName)
			continue
		}

		// 2) 字段号匹配（改名场景）：找到注释字段号一致但列名不同的现有列
		if oldName, ok := byFieldNum[fieldNum]; ok {
			if oldMeta, still := remaining[oldName]; still {
				if unsafeIntegerSignednessChange(oldMeta.colType, targetType) {
					return nil, fmt.Errorf("%w: 表 %s 的列 %s→%s 改名同时要求从 %s 变成 %s。"+
						"signed 与 unsigned 的值域互不包含，请把改名与人工数据迁移分开执行",
						ErrUnsafeSchemaConversion, m.tableName, oldName, fieldName, oldMeta.colType, targetType)
				}
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
				// 改名路径同样受"永不收窄"约束。isRenameConvertible 只判同族、不判宽窄，
				// 于是 bigint 改名成 int 是"可改名"的——CHANGE COLUMN 会连名字带类型一起改，
				// 数据在隐式转换里被截断。这里把类型对齐到"不比线上窄"，只改名不缩宽度。
				alignedType := alignedColumnType(oldMeta.colType, targetType)
				if alignedType != targetType {
					log.Printf("table %s: 列 %s→%s 改名时 proto 的目标类型 %s 比线上的 %s 窄——"+
						"本次只改名不收窄，按线上类型下 CHANGE COLUMN",
						m.tableName, oldName, fieldName, targetType, oldMeta.colType)
				}
				alterSQLs = append(alterSQLs, newAlterClause(
					fmt.Sprintf("CHANGE COLUMN %s %s %s%s",
						escapeMySQLName(oldName), escapeMySQLName(fieldName), alignedType, comment),
					fieldName, alignedType))
				delete(remaining, oldName)
				continue
			}
		}

		// 3) 全新字段
		alterSQLs = append(alterSQLs, newAlterClause(
			fmt.Sprintf("ADD COLUMN %s %s%s", escapeMySQLName(fieldName), targetType, comment),
			fieldName, targetType))
	}

	if expandOnly {
		if err := m.checkExpandOnly(alterClauseSQLs(alterSQLs)); err != nil {
			return nil, err
		}
	}

	return alterSQLs, nil
}

// newAlterClause 记下这条子句作用的列名，以及它带不带 AUTO_INCREMENT。
// 自增标记直接从生成好的列类型里读——列类型是 getMySQLFieldType 的产物，
// AUTO_INCREMENT 只可能由它加上，不会有别的来源。
func newAlterClause(sql, column, columnType string) alterClause {
	return alterClause{
		sql:           sql,
		column:        column,
		autoIncrement: strings.Contains(strings.ToUpper(columnType), "AUTO_INCREMENT"),
	}
}

// checkExpandOnly ExpandOnly 下的准入判定：只放行「纯新增」的语句。
//
// ⚠️ 这个判定必须覆盖**本次要下发的全部语句**，而不只是列对齐那一批。
// 原先它写在 buildAlterClauses 末尾，只看得见自己产出的 ADD/MODIFY/CHANGE COLUMN；
// 而 syncTableSchema 在那之后还会追加 ADD INDEX / ADD UNIQUE KEY / ADD PRIMARY KEY，
// 以及跟着主键走的那条列变更——它们**从来没有过这道闸**。闸门看起来是关着的，
// 实际上一整类语句从旁边走过去了。所以改成由 planSchemaAlignment 统一在最后判一次。
//
// 放行 ADD 全家（COLUMN / INDEX / UNIQUE KEY / PRIMARY KEY），拒绝 MODIFY / CHANGE。
// 判据就是 ExpandOnly 自己的立论：**旧版本副本不会把它撤销**。
// ADD COLUMN 是这样（旧版本的 SQL 里根本不出现新列名），索引与主键也是这样
// （本库只补不删，见 missingIndexClauses 与 syncTableSchema 的注释）；
// 只有 MODIFY / CHANGE 会被新旧副本来回执行，一次重启翻一次面。
func (m *MessageTable) checkExpandOnly(clauses []string) error {
	var offenders []string
	for _, clause := range clauses {
		if !strings.HasPrefix(clause, "ADD ") {
			offenders = append(offenders, clause)
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	return fmt.Errorf("%w: 表 %s 的本次对齐含非「纯新增」变更，ExpandOnly 下拒绝执行：\n  %s\n"+
		"  这些语句在滚动发布下会被新旧副本来回执行。请改成 expand→migrate→contract 三步，"+
		"或人工审核后单独执行", ErrExpandOnlyViolation, m.tableName, strings.Join(offenders, "\n  "))
}

// narrowingSuppressed 本次"判为兼容"是不是**因为挡下了一次收窄**（而不是两边本来就一样）。
//
// 专门用来打日志。收窄抑制是本库唯一一个「什么都不做、也什么都不说」的分支：
// 改名有 warning、ExpandOnly 违规有带语句清单的报错，唯独这里一声不吭——
// 于是有人在 proto 里把 bigint 改回 int、期待列跟着变窄，结果什么也没发生，
// 也没有任何线索告诉他为什么。
//
// 判据：类型确实不同，且线上那一侧更宽。
func narrowingSuppressed(currentType, targetType string) bool {
	current := parseMySQLType(currentType)
	target := parseMySQLType(targetType)
	currentBase := normalizeBaseType(current.baseType)
	targetBase := normalizeBaseType(target.baseType)

	if currentBase != targetBase {
		for _, family := range typeFamilies {
			c, okC := family.ranks[currentBase]
			t, okT := family.ranks[targetBase]
			if !okC || !okT {
				continue
			}
			if family.isInteger && current.unsigned != target.unsigned {
				return false // 值域方向不同，不是宽窄问题
			}
			return c > t
		}
		return false
	}

	switch currentBase {
	case "varchar", "char", "varbinary", "binary", "datetime":
		return current.length > target.length
	case "float", "double":
		return current.decimal > target.decimal
	}
	return false
}

// alignedColumnType 本次 ALTER 里该写哪个列类型：默认就是 proto 的目标类型，
// 但目标比线上**窄**时，保留线上的类型本体（"永不收窄"），只带上目标的属性。
//
// 属性必须来自目标：NOT NULL / DEFAULT / AUTO_INCREMENT 这些是 proto 侧的决定，
// 而 information_schema 的 COLUMN_TYPE 里根本没有它们。所以做的是"换类型本体、
// 留属性"，不是整段替换。
func alignedColumnType(currentType, targetType string) string {
	if unsafeIntegerSignednessChange(currentType, targetType) {
		return replaceColumnBaseType(targetType, currentType)
	}
	if !narrowingSuppressed(currentType, targetType) {
		return targetType
	}
	return replaceColumnBaseType(targetType, currentType)
}

// replaceColumnBaseType 把列定义 targetType 的**类型本体**换成 baseSpec，其余属性原样保留。
//
//	replaceColumnBaseType("int NOT NULL DEFAULT 0", "bigint")          → "bigint NOT NULL DEFAULT 0"
//	replaceColumnBaseType("int unsigned NOT NULL", "bigint unsigned")  → "bigint unsigned NOT NULL"
//
// baseSpec 取自 information_schema.COLUMN_TYPE，它自己就带着 unsigned / 长度，
// 所以目标里紧跟类型本体的那个 unsigned 要一起丢掉，否则会拼出 "bigint unsigned unsigned"。
func replaceColumnBaseType(targetType, baseSpec string) string {
	parts := strings.Fields(targetType)
	if len(parts) == 0 {
		return baseSpec
	}
	rest := parts[1:]
	if len(rest) > 0 && strings.EqualFold(rest[0], "unsigned") {
		rest = rest[1:]
	}
	if len(rest) == 0 {
		return baseSpec
	}
	return baseSpec + " " + strings.Join(rest, " ")
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
	if p.tx != nil {
		return fmt.Errorf("%w: UpdateTableField; MySQL DDL implicitly commits and cannot be rolled back with the business transaction",
			ErrSchemaSyncInTransaction)
	}
	tableName := GetTableName(m)
	table, ok := p.Tables[tableName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}
	// 即使当前只同步一张，也必须先检查整个 registry。否则两个不同 message 映射到
	// 同一物理表时，分别调用单表入口仍会把列按各自 descriptor 来回改名/改型。
	if _, err := p.registeredTablesForSQLGeneration(); err != nil {
		return err
	}
	if err := table.validateSchemaDefinition(); err != nil {
		return err
	}

	// 单表入口也必须覆盖“抢锁 → 读元数据 → DDL → 等待可见”的完整临界区。
	// SyncAllTables 已经在外层持锁并直接调用 syncTableSchema，因此不会在这里双重抢锁。
	syncDB := p
	lockConn, err := p.acquireSyncLock()
	if err != nil {
		return err
	}
	if lockConn != nil {
		defer p.releaseSyncLock(lockConn)
		syncDB = p.withPinnedConn(lockConn)
	}
	return syncDB.syncTableSchema(tableName, table)
}

// syncTableSchema 按 registryKey（proto full name）对应的 table 同步 MySQL 表结构：
// 表不存在则创建，存在则对齐字段类型。
func (p *DB) syncTableSchema(registryKey string, table *MessageTable) error {
	// 先校验，**再建表**。原先这道校验只在 buildAlterClauses 里做，而它跑在
	// CREATE TABLE 之后：一个没有 MySQL 映射的字段（sint32/fixed64…）会先按
	// 默认的 TEXT 建出一列来，然后函数才返回错误。表已经在库里了，事后改回正确类型
	// 是跨族 MODIFY，会把数据吃成 0——报错"提前"了，损害却已经落地。
	if err := table.validateSchemaDefinition(); err != nil {
		return err
	}

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
		if _, err := p.meta().ExecContext(p.context(), createSQL); err != nil {
			return fmt.Errorf("创建表 %s 失败: %w, SQL: %s", table.tableName, err, createSQL)
		}
		p.updateTableExistsCache(table.tableName, true)
	}

	// 表已存在，同步字段结构。
	//
	// 规划（读到的线上结构 → 要下发的语句）全部交给 planSchemaAlignment，
	// 与 GenerateMigrationSQL、GormDB.CreateOrUpdateTable **共用同一份逻辑**：
	// 原先只有这条路径会补索引和主键，另外两条只做列对齐，同一份 proto 在
	// "生成迁移脚本给人审"与"进程自己同步"两边产出的结构不一样，而且零提示。
	//
	// 计划分两条 ALTER（列+索引 / 补主键），理由见 schemaPlan 的注释：
	// TiDB 不支持给已存在的列加 AUTO_INCREMENT（Error 8200），合并成一条会让
	// 所有 ADD COLUMN 陪葬——服务启动"成功"，第一条 SELECT 报 Error 1054。
	currentCols, indexes, indexesKnown, currentPK, err := p.readSchemaState(registryKey, table)
	if err != nil {
		return err
	}
	plan, err := table.planSchemaAlignment(currentCols, indexes, indexesKnown, currentPK, p.ExpandOnly)
	if err != nil {
		return err
	}
	if plan.empty() {
		return nil
	}

	// 第一条：列 + 索引
	if len(plan.columns) > 0 {
		alterSQL := fmt.Sprintf("ALTER TABLE %s %s", escapeMySQLName(table.tableName), strings.Join(plan.columns, ", "))
		if _, err := p.meta().ExecContext(p.context(), alterSQL); err != nil {
			return fmt.Errorf("更新表 %s 结构失败: %w, SQL: %s", table.tableName, err, alterSQL)
		}
		p.clearColumnCache(registryKey) // 清除缓存，下次查询时重新加载字段
		p.awaitSchemaVisible(registryKey, table, plan.columns)
	}

	// 第二条：补主键。失败时列已经对齐好了，服务能跑。
	if len(plan.primaryKey) > 0 {
		pkSQL := fmt.Sprintf("ALTER TABLE %s %s", escapeMySQLName(table.tableName), strings.Join(plan.primaryKey, ", "))
		if _, err := p.meta().ExecContext(p.context(), pkSQL); err != nil {
			return fmt.Errorf("表 %s 的列已对齐，但补齐主键失败: %w\n"+
				"  SQL: %s\n"+
				"  两个常见原因：\n"+
				"    1) 线上已有重复行——先人工去重再重试（fail-closed，不会静默跳过）\n"+
				"    2) 后端是 TiDB——它**不支持给已存在的列加 AUTO_INCREMENT**\n"+
				"       （Error 8200），只能重建表或去掉 auto_increment_key 选项",
				table.tableName, err, pkSQL)
		}
		p.clearColumnCache(registryKey)
		p.awaitSchemaVisible(registryKey, table, plan.primaryKey)
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

// indexColumnMeta 是索引中的一列。sequence / subPart 分别对应
// INFORMATION_SCHEMA.STATISTICS.SEQ_IN_INDEX / SUB_PART。
type indexColumnMeta struct {
	name     string
	sequence int
	subPart  sql.NullInt64
}

// indexMeta 是本库当前支持校验的索引定义维度。只保留名字远远不够：同名索引可能在
// 唯一性、列顺序或 TEXT/BLOB 前缀长度上与 proto 声明不一致。
// MySQL 8 的 IS_VISIBLE、INDEX_TYPE 与升降序目前不在跨 5.7 的查询契约内，见审计文档。
type indexMeta struct {
	unique  bool
	columns []indexColumnMeta
}

// lookupIndexMeta 按 MySQL 的索引名语义查找，同时保留 information_schema 返回的
// 实际名字。MySQL 的索引名不区分大小写；直接用 map 下标会把线上 IDX_T_0 和声明的
// idx_t_0 当成两条索引，随后错误地产生 ADD INDEX，并在执行时撞 Error 1061。
func lookupIndexMeta(indexes map[string]indexMeta, name string) (indexMeta, bool) {
	if meta, ok := indexes[name]; ok {
		return meta, true
	}
	for actualName, meta := range indexes {
		if strings.EqualFold(actualName, name) {
			return meta, true
		}
	}
	return indexMeta{}, false
}

func hasIndexName(indexes map[string][]string, name string) bool {
	if _, ok := indexes[name]; ok {
		return true
	}
	for actualName := range indexes {
		if strings.EqualFold(actualName, name) {
			return true
		}
	}
	return false
}

// existingIndexNames 读取线上二级索引（不含 PRIMARY）的唯一性、列顺序和前缀长度。
//
// ⚠️ 只按名字判断"索引已存在"是不够的。索引名是本库自己拼的（idx_<表名>_<序号>），
// 而**序号是 proto 里索引的声明顺序**：在 .proto 的 index 列表中间插一条，
// 后面所有索引的名字都往后挪一位——于是 idx_t_1 这个名字依然存在，
// 但它现在对应的是另外一组列。本库看名字命中就跳过，**新声明的索引永远建不出来**，
// 而线上那条 idx_t_1 还盖着旧列。零日志，只是查询悄悄走全表扫描。
// 手工加过同名索引的 DBA 也会撞上同一件事。
//
// 所以把唯一性、列顺序和前缀长度一起读回来做 fail-closed 比对；错配时绝不自动
// DROP/重建索引（那是破坏性操作，且在滚动发布下会被新旧副本来回执行）。
func (p *DB) existingIndexNames(tableName string) (map[string]indexMeta, error) {
	rows, err := p.meta().QueryContext(p.context(),
		"SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART "+
			"FROM INFORMATION_SCHEMA.STATISTICS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME <> 'PRIMARY' "+
			"ORDER BY INDEX_NAME, SEQ_IN_INDEX",
		p.DBName, tableName)
	if err != nil {
		return nil, fmt.Errorf("query indexes for table %s: %w", tableName, err)
	}
	defer rows.Close()

	indexes := make(map[string]indexMeta)
	for rows.Next() {
		var name string
		var nonUnique, sequence int
		var column sql.NullString
		var subPart sql.NullInt64
		if err := rows.Scan(&name, &nonUnique, &sequence, &column, &subPart); err != nil {
			return nil, fmt.Errorf("scan index metadata for table %s: %w", tableName, err)
		}
		if name == "" {
			continue
		}
		meta, ok := indexes[name]
		unique := nonUnique == 0
		if ok && meta.unique != unique {
			return nil, fmt.Errorf("index metadata for table %s index %s has inconsistent NON_UNIQUE values", tableName, name)
		}
		meta.unique = unique
		meta.columns = append(meta.columns, indexColumnMeta{
			name:     column.String,
			sequence: sequence,
			subPart:  subPart,
		})
		indexes[name] = meta
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate index metadata for table %s: %w", tableName, err)
	}
	return indexes, nil
}

// primaryKeyMetadata 读取当前 PRIMARY 的列顺序和前缀长度；无主键返回 nil。
// KEY_COLUMN_USAGE 没有 SUB_PART，无法判断 string/blob 主键的前缀是否与声明一致，
// 因此这里与二级索引一样从 STATISTICS 读取。
func (p *DB) primaryKeyMetadata(tableName string) (*indexMeta, error) {
	rows, err := p.meta().QueryContext(p.context(),
		"SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART "+
			"FROM INFORMATION_SCHEMA.STATISTICS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' "+
			"ORDER BY SEQ_IN_INDEX", p.DBName, tableName)
	if err != nil {
		return nil, fmt.Errorf("query primary key metadata for table %s: %w", tableName, err)
	}
	defer rows.Close()

	var primary *indexMeta
	for rows.Next() {
		var name string
		var nonUnique, sequence int
		var col sql.NullString
		var subPart sql.NullInt64
		if err := rows.Scan(&name, &nonUnique, &sequence, &col, &subPart); err != nil {
			return nil, fmt.Errorf("scan primary key metadata for table %s: %w", tableName, err)
		}
		unique := nonUnique == 0
		if primary == nil {
			primary = &indexMeta{unique: unique}
		} else if primary.unique != unique {
			return nil, fmt.Errorf("primary key metadata for table %s has inconsistent NON_UNIQUE values", tableName)
		}
		primary.columns = append(primary.columns, indexColumnMeta{
			name:     col.String,
			sequence: sequence,
			subPart:  subPart,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate primary key metadata for table %s: %w", tableName, err)
	}
	return primary, nil
}

// validateSchemaDrift 对无法靠 expand-only DDL 安全修复的错配 fail closed。
//
// nullable/default/auto_increment 的自动修复都需要 MODIFY COLUMN；索引或主键错配则
// 需要 DROP 后重建。它们在滚动发布期间既可能翻面，也可能改坏存量数据，因此这里只返回
// ErrSchemaDrift，绝不把破坏性 ALTER 塞进计划。缺失列、索引和主键仍由原有 ADD 策略补齐。
func (m *MessageTable) validateSchemaDrift(
	currentCols map[string]columnMeta,
	existingIndexes map[string]indexMeta, indexesKnown bool,
	currentPK *indexMeta,
) error {
	var drifts []string
	var legacy []legacyKeyColumn
	fields := m.Descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		fieldDesc := fields.Get(i)
		name := string(fieldDesc.Name())
		onlineName, meta, ok, err := m.lookupColumnMeta(currentCols, name)
		if err != nil {
			return err
		}
		if !ok {
			onlineName = name
		}
		if !ok {
			// 改名路径按 pb:N 识别旧列；属性漂移必须在 CHANGE COLUMN 前检查旧列，
			// 不能因为新名字尚不存在就把 NULL/DEFAULT/AUTO_INCREMENT 全部跳过。
			for candidateName, candidate := range currentCols {
				if candidate.fieldNum != fieldDesc.Number() {
					continue
				}
				if !ok || candidateName < onlineName {
					onlineName, meta, ok = candidateName, candidate, true
				}
			}
		}
		if !ok || !meta.metadataComplete {
			continue // 这一列本轮会被 ADD COLUMN 建出来，属性自然是对的
		}
		displayName := name
		if onlineName != name {
			displayName = onlineName + "->" + name
		}

		// 键列先认形态。旧形态（TEXT/BLOB 前缀索引、*_ci 或 PAD SPACE 排序规则）的唯一性语义本身就不对，
		// 再拿 NOT NULL/DEFAULT ''/整列索引去逐项比较只会给出误导性的修法，所以这些列跳过通用比较。
		// 只看列元数据、不依赖 indexesKnown：proto 没声明二级索引时，旧形态主键同样必须拦下。
		if kind, isKey := m.keyColumnKind(fieldDesc); isKey {
			if reason := legacyKeyColumnReason(kind, meta); reason != "" {
				legacy = append(legacy, legacyKeyColumn{field: fieldDesc, onlineName: onlineName, meta: meta, reason: reason})
				continue
			}
		}

		targetType := m.getMySQLFieldType(fieldDesc)
		wantNullable := !strings.Contains(strings.ToUpper(targetType), "NOT NULL")
		if meta.nullable != wantNullable {
			drifts = append(drifts, fmt.Sprintf(
				"column %s nullable mismatch (online=%t, proto=%t)", displayName, meta.nullable, wantNullable))
		}

		wantDefault := expectedColumnDefault(targetType)
		if !columnDefaultsEqual(meta.defaultValue, wantDefault) {
			drifts = append(drifts, fmt.Sprintf(
				"column %s default mismatch (online=%s, proto=%s)",
				displayName, formatColumnDefault(meta.defaultValue), formatColumnDefault(wantDefault)))
		}

		wantAutoInc := m.isAutoIncrementField(name)
		if meta.isAutoIncrement() != wantAutoInc {
			drifts = append(drifts, fmt.Sprintf(
				"column %s auto_increment mismatch (online=%t, proto=%t)",
				displayName, meta.isAutoIncrement(), wantAutoInc))
		}
	}

	// 旧形态列所在的索引要随迁移一起删掉重建，前缀长度不对是必然的，不再单独报索引漂移。
	legacyNames := make(map[string]struct{}, len(legacy))
	for _, col := range legacy {
		legacyNames[string(col.field.Name())] = struct{}{}
	}

	if indexesKnown {
		for idx, indexCols := range m.indexes {
			if columnsReferenceAny(splitTrimmed(indexCols), legacyNames) {
				continue
			}
			name := m.indexNameFor(idx)
			online, ok := lookupIndexMeta(existingIndexes, name)
			if !ok {
				continue // 缺的那些由 missingIndexClauses 补
			}
			want := m.expectedIndexMeta(indexCols, false)
			if !indexMetaEqual(online, want) {
				drifts = append(drifts, fmt.Sprintf(
					"index %s definition mismatch (online=%s, proto=%s)",
					name, formatIndexMeta(online), formatIndexMeta(want)))
			}
		}
		if m.uniqueKeys != "" && !columnsReferenceAny(splitTrimmed(m.uniqueKeys), legacyNames) {
			name := m.uniqueKeyName()
			if online, ok := lookupIndexMeta(existingIndexes, name); ok {
				want := m.expectedIndexMeta(m.uniqueKeys, true)
				if !indexMetaEqual(online, want) {
					drifts = append(drifts, fmt.Sprintf(
						"unique index %s definition mismatch (online=%s, proto=%s)",
						name, formatIndexMeta(online), formatIndexMeta(want)))
				}
			}
		}
	}

	if len(m.primaryKey) > 0 && currentPK != nil && !columnsReferenceAny(m.primaryKey, legacyNames) {
		want := m.expectedIndexMeta(strings.Join(m.primaryKey, ","), true)
		if !indexMetaEqual(*currentPK, want) {
			drifts = append(drifts, fmt.Sprintf(
				"primary key definition mismatch (online=%s, proto=%s)",
				formatIndexMeta(*currentPK), formatIndexMeta(want)))
		}
	}

	if len(legacy) > 0 {
		return m.legacyKeyColumnError(legacy, drifts, currentCols, existingIndexes, indexesKnown)
	}
	if len(drifts) > 0 {
		return fmt.Errorf("%w: table %s: %s", ErrSchemaDrift, m.tableName, strings.Join(drifts, "; "))
	}
	return nil
}

// expectedColumnDefault proto 目标类型对应的 COLUMN_DEFAULT：数值列 "0"，键列 ""（空串而非 NULL），其余 NULL。
func expectedColumnDefault(targetType string) sql.NullString {
	upper := strings.ToUpper(targetType)
	if strings.Contains(upper, " DEFAULT 0") {
		return sql.NullString{String: "0", Valid: true}
	}
	if strings.Contains(upper, " DEFAULT ''") {
		return sql.NullString{String: "", Valid: true}
	}
	return sql.NullString{}
}

// columnDefaultsEqual 比较前去掉首尾空白与一对首尾单引号：键列声明的默认值是空串，MySQL 与 TiDB
// 回读成空串，也有兼容实现回读成两个单引号组成的字面量，二者是同一个默认值。
func columnDefaultsEqual(online, want sql.NullString) bool {
	if online.Valid != want.Valid {
		return false
	}
	if !online.Valid {
		return true
	}
	return strings.EqualFold(unquoteColumnDefault(online.String), unquoteColumnDefault(want.String))
}

func unquoteColumnDefault(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
		return value[1 : len(value)-1]
	}
	return value
}

func formatColumnDefault(value sql.NullString) string {
	if !value.Valid {
		return "NULL"
	}
	return fmt.Sprintf("%q", value.String)
}

func (m *MessageTable) expectedIndexMeta(columns string, unique bool) indexMeta {
	names := splitTrimmed(columns)
	meta := indexMeta{unique: unique, columns: make([]indexColumnMeta, 0, len(names))}
	for i, name := range names {
		column := indexColumnMeta{name: name, sequence: i + 1}
		if m.needsIndexPrefix(name) {
			column.subPart = sql.NullInt64{Int64: int64(TextIndexPrefixLength), Valid: true}
		}
		meta.columns = append(meta.columns, column)
	}
	return meta
}

func indexMetaEqual(online, want indexMeta) bool {
	if online.unique != want.unique || len(online.columns) != len(want.columns) {
		return false
	}
	for i := range online.columns {
		a, b := online.columns[i], want.columns[i]
		if a.sequence != b.sequence || !strings.EqualFold(a.name, b.name) ||
			a.subPart.Valid != b.subPart.Valid || (a.subPart.Valid && a.subPart.Int64 != b.subPart.Int64) {
			return false
		}
	}
	return true
}

func formatIndexMeta(meta indexMeta) string {
	kind := "INDEX"
	if meta.unique {
		kind = "UNIQUE"
	}
	columns := make([]string, 0, len(meta.columns))
	for _, column := range meta.columns {
		name := column.name
		if column.subPart.Valid {
			name = fmt.Sprintf("%s(%d)", name, column.subPart.Int64)
		}
		columns = append(columns, fmt.Sprintf("%d:%s", column.sequence, name))
	}
	return fmt.Sprintf("%s(%s)", kind, strings.Join(columns, ","))
}

// splitTrimmed 把 "a, b ,c" 拆成 ["a","b","c"]。
func splitTrimmed(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
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
		return nil, err
	}
	return table.missingIndexClauses(indexNameSet(existing)), nil
}

func indexNameSet(indexes map[string]indexMeta) map[string][]string {
	names := make(map[string][]string, len(indexes))
	for name := range indexes {
		names[name] = nil
	}
	return names
}

// missingIndexClauses 纯计算版：给定线上既有索引名的集合，算出还缺哪些。
// 与 DB / GormDB 两条读元信息的路径解耦，好让三条迁移路径共用同一份规划逻辑。
func (m *MessageTable) missingIndexClauses(existing map[string][]string) []string {
	table := m
	var clauses []string
	for idx, indexCols := range table.indexes {
		name := table.indexNameFor(idx)
		if hasIndexName(existing, name) {
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
		name := table.uniqueKeyName()
		if !hasIndexName(existing, name) {
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
	return clauses
}

// schemaPlan 一次结构对齐要下发的全部语句，已经按**必须的执行顺序**分成两条 ALTER。
//
// 为什么是两条而不是一条：主键列常带 AUTO_INCREMENT，而 MySQL 要求自增列必须是键，
// 所以那条列变更必须与 ADD PRIMARY KEY 同句；但 TiDB 又根本不支持给已存在的列加
// AUTO_INCREMENT（Error 8200），合并成一条会让**所有 ADD COLUMN 陪葬**——
// 服务启动"成功"，第一条 SELECT 报 Error 1054。拆开之后，补主键失败时列已经对齐好了。
type schemaPlan struct {
	// columns 第一条 ALTER：列对齐（ADD / MODIFY / CHANGE COLUMN）+ 补索引。
	columns []string
	// primaryKey 第二条 ALTER：主键列自己的列变更 + ADD PRIMARY KEY。
	primaryKey []string
}

func (s schemaPlan) empty() bool { return len(s.columns) == 0 && len(s.primaryKey) == 0 }

// statements 按执行顺序拼出完整 ALTER 语句（0~2 条）。
func (s schemaPlan) statements(tableName string) []string {
	var out []string
	if len(s.columns) > 0 {
		out = append(out, fmt.Sprintf("ALTER TABLE %s %s",
			escapeMySQLName(tableName), strings.Join(s.columns, ", ")))
	}
	if len(s.primaryKey) > 0 {
		out = append(out, fmt.Sprintf("ALTER TABLE %s %s",
			escapeMySQLName(tableName), strings.Join(s.primaryKey, ", ")))
	}
	return out
}

// planSchemaAlignment 把「线上结构」与「proto 定义」的差异规划成 schemaPlan。
//
// 纯计算，不碰数据库：读 information_schema 的活由调用方按自己的方式做（DB 走
// database/sql、GormDB 走 GORM），规划逻辑只有这一份。**三条迁移路径必须都走它**——
// 原先 DB.syncTableSchema 会补索引和主键，而 GenerateMigrationSQL 与
// GormDB.CreateOrUpdateTable 只做列对齐：同一份 proto、同一个库，
// "生成迁移脚本给人审"和"进程自己同步"产出的结构是不一样的，而这个差异零提示。
//
// indexesKnown 为 false 只表示 proto 没声明二级索引、因此调用方没有执行那次查询；
// 一旦需要读取索引，任何读取失败都会在 readSchemaState 直接返回，不能把"元数据未知"
// 当成"结构已对齐"。
func (m *MessageTable) planSchemaAlignment(
	currentCols map[string]columnMeta,
	existingIndexes map[string]indexMeta, indexesKnown bool,
	currentPK *indexMeta, expandOnly bool,
) (schemaPlan, error) {
	// ExpandOnly 的判定推迟到最后统一做（见 checkExpandOnly），
	// 所以这里先按 false 拿到全部列子句。
	columnClauses, err := m.buildColumnClauses(currentCols, false)
	if err != nil {
		return schemaPlan{}, err
	}

	if err := m.validateSchemaDrift(currentCols, existingIndexes, indexesKnown, currentPK); err != nil {
		return schemaPlan{}, err
	}

	var plan schemaPlan
	movedAutoIncrementColumns := make(map[string]struct{})

	// 补主键：只补"从无到有"，绝不自动 DROP / 改写已有主键。
	if len(m.primaryKey) > 0 && currentPK == nil {
		pkCols := make([]string, len(m.primaryKey))
		for i, pk := range m.primaryKey {
			pkCols[i] = m.indexColumn(pk)
		}

		// 把「主键列自己的、带 AUTO_INCREMENT 的那条列变更」挪到主键这一条来。
		// 判据是 (列名 ∈ 主键) && 带 AUTO_INCREMENT，三种子句形态（ADD / MODIFY /
		// CHANGE COLUMN）一视同仁——只认 MODIFY 前缀的老判据会漏掉另外两种，
		// 让它们在主键还不存在时带着 AUTO_INCREMENT 下发，报 Error 1075。
		//
		// 不带 AUTO_INCREMENT 的主键列变更**不用挪**：MODIFY 一个普通列不要求它是键，
		// 留在第一条里反而更好——补主键那条失败时它已经生效了。
		kept := columnClauses[:0:0]
		for _, c := range columnClauses {
			if c.autoIncrement && slices.Contains(m.primaryKey, c.column) {
				plan.primaryKey = append(plan.primaryKey, c.sql)
				movedAutoIncrementColumns[c.column] = struct{}{}
				continue
			}
			kept = append(kept, c)
		}
		columnClauses = kept

		pkClause := fmt.Sprintf("ADD PRIMARY KEY (%s)", strings.Join(pkCols, ","))
		if m.tidbNonclusteredPK {
			// TiDB 补主键只能是非聚簇（省略关键字时默认即非聚簇），显式注释仅为语义自文档化；MySQL 视为注释忽略
			pkClause += tidbNonclusteredPKSQL
		}
		plan.primaryKey = append(plan.primaryKey, pkClause)
		log.Printf("table %s is missing its primary key; adding %s", m.tableName, strings.Join(pkCols, ","))
	}

	plan.columns = alterClauseSQLs(columnClauses)
	if indexesKnown {
		for _, clause := range m.missingIndexClauses(indexNameSet(existingIndexes)) {
			if indexClauseReferencesAnyColumn(clause, movedAutoIncrementColumns) {
				// 自增主键列尚不存在时，依赖它的索引不能留在第一条 ALTER，
				// 否则 MySQL 先报 Error 1072，第二条永远没有机会补列和主键。
				plan.primaryKey = append(plan.primaryKey, clause)
				continue
			}
			plan.columns = append(plan.columns, clause)
		}
	}

	if expandOnly {
		if err := m.checkExpandOnly(append(append([]string{}, plan.columns...), plan.primaryKey...)); err != nil {
			return schemaPlan{}, err
		}
	}
	return plan, nil
}

func indexClauseReferencesAnyColumn(clause string, columns map[string]struct{}) bool {
	for column := range columns {
		if strings.Contains(clause, escapeMySQLName(column)) {
			return true
		}
	}
	return false
}

// readSchemaState 读线上结构：列元信息，以及二级索引/主键的受支持校验维度。
// indexesKnown 为 false 仅表示 proto 没声明二级索引、也不会用到线上二级索引，没有必要查询；
// 需要查询却失败时必须返回错误，不能在元数据未知时继续 ADD 或宣告同步成功。
func (p *DB) readSchemaState(registryKey string, table *MessageTable) (
	cols map[string]columnMeta, indexes map[string]indexMeta, indexesKnown bool, currentPK *indexMeta, err error,
) {
	cols, err = p.getTableColumnMeta(registryKey)
	if err != nil {
		return nil, nil, false, nil, fmt.Errorf("获取表 %s 字段: %w", registryKey, err)
	}

	// 主键里有 string/bytes 键列时即使 proto 一条二级索引都没声明也要读：线上若是旧形态，
	// 迁移只能走影子表重建（见 legacyKeyRebuildSQL），而影子表必须把线上那些本库未声明的
	// 索引一起带过去，否则 RENAME 之后它们就没了。
	if len(table.indexes) > 0 || table.uniqueKeys != "" || table.primaryKeyHasKeyColumn() {
		indexes, err = p.existingIndexNames(table.tableName)
		if err != nil {
			return nil, nil, false, nil, fmt.Errorf("读取表 %s 的既有索引: %w", table.tableName, err)
		}
		indexesKnown = true
	}

	if len(table.primaryKey) > 0 {
		currentPK, err = p.primaryKeyMetadata(table.tableName)
		if err != nil {
			return nil, nil, false, nil, fmt.Errorf("检查表 %s 主键: %w", table.tableName, err)
		}
	}
	return cols, indexes, indexesKnown, currentPK, nil
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
	if err := p.meta().QueryRowContext(p.context(), query, p.DBName, tableName).Scan(&count); err != nil {
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
	err := p.meta().QueryRowContext(p.context(), query, p.DBName, tableName).Scan(&count)
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
		val, err := m.serializeColumnValue(message, fieldDesc)
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
			val, err := m.serializeColumnValue(msg, fieldDesc)
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

// GetInsertOnDupUpdateSQLWithArgs 生成参数化的 INSERT...ON DUPLICATE KEY UPDATE。
// 低层兼容 API 同样不得被备用 UNIQUE 劫持：跳过主键列，并用完整主键守卫每个更新。
// 若需要把备用 UNIQUE 冲突识别为 ErrDuplicateKey，应使用 DB.InsertOnDupUpdate / Save。
func (m *MessageTable) GetInsertOnDupUpdateSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	insertSQL, err := m.GetInsertSQLWithArgs(message)
	if insertSQL == nil || err != nil {
		return nil, err
	}
	identity, err := m.saveIdentityPredicate()
	if err != nil {
		return nil, err
	}
	pkSet := make(map[string]bool, len(m.primaryKey))
	for _, pk := range m.primaryKey {
		pkSet[strings.TrimSpace(pk)] = true
	}

	var updateClauses []string
	var updateArgs []interface{}
	reflection := message.ProtoReflect()

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		name := string(fieldDesc.Name())
		if pkSet[name] {
			continue
		}
		if !reflection.Has(fieldDesc) {
			continue
		}
		val, err := m.serializeColumnValue(message, fieldDesc)
		if err != nil {
			return nil, fmt.Errorf("serialize update field %s: %w", fieldDesc.Name(), err)
		}
		escaped := escapeMySQLName(name)
		updateClauses = append(updateClauses, fmt.Sprintf("%s = IF(%s, ?, %s)", escaped, identity, escaped))
		updateArgs = append(updateArgs, val)
	}

	if len(updateClauses) == 0 {
		pk := escapeMySQLName(strings.TrimSpace(m.primaryKey[0]))
		updateClauses = append(updateClauses, pk+" = "+pk)
	}

	updateClauseStr := strings.Join(updateClauses, ", ")
	fullSQL := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSQL.Sql, updateClauseStr)
	fullArgs := append(insertSQL.Args, updateArgs...)

	return &SqlWithArgs{Sql: fullSQL, Args: fullArgs}, nil
}

// GetInsertOnDupKeyForPrimaryKeyWithArgs 生成冲突时保持原行不变的兼容语句。
// 不能把入参主键写回：冲突若来自备用 UNIQUE，会把另一行的身份改掉。
func (m *MessageTable) GetInsertOnDupKeyForPrimaryKeyWithArgs(message proto.Message) (*SqlWithArgs, error) {
	if m.primaryKeyField == nil {
		return nil, ErrPrimaryKeyNotFound
	}

	insertSQL, err := m.GetInsertSQLWithArgs(message)
	if insertSQL == nil || err != nil {
		return nil, err
	}

	primaryKeyName := string(m.primaryKeyField.Name())
	escaped := escapeMySQLName(primaryKeyName)
	updateClause := escaped + " = " + escaped
	fullSQL := fmt.Sprintf("%s ON DUPLICATE KEY UPDATE %s", insertSQL.Sql, updateClause)

	return &SqlWithArgs{Sql: fullSQL, Args: insertSQL.Args}, nil
}

// Insert 执行参数化的INSERT操作（直接用DB，无Tx）
func (p *DB) Insert(message proto.Message) error {
	tableName := GetTableName(message)
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	sqlWithArgs, err := table.GetInsertSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate insert SQL for table %s: %w", tableName, err)
	}

	_, err = p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec insert for table %s: sql=%s, args len=%d, err=%w",
			tableName, sqlWithArgs.Sql, len(sqlWithArgs.Args), wrapExecErr(err))
	}
	return nil
}

// BatchInsert 执行批量INSERT操作（直接用DB，无Tx）
func (p *DB) BatchInsert(messages []proto.Message) error {
	if len(messages) == 0 {
		return errors.New("no messages to insert")
	}

	// 分批写入不是原子的：先按同样的分批方式把整批键列值校验完再开始写，见 validateKeyValues。
	for i := 0; i < len(messages); i += BatchInsertMaxSize {
		end := i + BatchInsertMaxSize
		if end > len(messages) {
			end = len(messages)
		}
		table, err := p.tableForMessage(messages[i])
		if err != nil {
			return err
		}
		if err := table.validateKeyValues(messages[i:end], false); err != nil {
			return fmt.Errorf("batch insert rows from %d for table %s: %w", i, table.tableName, err)
		}
	}

	// 分批处理大批量数据
	for i := 0; i < len(messages); i += BatchInsertMaxSize {
		end := i + BatchInsertMaxSize
		if end > len(messages) {
			end = len(messages)
		}
		batch := messages[i:end]

		tableName := GetTableName(batch[0])
		table, err := p.tableForMessage(batch[0])
		if err != nil {
			return err
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

// InsertIgnore 幂等插入：只把 MySQL 1062（主键/唯一键冲突）解释成“未插入”。
// 不能使用 INSERT IGNORE；它还会把截断、越界、NOT NULL 等真实错误降级成 warning。
func (p *DB) InsertIgnore(message proto.Message) (bool, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return false, err
	}

	insertSQL, err := table.GetInsertSQLWithArgs(message)
	if insertSQL == nil || err != nil {
		return false, fmt.Errorf("generate insert SQL for table %s: %w", table.tableName, err)
	}

	result, err := p.conn().Exec(insertSQL.Sql, insertSQL.Args...)
	if err != nil {
		wrapped := wrapExecErr(err)
		if errors.Is(wrapped, ErrDuplicateKey) {
			return false, nil
		}
		return false, fmt.Errorf("exec insert ignore for table %s: %w", table.tableName, wrapped)
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

// InsertOnDupUpdate 保留历史名称，执行与 Save 相同的完整主键精确语义。
// 通用 ODKU 无法区分主键冲突与备用 UNIQUE 冲突；后者命中别人的行时必须返回
// ErrDuplicateKey，而不是改写那一行或留下旧主键缓存。
func (p *DB) InsertOnDupUpdate(message proto.Message) error {
	return p.Save(message)
}

// GetSelectSQLByKVWithArgs 生成参数化的KV查询语句
func (m *MessageTable) GetSelectSQLByKVWithArgs(whereKey, whereVal string) (*SqlWithArgs, error) {
	value, err := m.normalizeColumnComparisonValue(whereKey, whereVal)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf("%s WHERE %s = ?;", m.selectFieldsSQL, escapeMySQLName(whereKey))
	return &SqlWithArgs{Sql: sql, Args: []interface{}{value}}, nil
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
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	sqlWithArgs, err := table.GetDeleteSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return fmt.Errorf("generate delete SQL for table %s: %w", tableName, err)
	}

	_, err = p.conn().Exec(sqlWithArgs.Sql, sqlWithArgs.Args...)
	if err != nil {
		return fmt.Errorf("exec delete for table %s: sql=%s, args len=%d, err=%w",
			tableName, sqlWithArgs.Sql, len(sqlWithArgs.Args), err)
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
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}
	value, err = table.normalizeColumnComparisonValue(key, value)
	if err != nil {
		return err
	}
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
	if err := table.validateKeyValues(messages, true); err != nil {
		return fmt.Errorf("batch delete for table %s: %w", table.tableName, err)
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
		// 与 BatchSave 同理：分批不是原子的，删成功一批就失效一批，
		// 否则中途失败时前面那些批的缓存会留着已被删除的行。
		p.invalidateMessages(table, batch...)
	}
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
		val, err := table.serializeColumnValue(message, desc)
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

	if err := table.requireNumericColumn(versionField); err != nil {
		return false, err
	}
	curVersion, err := table.comparisonValue(message, versionDesc)
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
		val, err := table.serializeColumnValue(message, field)
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
	if err := table.requireNumericColumn(versionField); err != nil {
		return false, err
	}
	curVersion, err := table.comparisonValue(message, versionDesc)
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
		val, err := table.serializeColumnValue(message, desc)
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

// saveIdentityPredicate 生成“冲突命中的线上行就是入参主键那一行”的守卫。
// `<=>` 是 MySQL 的 NULL-safe equal；主键正常不会是 NULL，但用它可避免三值逻辑把
// 守卫结果变成 NULL。Save 没有主键时无法定义稳定身份，必须 fail-closed。
func (m *MessageTable) saveIdentityPredicate() (string, error) {
	if len(m.primaryKey) == 0 {
		return "", ErrPrimaryKeyNotFound
	}
	parts := make([]string, 0, len(m.primaryKey))
	for _, raw := range m.primaryKey {
		pk := strings.TrimSpace(raw)
		if _, ok := m.fieldNameToDesc[pk]; !ok {
			return "", fmt.Errorf("%w: primary key %s in table %s", ErrFieldNotFound, pk, m.tableName)
		}
		name := escapeMySQLName(pk)
		parts = append(parts, name+" <=> VALUES("+name+")")
	}
	return strings.Join(parts, " AND "), nil
}

// valuesUpdateClause 生成带主键身份守卫的 ODKU 列表，覆盖本进程认识的全部非主键列。
//
// ON DUPLICATE KEY UPDATE 只动子句里点名的列，本进程不认识的列原样保留——
// 这正是它比 REPLACE 安全的地方。
//
// 注意：VALUES() 在 MySQL 8.0.20 起被标记 deprecated（官方建议改 AS new 行别名），
// 至今仍可用，本库沿用它以兼容 5.7（与 sqlbuilder 里的 SetNew 等一致）。
// ⚠️ **主键列绝不进这个子句。**
//
// ODKU 会在任意 UNIQUE 冲突时触发，所以每个赋值都用完整主键谓词保护；命中备用
// UNIQUE 的另一行时，所有列都保持原值。这个低层兼容 helper 无法把这种 no-op 可靠地
// 报成 ErrDuplicateKey；需要完整语义时使用 DB.Save / InsertOnDupUpdate，它们按完整主键
// UPDATE → INSERT，并能区分备用 UNIQUE 冲突。
func (m *MessageTable) valuesUpdateClause() (string, error) {
	identity, err := m.saveIdentityPredicate()
	if err != nil {
		return "", err
	}
	pkSet := make(map[string]bool, len(m.primaryKey))
	for _, pk := range m.primaryKey {
		pkSet[strings.TrimSpace(pk)] = true
	}

	parts := make([]string, 0, m.Descriptor.Fields().Len())
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		raw := string(m.Descriptor.Fields().Get(i).Name())
		if pkSet[raw] {
			continue
		}
		name := escapeMySQLName(raw)
		parts = append(parts, name+" = IF("+identity+", VALUES("+name+"), "+name+")")
	}

	if len(parts) == 0 {
		// 整张表只有主键列（纯联合主键表）。ODKU 没有可更新的东西，但子句不能为空
		// （`ON DUPLICATE KEY UPDATE ` 后面没内容是语法错），用一条合法 no-op 占位——
		// 语义正是"这一行已经存在，什么都不用改"。写法与 SQLBuilder.KeepOld 一致。
		for _, pk := range m.primaryKey {
			if _, ok := m.fieldNameToDesc[strings.TrimSpace(pk)]; ok {
				e := escapeMySQLName(strings.TrimSpace(pk))
				return e + " = " + e, nil
			}
		}
		return "", ErrPrimaryKeyNotFound
	}
	return strings.Join(parts, ", "), nil
}

// GetSaveSQLWithArgs 生成兼容的单语句整行落库 SQL；每个更新都受完整主键守卫。
//
// 取代 REPLACE INTO 作为 Save 的实现——同样是「有则更新、无则插入」，
// 但**不会清掉本进程不认识的列**。
func (m *MessageTable) GetSaveSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	stmt, err := m.GetInsertSQLWithArgs(message)
	if err != nil {
		return nil, err
	}
	clause, err := m.valuesUpdateClause()
	if err != nil {
		return nil, err
	}
	stmt.Sql += " ON DUPLICATE KEY UPDATE " + clause
	return stmt, nil
}

// GetBatchSaveSQLWithArgs 批量整行落库，语义同 GetSaveSQLWithArgs。
func (m *MessageTable) GetBatchSaveSQLWithArgs(messages []proto.Message) (*SqlWithArgs, error) {
	stmt, err := m.GetBatchInsertSQLWithArgs(messages)
	if err != nil {
		return nil, err
	}
	clause, err := m.valuesUpdateClause()
	if err != nil {
		return nil, err
	}
	stmt.Sql += " ON DUPLICATE KEY UPDATE " + clause
	return stmt, nil
}

// getSaveUpdateSQLWithArgs 生成 Save 的第一阶段：按完整主键覆盖本版本认识的全部非主键列。
// 与普通 Update 不同，它必须包含零值/未设置字段，因为 Save 是整行语义。
func (m *MessageTable) getSaveUpdateSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	if err := m.validateMessageDescriptor(message); err != nil {
		return nil, err
	}
	whereClause, whereArgs, err := m.primaryKeyWhere(message)
	if err != nil {
		return nil, err
	}

	pkSet := make(map[string]bool, len(m.primaryKey))
	for _, pk := range m.primaryKey {
		pkSet[strings.TrimSpace(pk)] = true
	}
	clauses := make([]string, 0, m.Descriptor.Fields().Len())
	args := make([]interface{}, 0, m.Descriptor.Fields().Len()+len(whereArgs))
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		name := string(field.Name())
		if pkSet[name] {
			continue
		}
		value, err := m.serializeColumnValue(message, field)
		if err != nil {
			return nil, fmt.Errorf("serialize save field %s: %w", field.Name(), err)
		}
		clauses = append(clauses, escapeMySQLName(name)+" = ?")
		args = append(args, value)
	}
	if len(clauses) == 0 {
		pk := strings.TrimSpace(m.primaryKey[0])
		escaped := escapeMySQLName(pk)
		clauses = append(clauses, escaped+" = "+escaped)
	}
	args = append(args, whereArgs...)
	return &SqlWithArgs{
		Sql:  fmt.Sprintf("UPDATE %s SET %s WHERE %s", escapeMySQLName(m.tableName), strings.Join(clauses, ", "), whereClause),
		Args: args,
	}, nil
}

// getSaveCurrentRowMatchSQLWithArgs 生成 Save 在 INSERT 发生 1062、重试 UPDATE 仍为 0
// 时的最终分类查询。
//
// 这里不能再发普通 ExistsByPK：REPEATABLE READ 事务里的普通 SELECT 是一致性读，可能
// 看不到事务快照建立后由别的事务提交、却已被前一条 UPDATE current read 命中的行。
// FOR UPDATE 强制 current read；同时把全部非主键列放进谓词，避免事务外在“重试 UPDATE
// 返回 0”与分类查询之间刚插入同主键、不同值的行时误报 Save 成功。
//
// values 必须来自已经实际执行过的同一条 UPDATE。map/message 的 protobuf wire 编码可能
// 有多种等价字节序，重新序列化一次再比较会制造假冲突；复用原参数则检查的是数据库当前
// 行是否恰好等于 Save 试图写入的值。
func (m *MessageTable) getSaveCurrentRowMatchSQLWithArgs(update *SqlWithArgs) (*SqlWithArgs, error) {
	if update == nil {
		return nil, errors.New("save update statement cannot be nil")
	}
	if len(m.primaryKey) == 0 {
		return nil, ErrPrimaryKeyNotFound
	}

	pkSet := make(map[string]bool, len(m.primaryKey))
	for _, raw := range m.primaryKey {
		pk := strings.TrimSpace(raw)
		if _, ok := m.fieldNameToDesc[pk]; !ok {
			return nil, fmt.Errorf("%w: primary key %s in table %s", ErrFieldNotFound, pk, m.tableName)
		}
		pkSet[pk] = true
	}

	nonPrimaryCount := 0
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		if !pkSet[string(m.Descriptor.Fields().Get(i).Name())] {
			nonPrimaryCount++
		}
	}
	expectedArgs := nonPrimaryCount + len(m.primaryKey)
	if len(update.Args) != expectedArgs {
		return nil, fmt.Errorf("save update argument count for table %s: got %d, want %d",
			m.tableName, len(update.Args), expectedArgs)
	}

	parts := make([]string, 0, len(m.primaryKey)+nonPrimaryCount)
	args := make([]interface{}, 0, expectedArgs)
	// getSaveUpdateSQLWithArgs 的参数顺序是 [全部非主键值, 全部主键值]；分类查询
	// 先按主键定位，再逐列比较，因此这里把两段重新排序。
	for i, raw := range m.primaryKey {
		pk := strings.TrimSpace(raw)
		parts = append(parts, escapeMySQLName(pk)+" = ?")
		field, ok := m.fieldNameToDesc[pk]
		if !ok {
			return nil, fmt.Errorf("%w: primary key %s in table %s", ErrFieldNotFound, pk, m.tableName)
		}
		arg, err := normalizeComparisonArg(field, update.Args[nonPrimaryCount+i])
		if err != nil {
			return nil, fmt.Errorf("normalize save primary key %s: %w", pk, err)
		}
		args = append(args, arg)
	}

	nonPrimaryArg := 0
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		name := string(field.Name())
		if pkSet[name] {
			continue
		}

		escaped := escapeMySQLName(name)
		if saveValueNeedsBinaryComparison(field) {
			// TEXT 默认通常是大小写不敏感 collation；普通 <=> 会把 "A" 与 "a"
			// 判成相同。Save 是整行写入语义，必须按实际字节确认字符串/BLOB值。
			parts = append(parts, "CAST("+escaped+" AS BINARY) <=> CAST(? AS BINARY)")
		} else if field.Kind() == protoreflect.FloatKind {
			// SerializeFieldValue 为了跨驱动稳定，把标量统一下发为十进制字符串。
			// MySQL 比较 FLOAT 列与字符串参数时会先把列提升精度，导致 0.1 这类值
			// 被误判成不相等。SQL 里的 CAST(... AS FLOAT) 要 MySQL 8.0.17+，本库仍
			// 兼容 5.7；因此保留普通谓词，把复用参数在 Go 侧规范成 float32 精度。
			parts = append(parts, escaped+" <=> ?")
		} else {
			parts = append(parts, escaped+" <=> ?")
		}
		arg, err := saveCurrentRowMatchArg(field, update.Args[nonPrimaryArg])
		if err != nil {
			return nil, fmt.Errorf("normalize save comparison field %s: %w", field.Name(), err)
		}
		args = append(args, arg)
		nonPrimaryArg++
	}

	return &SqlWithArgs{
		Sql: fmt.Sprintf("SELECT 1 FROM %s WHERE %s FOR UPDATE",
			escapeMySQLName(m.tableName), strings.Join(parts, " AND ")),
		Args: args,
	}, nil
}

// saveCurrentRowMatchArg 保留实际 UPDATE 使用过的参数语义，只修正数据库比较所需的类型。
// SerializeFieldValue 为兼容写入路径把标量编码成十进制字符串；比较谓词若继续传字符串，
// MySQL 会把 BIGINT/UNSIGNED 提升成 DOUBLE，超过 2^53 后相邻整数可能被判成相等。
// float32 也必须先按 32 位精度还原，再以 driver 支持的 float64 容器绑定。
func saveCurrentRowMatchArg(field protoreflect.FieldDescriptor, arg interface{}) (interface{}, error) {
	return normalizeComparisonArg(field, arg)
}

// comparisonValue 序列化 message 字段后，把数值标量恢复成带类型的 driver 参数。
// 写入数值列时十进制字符串通常安全，但用于 WHERE/CAS 比较会触发 MySQL 的字符串↔数值
// 隐式 DOUBLE 转换，进而丢失 64 位整数精度。
func (m *MessageTable) comparisonValue(message proto.Message, field protoreflect.FieldDescriptor) (interface{}, error) {
	value, err := m.serializeColumnValue(message, field)
	if err != nil {
		return nil, err
	}
	return normalizeComparisonArg(field, value)
}

// normalizeComparisonArg 根据 protobuf 列类型规范化等值/IN/CAS 谓词参数。
// 容器和消息列实际落在 BLOB/DATETIME 中，必须保持原始序列化值；这里只处理数值标量。
func normalizeComparisonArg(field protoreflect.FieldDescriptor, arg interface{}) (interface{}, error) {
	if field == nil || arg == nil || field.IsMap() || field.IsList() {
		return arg, nil
	}

	text := func() (string, bool) {
		switch value := arg.(type) {
		case string:
			return value, true
		case []byte:
			return string(value), true
		default:
			return "", false
		}
	}

	switch field.Kind() {
	case protoreflect.Int32Kind:
		if raw, ok := text(); ok {
			return strconv.ParseInt(raw, 10, 32)
		}
	case protoreflect.Int64Kind:
		if raw, ok := text(); ok {
			return strconv.ParseInt(raw, 10, 64)
		}
	case protoreflect.Uint32Kind:
		if raw, ok := text(); ok {
			return strconv.ParseUint(raw, 10, 32)
		}
	case protoreflect.Uint64Kind:
		if raw, ok := text(); ok {
			return strconv.ParseUint(raw, 10, 64)
		}
	case protoreflect.FloatKind:
		if raw, ok := text(); ok {
			return strconv.ParseFloat(raw, 32)
		}
		if value, ok := comparisonFloat64(arg); ok {
			return float64(float32(value)), nil
		}
	case protoreflect.DoubleKind:
		if raw, ok := text(); ok {
			return strconv.ParseFloat(raw, 64)
		}
		if value, ok := comparisonFloat64(arg); ok {
			return value, nil
		}
	case protoreflect.BoolKind:
		if raw, ok := text(); ok {
			switch raw {
			case "1":
				return true, nil
			case "0":
				return false, nil
			default:
				return strconv.ParseBool(raw)
			}
		}
	case protoreflect.EnumKind:
		if raw, ok := text(); ok {
			return strconv.ParseInt(raw, 10, 32)
		}
	}
	return arg, nil
}

func comparisonFloat64(value interface{}) (float64, bool) {
	switch value := value.(type) {
	case float32:
		return float64(value), true
	case float64:
		return value, true
	case int:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	default:
		return 0, false
	}
}

func (m *MessageTable) normalizeColumnComparisonValue(column string, value interface{}) (interface{}, error) {
	field, ok := m.fieldNameToDesc[column]
	if !ok {
		return nil, fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, column, m.tableName)
	}
	value, err := normalizeComparisonArg(field, value)
	if err != nil {
		return nil, fmt.Errorf("normalize comparison field %s: %w", column, err)
	}
	return value, nil
}

func (m *MessageTable) normalizeColumnComparisonValues(column string, values []interface{}) ([]interface{}, error) {
	normalized := make([]interface{}, len(values))
	for i, value := range values {
		var err error
		normalized[i], err = m.normalizeColumnComparisonValue(column, value)
		if err != nil {
			return nil, fmt.Errorf("comparison value %d: %w", i, err)
		}
	}
	return normalized, nil
}

func saveValueNeedsBinaryComparison(field protoreflect.FieldDescriptor) bool {
	if field.IsMap() || field.IsList() {
		return true
	}
	switch field.Kind() {
	case protoreflect.StringKind, protoreflect.BytesKind:
		return true
	case protoreflect.MessageKind:
		return field.Message() == nil || field.Message().FullName() != timestampFullName
	default:
		return false
	}
}

// GetReplaceSQLWithArgs 生成参数化的REPLACE语句。
//
// ⚠️ REPLACE 的语义是「先 DELETE 再 INSERT」，语句里没提到的列不是"保持原值"，
// 是**回到列默认值**。而列清单来自本进程的 descriptor，所以滚动发布时旧版本进程
// 执行一次，新版本刚写进去的列就没了；本库「永不 DROP COLUMN」的保护在这里帮不上忙，
// 因为丢的是数据不是列。还会触发外键级联删除。
//
// DB.Save 已经改走完整主键 UPDATE → INSERT，只覆盖本进程认识的列。
// 本方法保留为**显式逃生口**：确实需要「整行推倒重来、未提及列一律归位」时才用。
func (m *MessageTable) GetReplaceSQLWithArgs(message proto.Message) (*SqlWithArgs, error) {
	if err := m.validateMessageDescriptor(message); err != nil {
		return nil, err
	}

	var args []interface{}
	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		fieldDesc := m.Descriptor.Fields().Get(i)
		val, err := m.serializeColumnValue(message, fieldDesc)
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

		val, err := m.serializeColumnValue(message, field)
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
	table, err := p.tableForMessage(message)
	if err != nil {
		return ""
	}
	return table.GetCreateTableSQL()
}

// Save 整行落库：按完整主键有则更新、无则插入。
//
// 不能用普通 ODKU 实现这个语义：MySQL 在任意 UNIQUE 冲突时都会进入 UPDATE，
// 备用唯一键命中的可能是主键不同的另一行。这里采用 UPDATE-by-PK → INSERT；
// INSERT 若遇到并发同主键插入，再重试一次 UPDATE。备用唯一键冲突会原样返回
// ErrDuplicateKey，且不会修改命中的另一行。
func (p *DB) Save(message proto.Message) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}
	if err := p.saveByPrimaryKey(table, message); err != nil {
		return err
	}
	p.invalidateMessages(table, message)
	return nil
}

func (p *DB) saveByPrimaryKey(table *MessageTable, message proto.Message) error {
	update, err := table.getSaveUpdateSQLWithArgs(message)
	if update == nil || err != nil {
		return fmt.Errorf("generate primary-key save update for table %s: %w", table.tableName, err)
	}
	insert, err := table.GetInsertSQLWithArgs(message)
	if insert == nil || err != nil {
		return fmt.Errorf("generate save insert for table %s: %w", table.tableName, err)
	}

	result, err := p.conn().Exec(update.Sql, update.Args...)
	if err != nil {
		return fmt.Errorf("update existing row for save on table %s: %w", table.tableName, wrapExecErr(err))
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read save update result for table %s: %w", table.tableName, err)
	}
	if affected > 0 {
		return nil
	}

	if _, err := p.conn().Exec(insert.Sql, insert.Args...); err == nil {
		return nil
	} else if !errors.Is(wrapExecErr(err), ErrDuplicateKey) {
		return fmt.Errorf("insert missing row for save on table %s: %w", table.tableName, wrapExecErr(err))
	} else {
		// 可能是并发请求刚插入了相同主键，也可能是备用 UNIQUE 撞到另一行。
		// 重试 UPDATE 能安全处理前者；它只按主键，不可能碰到后者那一行。
		duplicateErr := wrapExecErr(err)
		result, retryErr := p.conn().Exec(update.Sql, update.Args...)
		if retryErr != nil {
			return fmt.Errorf("retry primary-key save update for table %s: %w", table.tableName, wrapExecErr(retryErr))
		}
		affected, retryErr = result.RowsAffected()
		if retryErr != nil {
			return fmt.Errorf("read retried save update result for table %s: %w", table.tableName, retryErr)
		}
		if affected > 0 {
			return nil
		}
		match, matchErr := table.getSaveCurrentRowMatchSQLWithArgs(update)
		if matchErr != nil {
			return fmt.Errorf("generate duplicate classification query for save on table %s: %w", table.tableName, matchErr)
		}
		rows, matchErr := p.conn().Query(match.Sql, match.Args...)
		if matchErr != nil {
			return fmt.Errorf("classify duplicate during save on table %s: %w", table.tableName, matchErr)
		}
		matchesCurrentRow := rows.Next()
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil {
			return fmt.Errorf("classify duplicate during save on table %s: %w", table.tableName, rowsErr)
		}
		if matchesCurrentRow {
			// 默认 clientFoundRows=false 时，“同一行、值也完全相同”的 UPDATE 返回 0。
			// FOR UPDATE 是 current read；成功只能说明数据库此刻确实是目标值。
			return nil
		}
		return fmt.Errorf("save on table %s conflicts with a different unique row: %w", table.tableName, duplicateErr)
	}
}

// BatchSave 逐行执行与 Save 相同的主键精确语义。
//
// 单条批量 ODKU 无法区分“同主键冲突”和“备用唯一键撞到另一行”，因此这里刻意不再
// 合并成一条 SQL。方法本来就不是原子的；事务外某行失败时，之前成功的行已经提交并
// 立即失效缓存，事务内则由 RunInTransaction 统一提交/回滚。
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
	if err := table.validateKeyValues(messages, false); err != nil {
		return fmt.Errorf("batch save for table %s: %w", table.tableName, err)
	}

	for i, message := range messages {
		if err := p.saveByPrimaryKey(table, message); err != nil {
			return fmt.Errorf("batch save row %d for table %s: %w", i, table.tableName, err)
		}
		p.invalidateMessages(table, message)
	}
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
	// 复合主键必须 fail-closed。primaryKeyField 只是 primaryKey[0]（见 Init），
	// 于是 `WHERE pk0 IN (...)` **只按第一个分量过滤**：调用方以为自己在按主键取
	// N 行，实际拿回的是"第一分量等于这些值的**全部**行"——多出来的那些行属于别的
	// 主键，而且没有任何迹象表明范围被放大了。
	//
	// 这个接口的签名（pkValues []interface{}）本身就表达不了复合主键，
	// 所以不是"实现漏了"，是签名不适用。想按复合主键批量取，用
	// FindMultiByWhereWithArgs 自己拼 (a,b) IN ((?,?),(?,?)) —— 与 BatchDelete 同形。
	if len(table.primaryKey) > 1 {
		return fmt.Errorf("%w: 表 %s 是复合主键 %v，FindAllByPKIn 只能按单列主键查。"+
			"它会退化成只按第一列过滤，读回属于别的主键的行。"+
			"请改用 FindMultiByWhereWithArgs 自拼 (%s) IN ((?,?),...)",
			ErrPrimaryKeyNotFound, table.tableName, table.primaryKey,
			strings.Join(table.primaryKey, ","))
	}

	pkName := escapeMySQLName(string(table.primaryKeyField.Name()))
	pkValues, err = table.normalizeColumnComparisonValues(string(table.primaryKeyField.Name()), pkValues)
	if err != nil {
		return err
	}
	where := fmt.Sprintf("%s IN (%s)", pkName, buildPlaceholders(len(pkValues)))
	return p.FindAllByWhereWithArgs(list, where, pkValues)
}

// requireNumericColumn 这一列必须是数值列，否则拒绝下发算术更新。
//
// ⚠️ MySQL 对**非数值列**做算术不会报错，它会先把内容按数值解析（解析不出就是 0）
// 再赋值回去。于是 IncrByPK(msg, "nickname", 1) 在一条 MEDIUMTEXT 列上是这样的：
//
//	nickname = 'abc'  →  UPDATE ... SET `nickname` = `nickname` + 1  →  nickname = '1'
//
// 语句成功、RowsAffected=1、零 Warning（非严格模式下），玩家昵称变成 "1"。
// 原先这里只校验"字段存在"，一个写错的列名只要**碰巧存在**就一路放行到库里。
// 字段类型在 fieldNameToDesc 里现成拿得到，没有理由不查。
func (m *MessageTable) requireNumericColumn(col string) error {
	fieldDesc, ok := m.fieldNameToDesc[col]
	if !ok {
		return fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, col, m.tableName)
	}
	// 判据复用 sqlbuilder.go 的 isNumericKind——SQLBuilder.UpsertAdd 早就在用它做同样的
	// 把关（"必须显式指定数值列，避免把 string/BLOB 交给 MySQL 隐式转换后写坏数据"）。
	// 也就是说这条规则本来就是本仓库的既定立场，只是 IncrByPK / DecrByPKIfEnough 漏了。
	//
	// 额外挡掉 repeated / map：它们的 Kind() 仍是元素类型（repeated int32 就是 Int32Kind），
	// 只看 Kind 会放行，而这两类落的是 MEDIUMBLOB。
	if fieldDesc.IsMap() || fieldDesc.IsList() || !isNumericKind(fieldDesc.Kind()) {
		return fmt.Errorf("%w: 表 %s 的列 %s 是 %s，不是数值列。"+
			"MySQL 对非数值列做算术不报错——它先把内容按数值解析（解析不出算 0）再写回去，"+
			"于是 'abc' + 1 会静默变成 '1'。要改这一列请用 UpdateKVByPK / UpdateFieldsByPK",
			ErrFieldNotFound, m.tableName, col, fieldDesc.Kind())
	}
	return nil
}

// IncrByPK 按主键对数值字段原子加减（UPDATE ... SET f = f + delta），
// 适合货币/经验等计数器，避免“读-改-写”竞态
func (p *DB) IncrByPK(message proto.Message, field string, delta int64) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}
	if err := table.requireNumericColumn(field); err != nil {
		return err
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
	if delta <= 0 {
		return false, fmt.Errorf("delta must be positive, got %d", delta)
	}

	table, err := p.tableForMessage(message)
	if err != nil {
		return false, err
	}
	if err := table.requireNumericColumn(field); err != nil {
		return false, err
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
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}
	value, err := table.normalizeColumnComparisonValue(whereKey, whereVal)
	if err != nil {
		return err
	}
	return p.FindOneByWhereWithArgs(message, escapeMySQLName(whereKey)+" = ?", []interface{}{value})
}

// FindOneByWhereWithArgs 执行参数化的自定义WHERE查询（单条数据）
func (p *DB) FindOneByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	tableName := GetTableName(message)
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
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
	table, _, err := resolveListTable(p.Tables, list)
	if err != nil {
		return err
	}
	value, err = table.normalizeColumnComparisonValue(key, value)
	if err != nil {
		return err
	}
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

	table, _, err := resolveListTable(p.Tables, list)
	if err != nil {
		return err
	}
	values, err = table.normalizeColumnComparisonValues(key, values)
	if err != nil {
		return err
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

	// fn 里 panic 时也必须回滚。原先只在 fn 返回 error 的路径上 Rollback：
	// 一个 panic（空指针、越界、业务代码自己 panic）会**带着未提交的事务**穿过去，
	// 那条连接被 database/sql 一直算作"事务中"，既不回滚也不归还池子。
	// 攒够几次，连接池就被这些僵尸事务占满，而且它们在 InnoDB 里还持着行锁——
	// 表现为整个服务卡在拿连接上，与真正的根因（某处 panic）看不出任何关系。
	//
	// 用 committed 标记而不是 defer 里判 err：Rollback 在已 Commit 的事务上返回
	// ErrTxDone，无害但会污染日志。
	committed := false
	defer func() {
		if committed {
			return
		}
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r) // 原样抛回去，不吞调用方的 panic
		}
		_ = tx.Rollback()
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", err, rbErr)
		}
		committed = true // 已显式回滚，defer 不必再动手
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// tableForMessage 解析行消息对应的已注册表
func (p *DB) tableForMessage(message proto.Message) (*MessageTable, error) {
	return tableForRegistryKey(p.Tables, GetTableName(message))
}

// tableForRegistryKey 在返回映射前确认它没有与另一 protobuf message 竞争同一物理表。
// 这个约束不能只放在 schema sync：使用预建表、不调用自动迁移的服务同样会执行 DML，
// 而两种 descriptor 还会共享缓存 key，可能把一种 wire payload 当成另一种读取。
func tableForRegistryKey(tables map[string]*MessageTable, registryKey string) (*MessageTable, error) {
	table, ok := tables[registryKey]
	if !ok || table == nil {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, registryKey)
	}
	for otherKey, other := range tables {
		if otherKey == registryKey {
			continue
		}
		if other == nil {
			return nil, fmt.Errorf("registered table %s is nil", otherKey)
		}
		if strings.EqualFold(table.tableName, other.tableName) {
			return nil, fmt.Errorf("%w: table %q (%s) conflicts with %q (%s) under case-insensitive comparison",
				ErrDuplicateTableMapping,
				table.tableName, registryKey,
				other.tableName, otherKey)
		}
	}
	return table, nil
}

// resolveAnyTable 解析行消息或列表消息（包含单个repeated字段）对应的已注册表
func resolveAnyTable(tables map[string]*MessageTable, message proto.Message) (*MessageTable, error) {
	tableName := GetTableName(message)
	if table, err := tableForRegistryKey(tables, tableName); err == nil {
		return table, nil
	} else if !errors.Is(err, ErrTableNotFound) {
		return nil, err
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
	table, err := tableForRegistryKey(tables, tableName)
	if err != nil {
		return nil, nil, err
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
		table, err := p.tableForMessage(q.Message)
		if err != nil {
			return err
		}
		sqlParts = append(sqlParts, table.GetSelectSQL(false)+" WHERE "+q.WhereClause)
		allArgs = append(allArgs, q.WhereArgs...)
	}

	// 走 p.conn() 而不是 p.DB：事务内调用时必须落在**当前事务**上，否则读到的是
	// 事务外的快照——RunInTransaction 里"先改后查"会查不到自己刚写的值，
	// 而且这条查询还持着另一条连接，与事务本身互相等锁。
	sqlStmt := strings.Join(sqlParts, "; ")
	rows, err := p.conn().Query(sqlStmt, allArgs...)
	if err != nil {
		// 不打 args：里面可能是主键之外的业务参数（token、裸 protobuf 字节）。
		return fmt.Errorf("exec multi select: %w, SQL: %s, args len=%d", err, sqlStmt, len(allArgs))
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
	if p.tx != nil {
		return fmt.Errorf("%w: SyncAllTables; MySQL DDL implicitly commits and cannot be rolled back with the business transaction",
			ErrSchemaSyncInTransaction)
	}
	// 先对整批做纯内存预检。否则 map 排序后前几张合法表的 DDL 已经落库，后面一张
	// 非法表才报错，调用方拿到 error 时数据库却处于“半同步”状态。
	tables, err := p.registeredTablesForSQLGeneration()
	if err != nil {
		return err
	}
	for _, entry := range tables {
		if err := entry.table.validateSchemaDefinition(); err != nil {
			return fmt.Errorf("validate table %s before schema sync: %w", entry.registryKey, err)
		}
	}

	// 拿到锁就把这一整轮同步都钉在持锁的那条连接上（见 meta()）：
	// 用户锁属于 session，而"钉住一条、剩下的去池里另借"在 SetMaxOpenConns(1) 上是死锁。
	syncDB := p
	lockConn, err := p.acquireSyncLock()
	if err != nil {
		return err
	}
	if lockConn != nil {
		defer p.releaseSyncLock(lockConn)
		syncDB = p.withPinnedConn(lockConn)
	}

	// 表名排序后再遍历。Go 的 map 迭代是随机化的，不排序的话多个副本会以不同顺序
	// 抢同一批表的元数据锁，可能互相等待；而且失败时"改到第几张表"每次都不一样，
	// 排障时对不上。
	for _, entry := range tables {
		if err := syncDB.syncTableSchema(entry.registryKey, entry.table); err != nil {
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
	// SyncLockReleaseTimeout 独立于调用方请求上下文；请求超时也要给 RELEASE_LOCK 一次机会。
	SyncLockReleaseTimeout = 5 * time.Second
)

func (p *DB) syncLockName() string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(p.DBName))
	return fmt.Sprintf("%s:%08x", SyncLockName, h.Sum32())
}

// acquireSyncLock 抢 DDL 咨询锁。只有后端明确表示“不支持 GET_LOCK”时允许无锁降级；
// 锁竞争超时、NULL、连接或查询错误都中止同步，不能把锁保护悄悄降成 best-effort。
//
// 没有这把锁时：N 个副本同时冷启动 → 一个 ALTER 成功、其余全部撞
// Error 1060 Duplicate column name → 返回错误 → 启动失败。下一次重启会成功
// （列已经存在，对齐结果为空），所以它是**"自愈式蒙对"**——日志里留下一串启动失败、
// 服务最终起来了，很容易被当成偶发 flake 忽略，直到某次重启风暴把它放大。
//
// TiDB 从 v6.1 起支持用户级锁；更老的兼容实现可能明确返回“函数不存在/不支持”。
// 只有这种可识别的能力缺失才记录告警并降级，避免误把网络断开、权限错误或真实锁竞争
// 当成“兼容性差异”吞掉。
// disableSyncLockForTest 仅供**本包测试**用来验证锁真的在干活。
//
// 刻意不做成环境变量：那等于给生产环境留一个能关掉安全机制的开关。
// 包内私有变量外部改不了，而负向测试能证明这条路径不是摆设——
// 实测关掉锁后 8 个副本里 7 个直接撞 Error 1060。
var disableSyncLockForTest bool

// ⚠️ MySQL 的用户锁**属于 session**：GET_LOCK 与 RELEASE_LOCK 必须在**同一条连接**上
// 执行。原先两边都走 p.conn()，而 *sql.DB 是连接池——两条语句落在不同连接上是常态。
// 后果不是"少了个锁"，比那更糟：RELEASE_LOCK 在没持锁的 session 上返回 0（不报错），
// 锁一直挂在最初那条连接上，直到它被池回收关闭。在长驻连接池里那可能是**进程的一生**。
// 于是本进程后续无人能释放，别的副本每次启动都要白等 SyncLockTimeoutSeconds 秒
// 才降级——这把锁从"防并发 DDL"退化成了一个纯粹的 30 秒启动延迟。
//
// 所以持锁期间必须把连接**钉住**：拿到就一路带着，用完在同一条上释放并归还。
// 返回 (nil,nil) 只表示已确认后端不支持该函数（或测试显式关闭）；调用方无需释放。
func (p *DB) acquireSyncLock() (*sql.Conn, error) {
	if disableSyncLockForTest {
		return nil, nil
	}
	lockName := p.syncLockName()
	conn, err := p.DB.Conn(p.context())
	if err != nil {
		return nil, fmt.Errorf("取得 DDL 咨询锁 %s 的专用连接失败: %w", lockName, err)
	}

	var got sql.NullInt64
	err = conn.QueryRowContext(p.context(),
		"SELECT GET_LOCK(?, ?)", lockName, SyncLockTimeoutSeconds).Scan(&got)
	if err != nil {
		if isAdvisoryLockUnsupported(err) {
			log.Printf("warning: 后端明确不支持 GET_LOCK（%s），本次结构同步有边界地无锁执行：%v",
				lockName, err)
			_ = conn.Close()
			return nil, nil
		}
		discardSQLConn(conn)
		return nil, fmt.Errorf("取得 DDL 咨询锁 %s 失败: %w", lockName, err)
	}
	if !got.Valid {
		discardSQLConn(conn)
		return nil, fmt.Errorf("DDL 咨询锁 %s 返回 NULL，锁状态未知，已中止结构同步", lockName)
	}
	if got.Int64 == 0 {
		_ = conn.Close() // 明确超时，session 没持锁，可安全归还池
		return nil, fmt.Errorf("DDL 咨询锁 %s 在 %d 秒内未取得，另一个结构同步仍可能持锁",
			lockName, SyncLockTimeoutSeconds)
	}
	if got.Int64 != 1 {
		discardSQLConn(conn)
		return nil, fmt.Errorf("DDL 咨询锁 %s 返回意外值 %d，锁状态未知", lockName, got.Int64)
	}
	return conn, nil
}

func isAdvisoryLockUnsupported(err error) bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	message := strings.ToLower(mysqlErr.Message)
	if !strings.Contains(message, "get_lock") {
		return false
	}
	unsupported := strings.Contains(message, "does not exist") ||
		strings.Contains(message, "doesn't exist") ||
		strings.Contains(message, "not support") ||
		strings.Contains(message, "unsupported")
	if !unsupported {
		return false
	}
	switch mysqlErr.Number {
	case 1105, 1235, 1305:
		return true
	default:
		return false
	}
}

// releaseSyncLock 在**持锁的那条连接**上释放并把它归还给池。
func (p *DB) releaseSyncLock(conn *sql.Conn) {
	if conn == nil {
		return
	}
	lockName := p.syncLockName()
	ctx, cancel := context.WithTimeout(context.Background(), SyncLockReleaseTimeout)
	defer cancel()

	var released sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		"SELECT RELEASE_LOCK(?)", lockName).Scan(&released); err != nil {
		// Close 只会把健康连接归还池；释放结果不确定时必须把底层 session 标成坏连接，
		// 否则它可能带着 GET_LOCK 回到池里继续存活。
		log.Printf("warning: 释放 DDL 咨询锁 %s 失败，丢弃持锁连接：%v", lockName, err)
		discardSQLConn(conn)
		return
	}
	if !released.Valid || released.Int64 != 1 {
		// 走到这里说明"在持锁连接上释放"的前提被打破了，必须留痕——
		// 静默下去的话，下一个副本要白等一个超时才知道。
		log.Printf("warning: DDL 咨询锁 %s 释放返回 %v（预期 1）；立即丢弃该连接以关闭底层 session",
			lockName, released)
		discardSQLConn(conn)
		return
	}
	_ = conn.Close() // RELEASE=1 后才允许把这条 session 归还池
}

func discardSQLConn(conn *sql.Conn) {
	if conn == nil {
		return
	}
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
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

// WithMaxLength 设置主键/唯一键里 string/bytes 字段的列宽 n（string 按字符、bytes 按字节），
// 适用范围与取值区间见 proto2mysql_option.proto 的 max_length。
//
// 按字段合并，不是整体替换：多次调用只覆盖同名字段，其它字段已有的长度保留，同一字段后应用的覆盖先应用的。
// RegisterTable / NewSQLBuilder 先应用 proto 里声明的 max_length，再应用代码传入的选项，
// 所以代码传入的值覆盖 proto 声明，而代码没提到的字段仍沿用 proto 声明。
//
// 代码改了主键/唯一键、proto 里的 max_length 落到了键外字段上时，用 WithMaxLengths 整体替换，
// 合并语义清不掉 proto 的声明。
func WithMaxLength(field string, n uint32) TableOption {
	return func(t *MessageTable) {
		if t.maxLengths == nil {
			t.maxLengths = make(map[string]uint32)
		}
		t.maxLengths[field] = n
	}
}

// WithMaxLengths 整体替换 max_length 集合（语义与 WithNullableFields 一致），nil 或空 map 表示清空。
//
// 为什么需要它：max_length 只允许声明在主键/唯一键的 string/bytes 字段上，而键是可以被代码
// 改掉的——proto 在 email 上写了 max_length、代码却用 WithUniqueKey("name") 换掉了键之后，
// email 上残留的声明会让 validateMaxLengths 以 ErrInvalidTableOption 拒绝整张表，
// 且 WithMaxLength(email, 0) 同样越界被拒，按字段合并的语义清不掉它。
//
// 传入的 map 会被拷贝一份，调用方之后再改它不影响已注册的表。
func WithMaxLengths(lengths map[string]uint32) TableOption {
	return func(t *MessageTable) {
		if len(lengths) == 0 {
			t.maxLengths = nil
			return
		}
		t.maxLengths = make(map[string]uint32, len(lengths))
		for field, n := range lengths {
			t.maxLengths[field] = n
		}
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
