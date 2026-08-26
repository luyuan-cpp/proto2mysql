package proto2mysql

// SQLBuilder：由 proto.Message 直接产出各类参数化 SQL，不连库、不需要 RegisterTable。
//
// 与仓库里其它文件的分工：
//   - proto2mysql.go / proto2gorm.go：连库执行（Insert/FindOneByPK/...），SQL 只是中间产物；
//   - sqlgen.go：只产 DDL（CREATE TABLE / ALTER TABLE 迁移）；
//   - 本文件：只产 DML（INSERT / SELECT / UPDATE / DELETE），**只返回 SQL 和参数，不执行**。
//
// 适用场景：把语句交给已有的 *sql.DB / *sql.Tx / sqlx / kratos data 层自己执行、
// 塞进事务里和手写 SQL 混用、或先打日志再执行。
//
// 语句形态覆盖的是真实业务里出现过的写法（对照 XuanMing-Server 的手写 SQL 清单）：
// 列子集插入、INSERT IGNORE、ON DUPLICATE KEY UPDATE 的四种更新语义（覆盖 / 累加 /
// 取最值 / 首写生效）、SELECT ... FOR UPDATE、表达式 SET（col = col + ?）、
// CAS 守卫更新（WHERE pk = ? AND epoch = ?）、IN (...) 展开、保留期批删（ORDER BY ... LIMIT n）。
//
// 约定：
//   - 返回的 SQL **不带结尾分号**（直接给 Exec/Query 用）；
//   - 列名一律经 escapeMySQLName 转义，并校验存在于该 message，非法列名返回 ErrFieldNotFound；
//   - 消息取值沿用 pbconv.SerializeFieldValue（与 Insert/Update 一致：标量以字符串下发由 MySQL 侧隐式转换，
//     未设置的 Timestamp 下发 SQL NULL）；
//   - whereClause / OrderBy / SetColExpr 的表达式是原样拼接的裸 SQL，**不得传入不可信输入**。

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/luyuancpp/proto2mysql/pbconv"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

var (
	// ErrNoFieldsSet 消息里没有任何已赋值字段，无法生成列子集语句
	ErrNoFieldsSet = errors.New("no fields set in message")
	// ErrNoAssigns 没有提供任何赋值子句
	ErrNoAssigns = errors.New("no assignments provided")
	// ErrEmptyValues 传入的取值列表为空（IN (...) 无法生成合法 SQL）
	ErrEmptyValues = errors.New("empty value list")
	// ErrEmptyWhereClause UPDATE/DELETE 未提供条件；为防止误操作整表，必须显式写出条件
	ErrEmptyWhereClause = errors.New("empty where clause")
)

// SQLBuilder 单张表的 SQL 生成器。并发只读安全（构造后不再修改）。
type SQLBuilder struct {
	table *MessageTable
}

// NewSQLBuilder 由消息直接构造生成器：表配置优先取 proto 里声明的 option
// （table_name/primary_key/auto_increment_key/index/unique_key/nullable），
// opts 可覆盖。不连库、不需要 RegisterTable。
//
//	b := proto2mysql.NewSQLBuilder(&pb.Player{})
//	stmt, err := b.Upsert(player, "gold")
//	db.Exec(stmt.Sql, stmt.Args...)
func NewSQLBuilder(m proto.Message, opts ...TableOption) *SQLBuilder {
	return &SQLBuilder{table: newMessageTable(m, opts...)}
}

// SQLBuilder 复用 DB 里已注册表的配置生成 SQL（不会执行任何语句）。
// 未注册时返回 ErrTableNotFound。
func (p *DB) SQLBuilder(m proto.Message) (*SQLBuilder, error) {
	table, err := p.tableForMessage(m)
	if err != nil {
		return nil, err
	}
	return &SQLBuilder{table: table}, nil
}

// TableName 返回生成 SQL 时使用的表名（未转义）。
func (b *SQLBuilder) TableName() string { return b.table.tableName }

// Table 返回底层表映射，供需要字段描述符等细节的调用方使用。
func (b *SQLBuilder) Table() *MessageTable { return b.table }

// CreateTable 返回建表语句（等价 GenerateCreateTableSQL，带结尾分号）。
func (b *SQLBuilder) CreateTable() string { return b.table.GetCreateTableSQL() }

// PrimaryKeyWhere 返回按主键定位的 WHERE 片段与参数（不含 "WHERE" 关键字），
// 用于和 UpdateAssignsWhere / DeleteWhere 等自由拼接。
func (b *SQLBuilder) PrimaryKeyWhere(m proto.Message) (string, []interface{}, error) {
	return b.table.primaryKeyWhere(m)
}

// ---------------------------------------------------------------------------
// 赋值子句：UPDATE 的 SET、以及 INSERT ... ON DUPLICATE KEY UPDATE 的更新部分共用
// ---------------------------------------------------------------------------

// Assign 一条赋值子句（col = 表达式）。用 SetCol / AddCol / SetNew 等构造函数创建，
// 不要直接构造零值。
type Assign struct {
	col  string        // 原始列名，构建时用于校验该列存在于消息
	expr string        // 完整赋值表达式，列名已转义，如 "`gold` = `gold` + ?"
	args []interface{} // expr 里 ? 对应的参数，按出现顺序
	// numeric 这条赋值是算术表达式（+ / - / LEAST / GREATEST），列必须是数值列。
	//
	// MySQL 对非数值列做算术**不报错**：先把内容按数值解析（解析不出算 0）再写回，
	// 于是 MEDIUMTEXT 列上的 `nickname` = `nickname` + 1 会把 "abc" 静默变成 "1"。
	// 光校验"列存在"挡不住这个——写错的列名只要碰巧存在就一路放行到库里。
	numeric bool
}

// SetCol 设为给定值：col = ?
func SetCol(col string, val interface{}) Assign {
	e := escapeMySQLName(col)
	return Assign{col: col, expr: e + " = ?", args: []interface{}{val}}
}

// AddCol 原地累加：col = col + ?（货币/经验/计数器，避免读-改-写竞态）
func AddCol(col string, delta interface{}) Assign {
	e := escapeMySQLName(col)
	return Assign{col: col, numeric: true, expr: e + " = " + e + " + ?", args: []interface{}{delta}}
}

// SubCol 原地扣减：col = col - ?（是否允许扣成负数由 WHERE 守卫决定，见 UpdateAssignsWhere）
func SubCol(col string, delta interface{}) Assign {
	e := escapeMySQLName(col)
	return Assign{col: col, numeric: true, expr: e + " = " + e + " - ?", args: []interface{}{delta}}
}

// SetColExpr 自定义赋值表达式：col = <expr>，expr 里的 ? 由 args 依次填充。
// expr 原样拼入 SQL（如 "NOW()"、"IF(`a` = 0, ?, `a`)"），**勿传入不可信输入**。
func SetColExpr(col, expr string, args ...interface{}) Assign {
	return Assign{col: col, expr: escapeMySQLName(col) + " = " + expr, args: args}
}

// 下面几个只用于 INSERT ... ON DUPLICATE KEY UPDATE：VALUES(col) 表示
// “本次本该插入的新值”。冲突时按不同语义决定新旧值怎么合并。
//
// 注意：VALUES() 在 MySQL 8.0.20 起被标记为 deprecated（官方建议改用 AS new 行别名），
// 但至今仍可用，且兼容 5.7；本库沿用 VALUES() 以覆盖更宽的版本范围。

// SetNew 冲突时覆盖为新值：col = VALUES(col)
func SetNew(col string) Assign {
	e := escapeMySQLName(col)
	return Assign{col: col, expr: e + " = VALUES(" + e + ")"}
}

// AddNew 冲突时累加新值：col = col + VALUES(col)（发奖/加币的原子累加写法）
func AddNew(col string) Assign {
	e := escapeMySQLName(col)
	return Assign{col: col, numeric: true, expr: e + " = " + e + " + VALUES(" + e + ")"}
}

// MinNew 冲突时取较小值：col = LEAST(col, VALUES(col))（重试退避时间取更早的一次）
func MinNew(col string) Assign {
	e := escapeMySQLName(col)
	return Assign{col: col, numeric: true, expr: e + " = LEAST(" + e + ", VALUES(" + e + "))"}
}

// MaxNew 冲突时取较大值：col = GREATEST(col, VALUES(col))（水位/序号只增不减）
func MaxNew(col string) Assign {
	e := escapeMySQLName(col)
	return Assign{col: col, numeric: true, expr: e + " = GREATEST(" + e + ", VALUES(" + e + "))"}
}

// SetNewIfZero 首写生效：col = IF(col = 0, VALUES(col), col)
// 已经写过（非 0）就保持不动，用于“第一次落的时间戳/终态不可被覆盖”。
func SetNewIfZero(col string) Assign {
	e := escapeMySQLName(col)
	return Assign{col: col, numeric: true, expr: e + " = IF(" + e + " = 0, VALUES(" + e + "), " + e + ")"}
}

// KeepOld 保持原值不变：col = col。
// 用于 INSERT ... ON DUPLICATE KEY UPDATE 的“插入或加锁”写法：不改任何数据，
// 只为在冲突时也持有该行的行锁（相比 INSERT IGNORE 不会静默吞掉真实错误）。
func KeepOld(col string) Assign {
	e := escapeMySQLName(col)
	return Assign{col: col, expr: e + " = " + e}
}

// buildAssigns 校验列名并拼接赋值子句
func (b *SQLBuilder) buildAssigns(assigns []Assign) (string, []interface{}, error) {
	if len(assigns) == 0 {
		return "", nil, ErrNoAssigns
	}
	clauses := make([]string, 0, len(assigns))
	args := make([]interface{}, 0, len(assigns))
	for _, a := range assigns {
		// 数值赋值（AddCol / SubCol / AddNew / MinNew / MaxNew / SetNewIfZero）
		// 要额外查列是不是数值列。
		// 只查"列存在"挡不住 IncrByPK(m, "nickname", 1) 这种写错列名的调用——
		// MySQL 会把 MEDIUMTEXT 的 "abc" + 1 静默算成 1 再写回去。
		// UpsertAdd 早就在做同样的把关，这里补上其余入口。
		if a.numeric {
			if err := b.table.requireNumericColumn(a.col); err != nil {
				return "", nil, err
			}
			clauses = append(clauses, a.expr)
			args = append(args, a.args...)
			continue
		}
		if err := b.checkColumn(a.col); err != nil {
			return "", nil, err
		}
		clauses = append(clauses, a.expr)
		args = append(args, a.args...)
	}
	return strings.Join(clauses, ", "), args, nil
}

// checkColumn 校验列名对应消息里真实存在的字段
func (b *SQLBuilder) checkColumn(col string) error {
	if _, ok := b.table.fieldNameToDesc[col]; !ok {
		return fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, col, b.table.tableName)
	}
	return nil
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

// Insert 全字段插入：INSERT INTO t (所有列) VALUES (?, ...)
func (b *SQLBuilder) Insert(m proto.Message) (*SqlWithArgs, error) {
	return b.table.GetInsertSQLWithArgs(m)
}

// InsertSetFields 只插入“已赋值字段”，未赋值的列交给 MySQL 的列默认值
// （自增主键、DEFAULT CURRENT_TIMESTAMP 的 created_at 等）。
//
// 注意 proto3 语义：非 optional 的标量字段，值为 0/""/false 时 Has() 为 false，
// 会被当作“未赋值”而跳过。要显式写入零值，请把该字段声明为 optional，或改用 Insert。
func (b *SQLBuilder) InsertSetFields(m proto.Message) (*SqlWithArgs, error) {
	cols, args, err := b.setFieldColumns(m)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		escapeMySQLName(b.table.tableName), strings.Join(cols, ", "), buildPlaceholders(len(cols)))
	return &SqlWithArgs{Sql: stmt, Args: args}, nil
}

// InsertIgnore 幂等插入：唯一键冲突时执行明确的 no-op ODKU。
// 不使用 INSERT IGNORE：IGNORE 还会把类型截断、越界、NOT NULL 等真实错误降级成
// warning 并写入被 MySQL 修正过的值，远超“重复即跳过”的契约。
func (b *SQLBuilder) InsertIgnore(m proto.Message) (*SqlWithArgs, error) {
	stmt, err := b.table.GetInsertSQLWithArgs(m)
	if err != nil {
		return nil, err
	}
	return b.withDuplicateNoop(stmt), nil
}

// InsertIgnoreSetFields 列子集版（列选取规则同 InsertSetFields）。
func (b *SQLBuilder) InsertIgnoreSetFields(m proto.Message) (*SqlWithArgs, error) {
	stmt, err := b.InsertSetFields(m)
	if err != nil {
		return nil, err
	}
	return b.withDuplicateNoop(stmt), nil
}

// Replace 整行替换：REPLACE INTO ...（冲突时先删后插，会丢掉未提供的列并触发外键级联，慎用）
func (b *SQLBuilder) Replace(m proto.Message) (*SqlWithArgs, error) {
	return b.table.GetReplaceSQLWithArgs(m)
}

// BatchInsert 批量插入：INSERT INTO t (...) VALUES (...), (...)，条数上限 BatchInsertMaxSize
func (b *SQLBuilder) BatchInsert(msgs []proto.Message) (*SqlWithArgs, error) {
	return b.table.GetBatchInsertSQLWithArgs(msgs)
}

// BatchInsertIgnore 批量幂等插入
func (b *SQLBuilder) BatchInsertIgnore(msgs []proto.Message) (*SqlWithArgs, error) {
	stmt, err := b.table.GetBatchInsertSQLWithArgs(msgs)
	if err != nil {
		return nil, err
	}
	return b.withDuplicateNoop(stmt), nil
}

// BatchReplace 批量整行替换
func (b *SQLBuilder) BatchReplace(msgs []proto.Message) (*SqlWithArgs, error) {
	return b.table.GetBatchReplaceSQLWithArgs(msgs)
}

// Upsert 插入或覆盖：INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col)。
// cols 为空时默认覆盖所有非主键列。冲突时用本次的新值覆盖旧值。
//
//	b.Upsert(sec)                       // 覆盖全部非主键列
//	b.Upsert(sec, "generation", "data")  // 只覆盖这两列
func (b *SQLBuilder) Upsert(m proto.Message, cols ...string) (*SqlWithArgs, error) {
	return b.upsertBy(m, cols, SetNew)
}

// UpsertAdd 插入或累加：INSERT ... ON DUPLICATE KEY UPDATE col = col + VALUES(col)。
// 必须显式指定数值列，避免把 string/BLOB 等非数值列交给 MySQL 隐式转换后写坏数据。
func (b *SQLBuilder) UpsertAdd(m proto.Message, cols ...string) (*SqlWithArgs, error) {
	if len(cols) == 0 {
		return nil, ErrNoAssigns
	}
	for _, col := range cols {
		field, ok := b.table.fieldNameToDesc[col]
		if !ok {
			return nil, fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, col, b.table.tableName)
		}
		if !isNumericKind(field.Kind()) {
			return nil, fmt.Errorf("column %s in table %s is not numeric", col, b.table.tableName)
		}
	}
	return b.upsertBy(m, cols, AddNew)
}

// UpsertKeepOld 插入或占位：INSERT ... ON DUPLICATE KEY UPDATE pk = pk。
// 行不存在则建行，存在则不改任何数据但持有行锁——事务里“确保这行存在并锁住它”的标准写法。
func (b *SQLBuilder) UpsertKeepOld(m proto.Message) (*SqlWithArgs, error) {
	if len(b.table.primaryKey) == 0 {
		return nil, ErrPrimaryKeyNotFound
	}
	return b.UpsertWith(m, KeepOld(b.table.primaryKey[0]))
}

// UpsertWith 插入或按自定义语义合并：更新部分由 assigns 决定，可混用
// SetNew / AddNew / MinNew / MaxNew / SetNewIfZero / SetCol / SetColExpr。
//
//	// 冲突时：代次 +1、jti 换成新值、首次写入的时间戳不被覆盖
//	b.UpsertWith(row,
//	    proto2mysql.SetColExpr("generation", "`generation` + 1"),
//	    proto2mysql.SetNew("sess_jti"),
//	    proto2mysql.SetNewIfZero("first_seen_ms"))
func (b *SQLBuilder) UpsertWith(m proto.Message, assigns ...Assign) (*SqlWithArgs, error) {
	insert, err := b.table.GetInsertSQLWithArgs(m)
	if err != nil {
		return nil, err
	}
	return b.appendOnDuplicate(insert, assigns)
}

// BatchUpsert 批量插入或覆盖：VALUES 多行 + ON DUPLICATE KEY UPDATE col = VALUES(col)
func (b *SQLBuilder) BatchUpsert(msgs []proto.Message, cols ...string) (*SqlWithArgs, error) {
	insert, err := b.table.GetBatchInsertSQLWithArgs(msgs)
	if err != nil {
		return nil, err
	}
	assigns, err := b.assignsForCols(cols, SetNew)
	if err != nil {
		return nil, err
	}
	return b.appendOnDuplicate(insert, assigns)
}

// BatchUpsertWith 批量插入 + 自定义冲突合并语义
func (b *SQLBuilder) BatchUpsertWith(msgs []proto.Message, assigns ...Assign) (*SqlWithArgs, error) {
	insert, err := b.table.GetBatchInsertSQLWithArgs(msgs)
	if err != nil {
		return nil, err
	}
	return b.appendOnDuplicate(insert, assigns)
}

// upsertBy 按 cols（空则取全部非主键列）生成同一种语义的赋值子句后拼 upsert
func (b *SQLBuilder) upsertBy(m proto.Message, cols []string, mk func(string) Assign) (*SqlWithArgs, error) {
	assigns, err := b.assignsForCols(cols, mk)
	if err != nil {
		return nil, err
	}
	return b.UpsertWith(m, assigns...)
}

// assignsForCols cols 为空时取所有非主键列，逐列用 mk 构造赋值子句
func (b *SQLBuilder) assignsForCols(cols []string, mk func(string) Assign) ([]Assign, error) {
	if len(cols) == 0 {
		cols = b.nonPrimaryKeyColumns()
	}
	if len(cols) == 0 {
		return nil, ErrNoAssigns
	}
	assigns := make([]Assign, 0, len(cols))
	for _, col := range cols {
		if err := b.checkColumn(col); err != nil {
			return nil, err
		}
		assigns = append(assigns, mk(col))
	}
	return assigns, nil
}

// appendOnDuplicate 给 INSERT 语句追加 ON DUPLICATE KEY UPDATE 部分
func (b *SQLBuilder) appendOnDuplicate(insert *SqlWithArgs, assigns []Assign) (*SqlWithArgs, error) {
	setClause, setArgs, err := b.buildAssigns(assigns)
	if err != nil {
		return nil, err
	}
	args := make([]interface{}, 0, len(insert.Args)+len(setArgs))
	args = append(args, insert.Args...)
	args = append(args, setArgs...)
	return &SqlWithArgs{Sql: insert.Sql + " ON DUPLICATE KEY UPDATE " + setClause, Args: args}, nil
}

// nonPrimaryKeyColumns 按字段声明顺序返回所有非主键列
func (b *SQLBuilder) nonPrimaryKeyColumns() []string {
	fields := b.table.Descriptor.Fields()
	cols := make([]string, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		if isPrimaryKeyColumn(b.table.primaryKey, name) {
			continue
		}
		cols = append(cols, name)
	}
	return cols
}

func isPrimaryKeyColumn(primaryKey []string, col string) bool {
	for _, pk := range primaryKey {
		if pk == col {
			return true
		}
	}
	return false
}

// setFieldColumns 收集消息里已赋值字段的转义列名与取值
func (b *SQLBuilder) setFieldColumns(m proto.Message) ([]string, []interface{}, error) {
	if err := b.table.validateMessageDescriptor(m); err != nil {
		return nil, nil, err
	}
	fields := b.table.Descriptor.Fields()
	reflection := m.ProtoReflect()

	cols := make([]string, 0, fields.Len())
	args := make([]interface{}, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if !reflection.Has(field) {
			continue
		}
		val, err := pbconv.SerializeFieldValue(m, field)
		if err != nil {
			return nil, nil, fmt.Errorf("serialize field %s: %w", field.Name(), err)
		}
		cols = append(cols, escapeMySQLName(string(field.Name())))
		args = append(args, val)
	}
	if len(cols) == 0 {
		return nil, nil, ErrNoFieldsSet
	}
	return cols, args, nil
}

// withDuplicateNoop 只吞唯一键冲突，其他 INSERT 错误保持错误。
func (b *SQLBuilder) withDuplicateNoop(stmt *SqlWithArgs) *SqlWithArgs {
	column := ""
	for _, pk := range b.table.primaryKey {
		pk = strings.TrimSpace(pk)
		if _, ok := b.table.fieldNameToDesc[pk]; ok {
			column = pk
			break
		}
	}
	if column == "" && b.table.Descriptor.Fields().Len() > 0 {
		column = string(b.table.Descriptor.Fields().Get(0).Name())
	}
	escaped := escapeMySQLName(column)
	return &SqlWithArgs{
		Sql:  stmt.Sql + " ON DUPLICATE KEY UPDATE " + escaped + " = " + escaped,
		Args: stmt.Args,
	}
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

// SelectByPK 按主键查整行：SELECT 所有列 FROM t WHERE pk = ?
func (b *SQLBuilder) SelectByPK(m proto.Message) (*SqlWithArgs, error) {
	return b.selectByPK(m, QueryOptions{})
}

// SelectByPKForUpdate 按主键加写锁查整行：... WHERE pk = ? FOR UPDATE。
// 事务里“先锁后改”的标准第一步（读到的是加锁后的最新值，防止并发读-改-写丢更新）。
func (b *SQLBuilder) SelectByPKForUpdate(m proto.Message) (*SqlWithArgs, error) {
	return b.selectByPK(m, QueryOptions{ForUpdate: true})
}

func (b *SQLBuilder) selectByPK(m proto.Message, opts QueryOptions) (*SqlWithArgs, error) {
	where, args, err := b.table.primaryKeyWhere(m)
	if err != nil {
		return nil, err
	}
	return &SqlWithArgs{Sql: b.table.selectFieldsSQL + " WHERE " + where + opts.sqlSuffix(), Args: args}, nil
}

// SelectWhere 按条件查整行，支持 ORDER BY / LIMIT / OFFSET / FOR UPDATE。
// whereClause 原样拼接（"1 = 1" 表示无条件），**勿传入不可信输入**；取值一律走 args。
func (b *SQLBuilder) SelectWhere(whereClause string, args []interface{}, opts QueryOptions) *SqlWithArgs {
	return &SqlWithArgs{
		Sql:  b.table.selectFieldsSQL + " WHERE " + normalizeWhereClause(whereClause) + opts.sqlSuffix(),
		Args: args,
	}
}

// SelectColumns 只查指定列（列名会校验+转义），用于事务里只读一两个字段加锁，
// 如 SELECT `gold` FROM `player_currency` WHERE player_id = ? FOR UPDATE。
// cols 为空则退化为查全部列。
func (b *SQLBuilder) SelectColumns(cols []string, whereClause string, args []interface{}, opts QueryOptions) (*SqlWithArgs, error) {
	if len(cols) == 0 {
		return b.SelectWhere(whereClause, args, opts), nil
	}
	escaped := make([]string, 0, len(cols))
	for _, col := range cols {
		if err := b.checkColumn(col); err != nil {
			return nil, err
		}
		escaped = append(escaped, escapeMySQLName(col))
	}
	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE %s%s",
		strings.Join(escaped, ", "), escapeMySQLName(b.table.tableName),
		normalizeWhereClause(whereClause), opts.sqlSuffix())
	return &SqlWithArgs{Sql: stmt, Args: args}, nil
}

// SelectByKVIn 按某列的取值集合批量查：... WHERE col IN (?, ?, ...)（占位符按 vals 长度展开）
func (b *SQLBuilder) SelectByKVIn(col string, vals []interface{}, opts QueryOptions) (*SqlWithArgs, error) {
	where, err := b.inClause(col, len(vals))
	if err != nil {
		return nil, err
	}
	vals, err = b.table.normalizeColumnComparisonValues(col, vals)
	if err != nil {
		return nil, err
	}
	return b.SelectWhere(where, vals, opts), nil
}

// SelectByPKIn 按主键取值集合批量查（联合主键不适用，返回 ErrPrimaryKeyNotFound）
func (b *SQLBuilder) SelectByPKIn(pkValues []interface{}, opts QueryOptions) (*SqlWithArgs, error) {
	col, err := b.singlePrimaryKey()
	if err != nil {
		return nil, err
	}
	return b.SelectByKVIn(col, pkValues, opts)
}

// Count 计数：SELECT COUNT(*) FROM t WHERE ...（whereClause 为空表示全表）
func (b *SQLBuilder) Count(whereClause string, args []interface{}) *SqlWithArgs {
	stmt := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s",
		escapeMySQLName(b.table.tableName), normalizeWhereClause(whereClause))
	return &SqlWithArgs{Sql: stmt, Args: args}
}

// Exists 存在性判定：SELECT 1 FROM t WHERE ... LIMIT 1。
// 比 COUNT(*) 便宜（命中一行即停），只需要“有没有”时优先用它。
func (b *SQLBuilder) Exists(whereClause string, args []interface{}) *SqlWithArgs {
	stmt := fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1",
		escapeMySQLName(b.table.tableName), normalizeWhereClause(whereClause))
	return &SqlWithArgs{Sql: stmt, Args: args}
}

// ExistsByPKForUpdate 按主键判定存在并加写锁：SELECT 1 ... WHERE pk = ? LIMIT 1 FOR UPDATE
func (b *SQLBuilder) ExistsByPKForUpdate(m proto.Message) (*SqlWithArgs, error) {
	where, args, err := b.table.primaryKeyWhere(m)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf("SELECT 1 FROM %s WHERE %s LIMIT 1 FOR UPDATE",
		escapeMySQLName(b.table.tableName), where)
	return &SqlWithArgs{Sql: stmt, Args: args}, nil
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

// UpdateByPK 按主键更新已赋值字段：UPDATE t SET a = ?, b = ? WHERE pk = ?
// （proto3 零值视为未赋值，规则同 InsertSetFields）
func (b *SQLBuilder) UpdateByPK(m proto.Message) (*SqlWithArgs, error) {
	return b.table.GetUpdateSQLWithArgs(m)
}

// UpdateByPKIf 带守卫条件的按主键更新（CAS）：
// UPDATE t SET ... WHERE pk = ? AND <guard>。
// 用于 owner_epoch / 版本号 / 状态机流转这类“只有当前状态符合预期才允许写”的场景，
// 默认 MySQL RowsAffected 统计实际变更行数，因此 0 既可能是守卫失败，也可能是新旧值相同。
// 若必须区分，令更新同时递增版本列，或在连接启用 clientFoundRows 后按匹配行数判断。
func (b *SQLBuilder) UpdateByPKIf(m proto.Message, guard string, guardArgs []interface{}) (*SqlWithArgs, error) {
	setClause, setArgs, err := b.table.GetUpdateSetWithArgs(m)
	if err != nil {
		return nil, err
	}
	if setClause == "" {
		return nil, ErrNoFieldsSet
	}
	where, whereArgs, err := b.table.primaryKeyWhere(m)
	if err != nil {
		return nil, err
	}
	return b.updateStmt(setClause, setArgs, appendGuard(where, guard), concatArgs(whereArgs, guardArgs)), nil
}

// UpdateFieldsByPK 按主键只更新指定列（即使这些列当前是 proto3 零值也会写出去，
// 用于“把余额清零”这类必须写零值的场景）
func (b *SQLBuilder) UpdateFieldsByPK(m proto.Message, cols ...string) (*SqlWithArgs, error) {
	if err := b.table.validateMessageDescriptor(m); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, ErrNoAssigns
	}
	assigns := make([]Assign, 0, len(cols))
	for _, col := range cols {
		field, ok := b.table.fieldNameToDesc[col]
		if !ok {
			return nil, fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, col, b.table.tableName)
		}
		val, err := pbconv.SerializeFieldValue(m, field)
		if err != nil {
			return nil, fmt.Errorf("serialize field %s: %w", col, err)
		}
		assigns = append(assigns, SetCol(col, val))
	}
	return b.UpdateAssignsByPK(m, assigns...)
}

// UpdateWhere 按自定义条件更新已赋值字段
func (b *SQLBuilder) UpdateWhere(m proto.Message, whereClause string, whereArgs []interface{}) (*SqlWithArgs, error) {
	where, err := requireMutationWhereClause(whereClause)
	if err != nil {
		return nil, err
	}
	return b.table.GetUpdateSQLByWhereWithArgs(m, where, whereArgs)
}

// UpdateAssignsByPK 按主键做表达式更新：UPDATE t SET gold = gold + ?, updated_at = NOW() WHERE pk = ?
func (b *SQLBuilder) UpdateAssignsByPK(m proto.Message, assigns ...Assign) (*SqlWithArgs, error) {
	setClause, setArgs, err := b.buildAssigns(assigns)
	if err != nil {
		return nil, err
	}
	where, whereArgs, err := b.table.primaryKeyWhere(m)
	if err != nil {
		return nil, err
	}
	return b.updateStmt(setClause, setArgs, where, whereArgs), nil
}

// UpdateAssignsWhere 按自定义条件做表达式更新（最通用的一条，扣减守卫等都由它表达）：
//
//	// 余额够才扣，扣不成 RowsAffected 为 0
//	b.UpdateAssignsWhere([]proto2mysql.Assign{proto2mysql.SubCol("gold", 100)},
//	    "`player_id` = ? AND `gold` >= ?", []interface{}{pid, 100})
func (b *SQLBuilder) UpdateAssignsWhere(assigns []Assign, whereClause string, whereArgs []interface{}) (*SqlWithArgs, error) {
	where, err := requireMutationWhereClause(whereClause)
	if err != nil {
		return nil, err
	}
	setClause, setArgs, err := b.buildAssigns(assigns)
	if err != nil {
		return nil, err
	}
	return b.updateStmt(setClause, setArgs, where, whereArgs), nil
}

// IncrByPK 按主键累加单列：UPDATE t SET col = col + ? WHERE pk = ?
// （delta 取负即为扣减，但不做下限保护，会扣成负数；需要下限用 DecrByPKIfEnough）
func (b *SQLBuilder) IncrByPK(m proto.Message, col string, delta int64) (*SqlWithArgs, error) {
	return b.UpdateAssignsByPK(m, AddCol(col, delta))
}

// DecrByPKIfEnough 按主键扣减单列，且只有当前值 >= delta 才扣：
// UPDATE t SET col = col - ? WHERE pk = ? AND col >= ?（防止扣成负数）。
// 执行后必须据 RowsAffected 判断是否真的扣到，为 0 表示余额不足或记录不存在。
// delta 必须为正数：0 不产生实际变更，负数则会让扣减变成累加，二者都会破坏结果判定。
func (b *SQLBuilder) DecrByPKIfEnough(m proto.Message, col string, delta int64) (*SqlWithArgs, error) {
	if delta <= 0 {
		return nil, fmt.Errorf("delta must be positive, got %d", delta)
	}
	// 这里的守卫条件 `col >= ?` 也要求数值列，与下面的 SubCol 同一条规则。
	if err := b.table.requireNumericColumn(col); err != nil {
		return nil, err
	}
	where, whereArgs, err := b.table.primaryKeyWhere(m)
	if err != nil {
		return nil, err
	}
	escaped := escapeMySQLName(col)
	guard := escaped + " >= ?"
	return b.UpdateAssignsWhere([]Assign{SubCol(col, delta)},
		appendGuard(where, guard), concatArgs(whereArgs, []interface{}{delta}))
}

// updateStmt 拼 UPDATE 语句并按 SET→WHERE 顺序合并参数
func (b *SQLBuilder) updateStmt(setClause string, setArgs []interface{}, where string, whereArgs []interface{}) *SqlWithArgs {
	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s", escapeMySQLName(b.table.tableName), setClause, where)
	return &SqlWithArgs{Sql: stmt, Args: concatArgs(setArgs, whereArgs)}
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

// DeleteByPK 按主键删除
func (b *SQLBuilder) DeleteByPK(m proto.Message) (*SqlWithArgs, error) {
	return b.table.GetDeleteSQLWithArgs(m)
}

// DeleteByPKIf 带守卫条件的按主键删除：DELETE FROM t WHERE pk = ? AND <guard>
func (b *SQLBuilder) DeleteByPKIf(m proto.Message, guard string, guardArgs []interface{}) (*SqlWithArgs, error) {
	where, whereArgs, err := b.table.primaryKeyWhere(m)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s",
		escapeMySQLName(b.table.tableName), appendGuard(where, guard))
	return &SqlWithArgs{Sql: stmt, Args: concatArgs(whereArgs, guardArgs)}, nil
}

// DeleteWhere 按条件删除。whereClause 不得为空；确需全表删除时必须显式传入 "1=1"。
func (b *SQLBuilder) DeleteWhere(whereClause string, args []interface{}) (*SqlWithArgs, error) {
	where, err := requireMutationWhereClause(whereClause)
	if err != nil {
		return nil, err
	}
	return b.table.GetDeleteSQLByWhereWithArgs(where, args), nil
}

// DeleteWhereLimit 有界批删：DELETE FROM t WHERE ... [ORDER BY ...] LIMIT n。
// 保留期清理必须走这条：一次删干净会长时间持锁并撑爆 binlog，
// 正确做法是小批量循环删到 RowsAffected < limit 为止。orderBy 可为空。
func (b *SQLBuilder) DeleteWhereLimit(whereClause string, args []interface{}, orderBy string, limit int) (*SqlWithArgs, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("delete limit must be positive, got %d", limit)
	}
	where, err := requireMutationWhereClause(whereClause)
	if err != nil {
		return nil, err
	}
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(escapeMySQLName(b.table.tableName))
	sb.WriteString(" WHERE ")
	sb.WriteString(where)
	if orderBy != "" {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(orderBy)
	}
	sb.WriteString(" LIMIT ")
	sb.WriteString(strconv.Itoa(limit))
	return &SqlWithArgs{Sql: sb.String(), Args: args}, nil
}

// DeleteByKVIn 按某列的取值集合批删：DELETE FROM t WHERE col IN (?, ?, ...)
func (b *SQLBuilder) DeleteByKVIn(col string, vals []interface{}) (*SqlWithArgs, error) {
	where, err := b.inClause(col, len(vals))
	if err != nil {
		return nil, err
	}
	vals, err = b.table.normalizeColumnComparisonValues(col, vals)
	if err != nil {
		return nil, err
	}
	return b.DeleteWhere(where, vals)
}

// DeleteByPKIn 按主键取值集合批删（联合主键不适用）
func (b *SQLBuilder) DeleteByPKIn(pkValues []interface{}) (*SqlWithArgs, error) {
	col, err := b.singlePrimaryKey()
	if err != nil {
		return nil, err
	}
	return b.DeleteByKVIn(col, pkValues)
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

// requireMutationWhereClause 拒绝 UPDATE/DELETE 的隐式全表条件。
// 确需操作全表时，调用方必须显式传入 "1=1"，让危险意图在代码审查中可见。
func requireMutationWhereClause(whereClause string) (string, error) {
	if strings.TrimSpace(whereClause) == "" {
		return "", ErrEmptyWhereClause
	}
	return whereClause, nil
}

func isNumericKind(kind protoreflect.Kind) bool {
	switch kind {
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind,
		protoreflect.FloatKind, protoreflect.DoubleKind:
		return true
	default:
		return false
	}
}

// inClause 生成 `col` IN (?, ?, ...)，校验列存在且取值非空
func (b *SQLBuilder) inClause(col string, n int) (string, error) {
	if err := b.checkColumn(col); err != nil {
		return "", err
	}
	if n == 0 {
		return "", fmt.Errorf("%w: column %s", ErrEmptyValues, col)
	}
	return escapeMySQLName(col) + " IN (" + buildPlaceholders(n) + ")", nil
}

// singlePrimaryKey 返回唯一主键列名，联合主键或未声明主键时报错
func (b *SQLBuilder) singlePrimaryKey() (string, error) {
	if len(b.table.primaryKey) != 1 {
		return "", fmt.Errorf("%w: table %s needs exactly one primary key column, got %d",
			ErrPrimaryKeyNotFound, b.table.tableName, len(b.table.primaryKey))
	}
	return b.table.primaryKey[0], nil
}

// appendGuard 把守卫条件用 AND 接到已有 WHERE 片段后（guard 为空时原样返回）
func appendGuard(where, guard string) string {
	if strings.TrimSpace(guard) == "" {
		return where
	}
	return where + " AND " + guard
}

// concatArgs 拼接参数切片，返回新切片（避免 append 复用底层数组污染调用方）
func concatArgs(head, tail []interface{}) []interface{} {
	args := make([]interface{}, 0, len(head)+len(tail))
	args = append(args, head...)
	args = append(args, tail...)
	return args
}
