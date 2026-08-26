package proto2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/luyuancpp/proto2mysql/pbconv"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GormDB keeps the protobuf mapping layer, while delegating database access to GORM.
type GormDB struct {
	Tables map[string]*MessageTable
	DB     *gorm.DB
	DBName string
	// expandOnly 只允许「纯新增」的结构变更，语义与 DB.ExpandOnly 一致。用 SetExpandOnly 打开。
	expandOnly bool
}

func NewGormDB(db *gorm.DB, dbname string) *GormDB {
	return &GormDB{
		Tables: make(map[string]*MessageTable),
		DB:     db,
		DBName: dbname,
	}
}

func (p *GormDB) WithDB(db *gorm.DB) *GormDB {
	return &GormDB{
		Tables:     p.Tables,
		DB:         db,
		DBName:     p.DBName,
		expandOnly: p.expandOnly,
	}
}

// RegisterTable 注册Protobuf与表的映射关系。注册键固定为proto full name；
// table.tableName仅决定生成SQL中的表名，可用WithTableName自定义。
func (p *GormDB) RegisterTable(m proto.Message, opts ...TableOption) {
	table := newMessageTable(m, opts...)
	p.Tables[GetTableName(m)] = table
}

// ExpandOnly 只允许「纯新增」的结构变更，语义与 DB.ExpandOnly 完全一致。
// 见 DB 结构体上那段注释。
func (p *GormDB) SetExpandOnly(enabled bool) { p.expandOnly = enabled }

// CreateOrUpdateTable 把目标表交给 core DB.SyncAllTables 对齐。
//
// schema sync 不能在 GORM 侧另写一套：MySQL GET_LOCK 属于 session，DDL 与所有
// information_schema 查询必须固定在同一连接；TiDB ALTER 返回后还要等待 schema 对
// 其他节点可见。core 路径已经统一处理锁、元数据、索引/主键受支持的校验维度、ExpandOnly 与
// 可见性等待，这里只做 GORM → database/sql 的适配，并只注册调用方指定的这一张表。
//
// MySQL DDL 会隐式提交，所以事务内调用必须 fail-closed，不能悄悄逃出调用方事务。
func (p *GormDB) CreateOrUpdateTable(m proto.Message) error {
	table, err := p.tableForMessage(m)
	if err != nil {
		return err
	}
	registry := NewDB()
	registry.Tables = p.Tables
	if _, err := registry.registeredTablesForSQLGeneration(); err != nil {
		return err
	}
	if p.DB == nil {
		return errors.New("GormDB.CreateOrUpdateTable requires a database")
	}
	if p.inTransaction() {
		return fmt.Errorf("%w: GormDB.CreateOrUpdateTable; MySQL DDL implicitly commits",
			ErrSchemaSyncInTransaction)
	}

	sqlDB, err := p.DB.DB()
	if err != nil {
		return fmt.Errorf("get database/sql pool for GORM schema sync: %w", err)
	}
	core := NewDB()
	if p.DB.Statement != nil && p.DB.Statement.Context != nil {
		core.ctx = p.DB.Statement.Context
	}
	// 不能直接相信调用方传入的 DBName：information_schema 会按它读取，而未限定名的
	// ALTER 会在 DSN 当前库执行。二者错位时会从库 B 规划、改坏库 A。
	// 复用 core OpenDB 的 SELECT DATABASE()/lower_case_table_names 校验与规范化。
	if err := core.OpenDB(sqlDB, p.DBName); err != nil {
		return fmt.Errorf("validate GORM schema database: %w", err)
	}
	core.ExpandOnly = p.expandOnly
	core.Tables[GetTableName(m)] = table
	return core.SyncAllTables()
}

func (p *GormDB) GetCreateTableSQL(message proto.Message) string {
	table, err := p.tableForMessage(message)
	if err != nil {
		return ""
	}
	return table.GetCreateTableSQL()
}

func (p *GormDB) Insert(message proto.Message) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	values, err := table.messageValues(message, true, true)
	if err != nil {
		return err
	}

	return p.DB.Table(escapeMySQLName(table.tableName)).Create(values).Error
}

func (p *GormDB) BatchInsert(messages []proto.Message) error {
	if len(messages) == 0 {
		return nil
	}

	for i := 0; i < len(messages); i += BatchInsertMaxSize {
		end := i + BatchInsertMaxSize
		if end > len(messages) {
			end = len(messages)
		}

		table, err := p.tableForMessage(messages[i])
		if err != nil {
			return err
		}

		rows := make([]map[string]interface{}, 0, end-i)
		for _, message := range messages[i:end] {
			if message.ProtoReflect().Descriptor() != table.Descriptor {
				return fmt.Errorf("messages have different descriptors")
			}

			values, err := table.messageValues(message, true, true)
			if err != nil {
				return err
			}
			rows = append(rows, values)
		}

		if err := p.DB.Table(escapeMySQLName(table.tableName)).Create(rows).Error; err != nil {
			return err
		}
	}

	return nil
}

// Save 按完整主键有则更新、无则插入。
//
// MySQL 的 ODKU 会在任意 UNIQUE 冲突时更新命中行，无法表达“只把主键冲突当作
// upsert”。因此这里与 DB.Save 一样走 UPDATE-by-PK → INSERT → 并发同主键重试。
// 备用 UNIQUE 命中主键不同的行时，重试 UPDATE 仍为 0，最终返回 ErrDuplicateKey，
// 不会把入参的非主键字段写进另一条记录。
func (p *GormDB) Save(message proto.Message) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}
	return p.saveByPrimaryKey(table, message)
}

func wrapGormExecErr(err error) error {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return fmt.Errorf("%w: %w", ErrDuplicateKey, err)
	}
	return wrapExecErr(err)
}

func (p *GormDB) saveByPrimaryKey(table *MessageTable, message proto.Message) error {
	update, err := table.getSaveUpdateSQLWithArgs(message)
	if update == nil || err != nil {
		return fmt.Errorf("generate primary-key save update for table %s: %w", table.tableName, err)
	}
	insert, err := table.GetInsertSQLWithArgs(message)
	if insert == nil || err != nil {
		return fmt.Errorf("generate save insert for table %s: %w", table.tableName, err)
	}

	result := p.DB.Exec(update.Sql, update.Args...)
	if result.Error != nil {
		return fmt.Errorf("update existing row for save on table %s: %w", table.tableName, wrapGormExecErr(result.Error))
	}
	if result.RowsAffected > 0 {
		return nil
	}

	result = p.DB.Exec(insert.Sql, insert.Args...)
	if result.Error == nil {
		return nil
	}
	duplicateErr := wrapGormExecErr(result.Error)
	if !errors.Is(duplicateErr, ErrDuplicateKey) {
		return fmt.Errorf("insert missing row for save on table %s: %w", table.tableName, duplicateErr)
	}

	// UPDATE 返回 0 既可能是主键不存在，也可能是行存在但所有值都没变。
	// INSERT 的 1062 还可能来自“另一请求刚插入同主键”。重试完整主键 UPDATE，
	// 再用锁定 current read 确认当前行的全部非主键值确实与目标一致。
	result = p.DB.Exec(update.Sql, update.Args...)
	if result.Error != nil {
		return fmt.Errorf("retry primary-key save update for table %s: %w", table.tableName, wrapGormExecErr(result.Error))
	}
	if result.RowsAffected > 0 {
		return nil
	}
	match, matchErr := table.getSaveCurrentRowMatchSQLWithArgs(update)
	if matchErr != nil {
		return fmt.Errorf("generate duplicate classification query for save on table %s: %w", table.tableName, matchErr)
	}
	rows, matchErr := p.DB.Raw(match.Sql, match.Args...).Rows()
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
		return nil
	}
	return fmt.Errorf("save on table %s conflicts with a different unique row: %w", table.tableName, duplicateErr)
}

func (p *GormDB) InsertOnDupUpdate(message proto.Message) error {
	return p.Save(message)
}

// InsertIgnore 幂等插入：只把 MySQL 1062（主键/唯一键冲突）解释成“未插入”。
//
// 不能使用 INSERT IGNORE：IGNORE 会把 1406 截断、越界、NOT NULL 等真实错误也降级成
// warning，并把修正后的数据写进去。普通 INSERT + 精确识别 1062 才不会吞数据错误。
func (p *GormDB) InsertIgnore(message proto.Message) (bool, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return false, err
	}

	insert, err := table.GetInsertSQLWithArgs(message)
	if insert == nil || err != nil {
		return false, fmt.Errorf("generate insert SQL for table %s: %w", table.tableName, err)
	}
	result := p.DB.Exec(insert.Sql, insert.Args...)
	if result.Error != nil {
		wrapped := wrapGormExecErr(result.Error)
		if errors.Is(wrapped, ErrDuplicateKey) {
			return false, nil
		}
		return false, fmt.Errorf("exec insert for table %s: %w", table.tableName, wrapped)
	}
	return result.RowsAffected > 0, nil
}

// InsertReturningID 插入并返回自增主键 ID（LAST_INSERT_ID，同一连接内执行保证正确）。
//
// ⚠️ **不能用 p.DB.Connection()**。它看起来正是"把这段跑在同一条连接上"的意思，
// 但 gorm@v1.30.0/finisher_api.go 里它做的是：
//
//	sqlDB, _ := tx.DB()          // 从 *sql.Tx 里用 unsafe 反射掏出底层 *sql.DB（gorm.go:DB()）
//	conn, _ := sqlDB.Conn(ctx)   // 从**连接池**新借一条连接
//	tx.Statement.ConnPool = conn
//
// 也就是说在事务里调它，这条 INSERT 会跑在**事务之外的另一条连接**上——
// 外层 Rollback 覆盖不到它，数据留在库里；而调用方看到的是"事务回滚了"。
// 这是静默的：没有报错，只是那一行凭空多出来。
//
// 正确做法是直接用当前 *gorm.DB（事务里它就是事务、事务外它就是池）。
// 事务外的正确性靠的不是"钉住连接"而是 MySQL 的语义：LAST_INSERT_ID() 是
// **连接级**的，而 database/sql 在同一条 *sql.DB 上的两次调用可能落在不同连接——
// 所以事务外这里显式借一条连接来跑，事务内则必须原样用事务。
func (p *GormDB) InsertReturningID(message proto.Message) (int64, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return 0, err
	}

	sqlWithArgs, err := table.GetInsertSQLWithArgs(message)
	if sqlWithArgs == nil || err != nil {
		return 0, fmt.Errorf("generate insert SQL for table %s: %w", table.tableName, err)
	}

	var id int64

	// 事务内：直接用事务本身，绝不另开连接。
	if p.inTransaction() {
		if err := p.DB.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...).Error; err != nil {
			return 0, err
		}
		if err := p.DB.Raw("SELECT LAST_INSERT_ID()").Scan(&id).Error; err != nil {
			return 0, err
		}
		return id, nil
	}

	// 事务外：借一条连接把 INSERT 与 LAST_INSERT_ID() 绑在同一个 session 上。
	err = p.DB.Connection(func(tx *gorm.DB) error {
		if err := tx.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...).Error; err != nil {
			return err
		}
		return tx.Raw("SELECT LAST_INSERT_ID()").Scan(&id).Error
	})
	return id, err
}

// inTransaction 当前 *gorm.DB 是不是绑在一个事务上。
// GORM 开事务时会把 Statement.ConnPool 换成 *sql.Tx，这是唯一可靠的判据。
func (p *GormDB) inTransaction() bool {
	if p.DB == nil || p.DB.Statement == nil {
		return false
	}
	_, ok := p.DB.Statement.ConnPool.(gorm.TxCommitter)
	return ok
}

// BatchSave 逐行执行与 Save 相同的完整主键语义。
// 单条批量 ODKU 无法区分同主键冲突与备用 UNIQUE 冲突，所以这里刻意不合并 SQL。
func (p *GormDB) BatchSave(messages []proto.Message) error {
	if len(messages) == 0 {
		return nil
	}

	table, err := p.tableForMessage(messages[0])
	if err != nil {
		return err
	}
	for _, message := range messages {
		if err := table.validateMessageDescriptor(message); err != nil {
			return err
		}
	}

	for i, message := range messages {
		if err := p.saveByPrimaryKey(table, message); err != nil {
			return fmt.Errorf("batch save row %d for table %s: %w", i, table.tableName, err)
		}
	}
	return nil
}

func (p *GormDB) Update(message proto.Message) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	values, err := table.messageValues(message, false, false)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return fmt.Errorf("no fields to update")
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}

	return p.DB.Table(escapeMySQLName(table.tableName)).Where(whereClause, whereArgs...).Updates(values).Error
}

func (p *GormDB) UpdateByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	values, err := table.messageValues(message, false, false)
	if err != nil {
		return err
	}
	if len(values) == 0 {
		return fmt.Errorf("no fields to update")
	}

	return p.DB.Table(escapeMySQLName(table.tableName)).Where(whereClause, whereArgs...).Updates(values).Error
}

// UpdateFieldsByPK 按主键只更新指定字段（部分更新），避免Update全字段覆盖冲掉并发写入
func (p *GormDB) UpdateFieldsByPK(message proto.Message, fields ...string) error {
	if len(fields) == 0 {
		return errors.New("no fields to update")
	}
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	values := make(map[string]interface{}, len(fields))
	for _, field := range fields {
		desc, ok := table.fieldNameToDesc[field]
		if !ok {
			return fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, field, table.tableName)
		}
		val, err := pbconv.SerializeFieldValue(message, desc)
		if err != nil {
			return fmt.Errorf("serialize update field %s: %w", field, err)
		}
		values[field] = val
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}
	return p.DB.Table(escapeMySQLName(table.tableName)).Where(whereClause, whereArgs...).Updates(values).Error
}

// UpdateKVByPK 按主键设置单个字段的值（如改状态、封号）
func (p *GormDB) UpdateKVByPK(message proto.Message, field string, value interface{}) error {
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
	return p.DB.Table(escapeMySQLName(table.tableName)).Where(whereClause, whereArgs...).Update(field, value).Error
}

// UpdateIfVersion 乐观锁CAS更新：按主键更新消息中已设置的字段（versionField自动+1），
// 仅当数据库中versionField等于message当前值时生效。返回false表示版本冲突，调用方应重读后重试
func (p *GormDB) UpdateIfVersion(message proto.Message, versionField string) (bool, error) {
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
	curVersion, err := comparisonValue(message, versionDesc)
	if err != nil {
		return false, fmt.Errorf("serialize version field %s: %w", versionField, err)
	}

	values, err := table.messageValues(message, false, false)
	if err != nil {
		return false, err
	}
	delete(values, versionField)
	for _, pk := range table.primaryKey {
		delete(values, pk)
	}
	if len(values) == 0 {
		return false, errors.New("no fields to update")
	}

	escapedVersion := escapeMySQLName(versionField)
	values[versionField] = gorm.Expr(escapedVersion + " + 1")

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return false, err
	}

	result := p.DB.Table(escapeMySQLName(table.tableName)).
		Where(whereClause, whereArgs...).
		Where(escapedVersion+" = ?", curVersion).
		Updates(values)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// UpdateFieldsIfVersion 乐观锁CAS+显式字段列表：不用Has()自动挑字段，
// 规避proto3隐式presence下零值字段被跳过的坑。返回false=版本冲突
func (p *GormDB) UpdateFieldsIfVersion(message proto.Message, versionField string, fields ...string) (bool, error) {
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
	curVersion, err := comparisonValue(message, versionDesc)
	if err != nil {
		return false, fmt.Errorf("serialize version field %s: %w", versionField, err)
	}

	values := make(map[string]interface{}, len(fields)+1)
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
		values[name] = val
	}
	escapedVersion := escapeMySQLName(versionField)
	values[versionField] = gorm.Expr(escapedVersion + " + 1")

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return false, err
	}

	result := p.DB.Table(escapeMySQLName(table.tableName)).
		Where(whereClause, whereArgs...).
		Where(escapedVersion+" = ?", curVersion).
		Updates(values)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (p *GormDB) Delete(message proto.Message) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}

	return p.DB.Table(escapeMySQLName(table.tableName)).Where(whereClause, whereArgs...).Delete(nil).Error
}

func (p *GormDB) DeleteByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	return p.DB.Table(escapeMySQLName(table.tableName)).Where(whereClause, whereArgs...).Delete(nil).Error
}

// DeleteByKV 按单个字段等值条件删除
func (p *GormDB) DeleteByKV(message proto.Message, key string, value interface{}) error {
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
func (p *GormDB) BatchDelete(messages []proto.Message) error {
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
		if msg.ProtoReflect().Descriptor() != table.Descriptor {
			return fmt.Errorf("messages have different descriptors")
		}
	}

	// ⚠️ 必须按**全部**主键列构造条件。原先这里只取 primaryKeyField
	// （= primaryKey[0]，见 Init），复合主键下发出去的是
	//
	//	DELETE FROM t WHERE `a` IN (...)
	//
	// ——**第二个分量整个被忽略**。想删 (a=1,b=1) 一行，实际删掉的是 a=1 的**所有行**，
	// 包括 (1,2)、(1,3)…… 语句成功、无报错，只是数据没了。
	// DB.BatchDelete 一直用的是元组 IN 的正确写法，这里对齐它。
	pkNames := make([]string, len(table.primaryKey))
	for i, pk := range table.primaryKey {
		pkNames[i] = escapeMySQLName(pk)
	}

	for i := 0; i < len(messages); i += BatchInsertMaxSize {
		end := i + BatchInsertMaxSize
		if end > len(messages) {
			end = len(messages)
		}

		var args []interface{}
		tuples := make([]string, 0, end-i)
		for _, msg := range messages[i:end] {
			values, err := table.primaryKeyValues(msg)
			if err != nil {
				return err
			}
			args = append(args, values...)
			tuples = append(tuples, "("+buildPlaceholders(len(table.primaryKey))+")")
		}

		where := fmt.Sprintf("(%s) IN (%s)", strings.Join(pkNames, ", "), strings.Join(tuples, ", "))
		if err := p.DB.Table(escapeMySQLName(table.tableName)).
			Where(where, args...).
			Delete(nil).Error; err != nil {
			return err
		}
	}
	return nil
}

func (p *GormDB) FindOneByKV(message proto.Message, whereKey string, whereVal string) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}
	value, err := table.normalizeColumnComparisonValue(whereKey, whereVal)
	if err != nil {
		return err
	}
	return p.FindOneByWhereWithArgs(message, fmt.Sprintf("%s = ?", escapeMySQLName(whereKey)), []interface{}{value})
}

// FindOneByPK 按消息中的主键值查询单条数据（查到后覆盖message其余字段）
func (p *GormDB) FindOneByPK(message proto.Message) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}
	return p.FindOneByWhereWithArgs(message, whereClause, whereArgs)
}

// FindAllByKVIn 按单个字段的IN条件查询批量数据（WHERE key IN (...)）
func (p *GormDB) FindAllByKVIn(list proto.Message, key string, values []interface{}) error {
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
	return p.FindAllByWhereWithArgs(list, escapeMySQLName(key)+" IN ?", []interface{}{values})
}

// FindAllByPKIn 按主键批量查询，返回列表（类似Redis MGET：不存在的主键自动跳过）
func (p *GormDB) FindAllByPKIn(list proto.Message, pkValues []interface{}) error {
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
	// 复合主键 fail-closed，理由同 DB.FindAllByPKIn：只按第一列过滤会读回
	// 属于别的主键的行，而调用方看不出范围被放大了。
	if len(table.primaryKey) > 1 {
		return fmt.Errorf("%w: 表 %s 是复合主键 %v，FindAllByPKIn 只能按单列主键查。"+
			"它会退化成只按第一列过滤，读回属于别的主键的行。"+
			"请改用 FindAllByWhereWithArgs 自拼 (%s) IN ((?,?),...)",
			ErrPrimaryKeyNotFound, table.tableName, table.primaryKey,
			strings.Join(table.primaryKey, ","))
	}

	pkName := escapeMySQLName(string(table.primaryKeyField.Name()))
	pkValues, err = table.normalizeColumnComparisonValues(string(table.primaryKeyField.Name()), pkValues)
	if err != nil {
		return err
	}
	return p.FindAllByWhereWithArgs(list, pkName+" IN ?", []interface{}{pkValues})
}

// FindOrCreate 按主键查询，不存在则用message当前值插入（玩家首次登录常用）。
// 返回created表示是否新建了记录。
func (p *GormDB) FindOrCreate(message proto.Message) (created bool, err error) {
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

// FindOneByPKForUpdate 按主键查询并加行锁（SELECT ... FOR UPDATE），
// 仅在Transaction内有意义，用于防止并发修改同一玩家数据
func (p *GormDB) FindOneByPKForUpdate(message proto.Message) error {
	if !p.inTransaction() {
		return errors.New("FindOneByPKForUpdate must be called inside GormDB.Transaction")
	}

	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	whereClause, whereArgs, err := table.primaryKeyWhere(message)
	if err != nil {
		return err
	}

	rows, err := p.DB.Table(escapeMySQLName(table.tableName)).
		Select(table.fieldsListSQL).
		Where(whereClause, whereArgs...).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Limit(2).
		Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanOneProtoRow(rows, message)
}

// IncrByPK 按主键对数值字段原子加减（UPDATE ... SET f = f + delta），
// 适合货币/经验等计数器，避免"读-改-写"竞态
func (p *GormDB) IncrByPK(message proto.Message, field string, delta int64) error {
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
	return p.DB.Table(escapeMySQLName(table.tableName)).
		Where(whereClause, whereArgs...).
		Update(field, gorm.Expr(escapedField+" + ?", delta)).Error
}

// DecrByPKIfEnough 按主键原子扣减数值字段，余额不足时不扣并返回false
// （防止负数余额，扣钱/扣道具常用）
func (p *GormDB) DecrByPKIfEnough(message proto.Message, field string, delta int64) (bool, error) {
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
	result := p.DB.Table(escapeMySQLName(table.tableName)).
		Where(whereClause, whereArgs...).
		Where(escapedField+" >= ?", delta).
		Update(field, gorm.Expr(escapedField+" - ?", delta))
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (p *GormDB) FindOneByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	rows, err := p.DB.Table(escapeMySQLName(table.tableName)).
		Select(table.fieldsListSQL).
		Where(whereClause, whereArgs...).
		Limit(2).
		Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanOneProtoRow(rows, message)
}

func (p *GormDB) FindAll(message proto.Message) error {
	return p.FindAllByWhereWithArgs(message, "1=1", nil)
}

func (p *GormDB) FindAllByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error {
	table, listField, err := resolveListTable(p.Tables, message)
	if err != nil {
		return err
	}

	rows, err := p.DB.Table(escapeMySQLName(table.tableName)).
		Select(table.fieldsListSQL).
		Where(whereClause, whereArgs...).
		Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanProtoRowsToList(rows, message.ProtoReflect().Mutable(listField).List())
}

// FindAllWithOptions 按条件查询批量数据，支持ORDER BY / LIMIT / OFFSET
func (p *GormDB) FindAllWithOptions(list proto.Message, whereClause string, whereArgs []interface{}, opts QueryOptions) error {
	table, listField, err := resolveListTable(p.Tables, list)
	if err != nil {
		return err
	}

	query := p.DB.Table(escapeMySQLName(table.tableName)).
		Select(table.fieldsListSQL).
		Where(normalizeWhereClause(whereClause), whereArgs...)
	if opts.OrderBy != "" {
		query = query.Order(opts.OrderBy)
	}
	if opts.Limit > 0 {
		query = query.Limit(opts.Limit)
		if opts.Offset > 0 {
			query = query.Offset(opts.Offset)
		}
	}
	// ForUpdate 原先在 GormDB 侧被整个忽略：调用方传 QueryOptions{ForUpdate: true}
	// 期待的是"读到加锁后的最新值"，发出去的却是一条普通 SELECT。行锁静默丢失，
	// 于是"先锁后改"退化成读-改-写，并发下丢更新——而 DB 侧同一个选项是生效的
	// （QueryOptions.sqlSuffix 会拼 FOR UPDATE），两条路径行为不一致且零提示。
	if opts.ForUpdate {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}

	rows, err := query.Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanProtoRowsToList(rows, list.ProtoReflect().Mutable(listField).List())
}

// FindPage 分页查询批量数据（pageIndex从1开始）
func (p *GormDB) FindPage(list proto.Message, whereClause string, whereArgs []interface{}, pageIndex, pageSize int) error {
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
func (p *GormDB) FindOneWithOptions(message proto.Message, whereClause string, whereArgs []interface{}, opts QueryOptions) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	query := p.DB.Table(escapeMySQLName(table.tableName)).
		Select(table.fieldsListSQL).
		Where(normalizeWhereClause(whereClause), whereArgs...)
	if opts.OrderBy != "" {
		query = query.Order(opts.OrderBy)
	}
	if opts.ForUpdate {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}

	rows, err := query.Limit(1).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	return scanOneProtoRow(rows, message)
}

// FindPageByCursor 游标分页（keyset pagination）：按cursorField升序返回cursorVal之后的pageSize条，
// 深分页时性能远好于OFFSET。首页传cursorVal=nil，下一页传上一页最后一条的cursorField值。
// cursorField应有索引且唯一（如自增id）。
func (p *GormDB) FindPageByCursor(list proto.Message, whereClause string, whereArgs []interface{}, cursorField string, cursorVal interface{}, pageSize int) error {
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
func (p *GormDB) Count(message proto.Message) (int64, error) {
	return p.CountByWhereWithArgs(message, "", nil)
}

// CountByWhereWithArgs 按条件统计行数，message可为行消息或列表消息
func (p *GormDB) CountByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) (int64, error) {
	table, err := resolveAnyTable(p.Tables, message)
	if err != nil {
		return 0, err
	}

	var count int64
	err = p.DB.Table(escapeMySQLName(table.tableName)).
		Where(normalizeWhereClause(whereClause), whereArgs...).
		Count(&count).Error
	return count, err
}

// Exists 判断是否存在满足条件的行，message可为行消息或列表消息
func (p *GormDB) Exists(message proto.Message, whereClause string, whereArgs []interface{}) (bool, error) {
	table, err := resolveAnyTable(p.Tables, message)
	if err != nil {
		return false, err
	}

	rows, err := p.DB.Table(escapeMySQLName(table.tableName)).
		Select("1").
		Where(normalizeWhereClause(whereClause), whereArgs...).
		Limit(1).
		Rows()
	if err != nil {
		return false, err
	}
	defer rows.Close()

	exists := rows.Next()
	return exists, rows.Err()
}

// ExistsByPK 按消息中的主键值判断行是否存在
func (p *GormDB) ExistsByPK(message proto.Message) (bool, error) {
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

func (p *GormDB) Transaction(fn func(tx *GormDB) error) error {
	return p.DB.Transaction(func(tx *gorm.DB) error {
		return fn(p.WithDB(tx))
	})
}

func (p *GormDB) tableForMessage(message proto.Message) (*MessageTable, error) {
	return tableForRegistryKey(p.Tables, GetTableName(message))
}

func (m *MessageTable) messageValues(message proto.Message, includeUnset bool, skipUnsetAutoIncrement bool) (map[string]interface{}, error) {
	if err := m.validateMessageDescriptor(message); err != nil {
		return nil, err
	}

	values := make(map[string]interface{}, m.Descriptor.Fields().Len())
	reflection := message.ProtoReflect()

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		fieldName := string(field.Name())

		if !includeUnset && !reflection.Has(field) {
			continue
		}
		if skipUnsetAutoIncrement && m.isAutoIncrementField(fieldName) && !reflection.Has(field) {
			continue
		}

		val, err := pbconv.SerializeFieldValue(message, field)
		if err != nil {
			return nil, fmt.Errorf("serialize field %s: %w", field.Name(), err)
		}
		values[fieldName] = val
	}

	return values, nil
}

func (m *MessageTable) validateMessageDescriptor(message proto.Message) error {
	if message == nil {
		return fmt.Errorf("message cannot be nil")
	}
	if message.ProtoReflect().Descriptor() != m.Descriptor {
		return fmt.Errorf("message descriptor %s does not match table %s", message.ProtoReflect().Descriptor().FullName(), m.tableName)
	}
	return m.validateFieldKinds()
}

func (m *MessageTable) primaryKeyValues(message proto.Message) ([]interface{}, error) {
	values, err := m.primaryKeySerializedValues(message)
	if err != nil {
		return nil, err
	}
	for i, primaryKey := range m.primaryKey {
		field := m.fieldNameToDesc[primaryKey]
		values[i], err = normalizeComparisonArg(field, values[i])
		if err != nil {
			return nil, fmt.Errorf("normalize primary key %s: %w", primaryKey, err)
		}
	}
	return values, nil
}

// primaryKeySerializedValues 返回 protobuf 的稳定十进制/字节表示。
// 缓存 key 依赖这份表示保持升级兼容；SQL predicate 必须再由 primaryKeyValues
// 转为带类型参数，避免 MySQL 把 64 位整数经 DOUBLE 比较后丢精度。
func (m *MessageTable) primaryKeySerializedValues(message proto.Message) ([]interface{}, error) {
	if err := m.validateMessageDescriptor(message); err != nil {
		return nil, err
	}
	if len(m.primaryKey) == 0 {
		return nil, ErrPrimaryKeyNotFound
	}

	values := make([]interface{}, 0, len(m.primaryKey))
	for _, primaryKey := range m.primaryKey {
		field, ok := m.fieldNameToDesc[primaryKey]
		if !ok {
			return nil, fmt.Errorf("%w: primary key %s in table %s", ErrFieldNotFound, primaryKey, m.tableName)
		}

		val, err := pbconv.SerializeFieldValue(message, field)
		if err != nil {
			return nil, fmt.Errorf("serialize primary key %s: %w", primaryKey, err)
		}
		values = append(values, val)
	}
	return values, nil
}

func (m *MessageTable) primaryKeyWhere(message proto.Message) (string, []interface{}, error) {
	whereArgs, err := m.primaryKeyValues(message)
	if err != nil {
		return "", nil, err
	}

	whereClause := ""
	for i, primaryKey := range m.primaryKey {
		if i > 0 {
			whereClause += " AND "
		}
		whereClause += fmt.Sprintf("%s = ?", escapeMySQLName(primaryKey))
	}

	return whereClause, whereArgs, nil
}

func scanOneProtoRow(rows *sql.Rows, message proto.Message) error {
	found := false
	for rows.Next() {
		if found {
			return ErrMultipleRowsFound
		}

		result, err := scanRowStrings(rows)
		if err != nil {
			return err
		}
		if err := pbconv.ParseFromString(message, result); err != nil {
			return err
		}
		found = true
	}

	if err := rows.Err(); err != nil {
		return err
	}
	if !found {
		return ErrNoRowsFound
	}

	return nil
}

func scanRowStrings(rows *sql.Rows) ([]string, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	columnValues := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range columnValues {
		scans[i] = &columnValues[i]
	}

	if err := rows.Scan(scans...); err != nil {
		return nil, err
	}

	result := make([]string, len(columns))
	for i, v := range columnValues {
		result[i] = string(v)
	}

	return result, nil
}
