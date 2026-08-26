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

// CreateOrUpdateTable 建表并把线上结构对齐到当前 proto。
//
// ⚠️ 早先这个方法**只执行 CREATE TABLE IF NOT EXISTS，整个 GormDB 没有任何 ALTER 路径**。
// 在一张已经存在的表上，它是无条件、每次、永久的 no-op——注意这不是 DB.syncTableSchema
// 那种"并发下才触发"的竞态，而是百分之百不生效：proto 里加了字段，进程照常启动、
// 零日志零错误，一直到第一条 SELECT 才报 Error 1054 Unknown column，而且重启也不会好。
// 用 GormDB 的人以为自己有自动建表能力，实际上只有"第一次建表"能力。
//
// 现在与 DB.syncTableSchema 走同一套逻辑：建表（IF NOT EXISTS 可能是 no-op，所以
// 建完继续往下）→ 读 information_schema → buildAlterClauses → 一条 ALTER 落地。
func (p *GormDB) CreateOrUpdateTable(m proto.Message) error {
	table, err := p.tableForMessage(m)
	if err != nil {
		return err
	}
	registryKey := GetTableName(m)

	exists, err := p.isTableExists(table.tableName)
	if err != nil {
		return fmt.Errorf("检查表 %s 存在性: %w", table.tableName, err)
	}
	if !exists {
		if err := p.DB.Exec(table.GetCreateTableSQL()).Error; err != nil {
			return fmt.Errorf("创建表 %s 失败: %w", table.tableName, err)
		}
		// 刻意不 return：CREATE TABLE IF NOT EXISTS 在并发下可能整条是 no-op，
		// 理由与 DB.syncTableSchema 里那段完全一致。
	}

	currentCols, err := p.getTableColumnMeta(registryKey)
	if err != nil {
		return fmt.Errorf("获取表 %s 字段: %w", registryKey, err)
	}
	alterSQLs, err := table.buildAlterClauses(currentCols, p.expandOnly)
	if err != nil {
		return err
	}
	if len(alterSQLs) == 0 {
		return nil
	}

	alterSQL := fmt.Sprintf("ALTER TABLE %s %s",
		escapeMySQLName(table.tableName), strings.Join(alterSQLs, ", "))
	if err := p.DB.Exec(alterSQL).Error; err != nil {
		return fmt.Errorf("更新表 %s 结构失败: %w, SQL: %s", table.tableName, err, alterSQL)
	}
	table.clearColumnCache()
	return nil
}

// isTableExists 查 information_schema 判断表是否存在。
func (p *GormDB) isTableExists(tableName string) (bool, error) {
	var count int64
	err := p.DB.Raw(
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		p.DBName, tableName,
	).Scan(&count).Error
	return count > 0, err
}

// getTableColumnMeta 读线上每列的类型 + 从注释解析出的 proto 字段号。
// **刻意不走列类型缓存**：迁移不频繁，且这里要的是注释信息，混用会读到只有类型的旧缓存。
func (p *GormDB) getTableColumnMeta(registryKey string) (map[string]columnMeta, error) {
	table, ok := p.Tables[registryKey]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, registryKey)
	}

	var rows []struct {
		ColumnName    string
		ColumnType    string
		ColumnComment string
	}
	err := p.DB.Raw(
		"SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		p.DBName, table.tableName,
	).Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	metas := make(map[string]columnMeta, len(rows))
	for _, r := range rows {
		num, ok := parseFieldNumFromComment(r.ColumnComment)
		if !ok {
			num = 0
		}
		metas[r.ColumnName] = columnMeta{colType: r.ColumnType, fieldNum: num}
	}
	return metas, nil
}

func (p *GormDB) GetCreateTableSQL(message proto.Message) string {
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
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

func (p *GormDB) Save(message proto.Message) error {
	table, err := p.tableForMessage(message)
	if err != nil {
		return err
	}

	values, err := table.messageValues(message, true, false)
	if err != nil {
		return err
	}

	return p.DB.Table(escapeMySQLName(table.tableName)).
		Clauses(clause.OnConflict{UpdateAll: true}).
		Create(values).Error
}

func (p *GormDB) InsertOnDupUpdate(message proto.Message) error {
	return p.Save(message)
}

// InsertIgnore 幂等插入：主键/唯一键冲突时跳过不报错。返回是否实际插入了新行
func (p *GormDB) InsertIgnore(message proto.Message) (bool, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return false, err
	}

	values, err := table.messageValues(message, true, true)
	if err != nil {
		return false, err
	}

	result := p.DB.Table(escapeMySQLName(table.tableName)).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(values)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// InsertReturningID 插入并返回自增主键ID（LAST_INSERT_ID，同一连接内执行保证正确）
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
	err = p.DB.Connection(func(tx *gorm.DB) error {
		if err := tx.Exec(sqlWithArgs.Sql, sqlWithArgs.Args...).Error; err != nil {
			return err
		}
		return tx.Raw("SELECT LAST_INSERT_ID()").Scan(&id).Error
	})
	return id, err
}

// BatchSave 批量保存（INSERT ... ON DUPLICATE KEY UPDATE，自动分批）
func (p *GormDB) BatchSave(messages []proto.Message) error {
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

			values, err := table.messageValues(message, true, false)
			if err != nil {
				return err
			}
			rows = append(rows, values)
		}

		err = p.DB.Table(escapeMySQLName(table.tableName)).
			Clauses(clause.OnConflict{UpdateAll: true}).
			Create(rows).Error
		if err != nil {
			return err
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

	curVersion, err := pbconv.SerializeFieldValue(message, versionDesc)
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
	curVersion, err := pbconv.SerializeFieldValue(message, versionDesc)
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

	pkValues := make([]interface{}, 0, len(messages))
	for _, msg := range messages {
		if msg.ProtoReflect().Descriptor() != table.Descriptor {
			return fmt.Errorf("messages have different descriptors")
		}
		val, err := pbconv.SerializeFieldValue(msg, table.primaryKeyField)
		if err != nil {
			return fmt.Errorf("serialize primary key: %w", err)
		}
		pkValues = append(pkValues, val)
	}

	pkName := escapeMySQLName(string(table.primaryKeyField.Name()))
	for i := 0; i < len(pkValues); i += BatchInsertMaxSize {
		end := i + BatchInsertMaxSize
		if end > len(pkValues) {
			end = len(pkValues)
		}

		err := p.DB.Table(escapeMySQLName(table.tableName)).
			Where(pkName+" IN ?", pkValues[i:end]).
			Delete(nil).Error
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *GormDB) FindOneByKV(message proto.Message, whereKey string, whereVal string) error {
	return p.FindOneByWhereWithArgs(message, fmt.Sprintf("%s = ?", escapeMySQLName(whereKey)), []interface{}{whereVal})
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

	pkName := escapeMySQLName(string(table.primaryKeyField.Name()))
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
	if _, ok := table.fieldNameToDesc[field]; !ok {
		return fmt.Errorf("%w: %s in table %s", ErrFieldNotFound, field, table.tableName)
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
	tableName := GetTableName(message)
	table, ok := p.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}
	return table, nil
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
	return nil
}

func (m *MessageTable) primaryKeyValues(message proto.Message) ([]interface{}, error) {
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
