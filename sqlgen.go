package proto2mysql

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
)

// GenerateCreateTableSQL 直接由 proto.Message 生成 CREATE TABLE 语句，无需 RegisterTable 也无需连库。
// 可用 WithPrimaryKey / WithIndexes / WithUniqueKey / WithTableName 等 TableOption 定制表结构。
//
//	sql := proto2mysql.GenerateCreateTableSQL(&pb.Player{}, proto2mysql.WithPrimaryKey("id"))
func GenerateCreateTableSQL(m proto.Message, opts ...TableOption) string {
	return newMessageTable(m, opts...).GetCreateTableSQL()
}

// WriteCreateTableSQL 把所有已注册表的 CREATE TABLE 语句写入 w（按表名排序，输出稳定），
// 用于离线生成 schema.sql，无需连库。建议在 RegisterTable 完成后调用。
func (p *DB) WriteCreateTableSQL(w io.Writer) error {
	// 按 **table_name** 排序，不是按注册键（proto full name）。
	//
	// 两者在没声明 table_name 选项时恰好相同，所以这个分叉长期看不出来；一旦用了
	// WithTableName（比如 proto 叫 game.v1.PlayerData、表叫 player_data），
	// Go 按注册键排、Python 按表名排，**同一批语句会以不同顺序落进 schema.sql**——
	// 逐字节就不一致了，而现有 golden 是逐表断言、盖不到文件级顺序。
	//
	// 排序键取 table_name：它才是真正出现在 SQL 里的东西，也是人看 schema.sql 时
	// 会用来找表的东西。
	tables := make([]*MessageTable, 0, len(p.Tables))
	for _, table := range p.Tables {
		tables = append(tables, table)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].tableName < tables[j].tableName })

	for _, table := range tables {
		if _, err := fmt.Fprintln(w, table.GetCreateTableSQL()); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	return nil
}

// DumpCreateTableSQLFile 把所有已注册表的建表语句写到 path 指定的文件（覆盖写）。
func (p *DB) DumpCreateTableSQLFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create schema file %s: %w", path, err)
	}
	defer f.Close()

	if err := p.WriteCreateTableSQL(f); err != nil {
		return err
	}
	return f.Close()
}

// GenerateMigrationSQL 生成把线上表结构对齐到 proto 定义所需的 SQL：
//   - 表不存在   → 返回 CREATE TABLE 语句
//   - 表已存在   → 返回 ALTER TABLE（新增字段 / 按字段号改名 / 类型对齐）语句
//   - 无任何差异 → 返回空串
//
// 需要已连库（读 information_schema 比对当前结构），且该消息对应表已 RegisterTable。
// 与 UpdateTableField 的区别：只产出 SQL 不执行，便于生成迁移文件供人工/CI 审核。
func (p *DB) GenerateMigrationSQL(m proto.Message) (string, error) {
	tableName := GetTableName(m)
	table, ok := p.Tables[tableName]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}

	exists, err := p.IsTableExists(table.tableName)
	if err != nil {
		return "", fmt.Errorf("check table %s exists: %w", table.tableName, err)
	}
	if !exists {
		return table.GetCreateTableSQL(), nil
	}

	currentCols, err := p.getTableColumnMeta(tableName)
	if err != nil {
		return "", fmt.Errorf("get table %s columns: %w", tableName, err)
	}

	alterSQLs, err := table.buildAlterClauses(currentCols, p.ExpandOnly)
	if err != nil {
		return "", err
	}
	if len(alterSQLs) == 0 {
		return "", nil
	}
	return fmt.Sprintf("ALTER TABLE %s %s;", escapeMySQLName(table.tableName), strings.Join(alterSQLs, ", ")), nil
}

// WriteMigrationSQL 依次为每个消息生成迁移 SQL 并写入 w（无差异的表自动跳过），需连库。
// 常用于生成一份 migrate.sql：把当前库结构对齐到最新 proto 定义。
func (p *DB) WriteMigrationSQL(w io.Writer, messages ...proto.Message) error {
	for _, m := range messages {
		stmt, err := p.GenerateMigrationSQL(m)
		if err != nil {
			return err
		}
		if stmt == "" {
			continue
		}
		if _, err := fmt.Fprintln(w, stmt); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	return nil
}

// DumpMigrationSQLFile 把多个消息的迁移 SQL 写到 path 指定的文件（覆盖写），需连库。
func (p *DB) DumpMigrationSQLFile(path string, messages ...proto.Message) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create migration file %s: %w", path, err)
	}
	defer f.Close()

	if err := p.WriteMigrationSQL(f, messages...); err != nil {
		return err
	}
	return f.Close()
}
