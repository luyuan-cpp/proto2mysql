package proto2mysql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
)

// ErrDuplicateTableMapping 表示两个不同 protobuf message 被注册到同一张物理表。
// 离线 DDL 不能替调用方猜哪一份定义才是权威：CREATE TABLE IF NOT EXISTS 会让
// 第一份静默胜出，而迁移规划会针对同一张线上表生成互相冲突的两套语句。
var ErrDuplicateTableMapping = errors.New("multiple protobuf messages map to the same physical table")

// GenerateCreateTableSQL 直接由 proto.Message 生成 CREATE TABLE 语句，无需 RegisterTable 也无需连库。
// 可用 WithPrimaryKey / WithIndexes / WithUniqueKey / WithTableName 等 TableOption 定制表结构。
// 该历史 API 没有 error 返回位；校验失败时返回空串。需要错误原因请用 checked 版本。
//
//	sql := proto2mysql.GenerateCreateTableSQL(&pb.Player{}, proto2mysql.WithPrimaryKey("id"))
func GenerateCreateTableSQL(m proto.Message, opts ...TableOption) string {
	stmt, _ := GenerateCreateTableSQLChecked(m, opts...)
	return stmt
}

// GenerateCreateTableSQLChecked 是带错误通道的建表 SQL 入口。
// 不支持的字段类型、真实 oneof、超长表名会在任何 DDL 产出前 fail-closed。
func GenerateCreateTableSQLChecked(m proto.Message, opts ...TableOption) (string, error) {
	table := newMessageTable(m, opts...)
	if err := table.validateSchemaDefinition(); err != nil {
		return "", err
	}
	return table.GetCreateTableSQL(), nil
}

type sqlGenerationTable struct {
	registryKey string
	table       *MessageTable
}

// sortAndValidateTableMappings 按物理表名、再按 proto 注册键确定性排序，并拒绝
// 不同 message 竞争同一物理表。调用方必须在任何 writer 或 DB I/O 前调用。
func sortAndValidateTableMappings(tables []sqlGenerationTable) error {
	sort.Slice(tables, func(i, j int) bool {
		left, right := tables[i], tables[j]
		if left.table.tableName != right.table.tableName {
			return left.table.tableName < right.table.tableName
		}
		return left.registryKey < right.registryKey
	})
	// 保留历史的原始 table_name 输出顺序，避免仅为判重改变 schema.sql 的稳定字节；
	// 冲突检查用确定性排序后的全对扫描，而不是只看相邻项。后者会漏掉
	// "Keys" / "Zulu" / "Keys"：K 与 Kelvin sign EqualFold，但原始排序并不相邻。
	for i := 0; i < len(tables); i++ {
		for j := i + 1; j < len(tables); j++ {
			prev, current := tables[i], tables[j]
			if !strings.EqualFold(prev.table.tableName, current.table.tableName) {
				continue
			}
			if prev.registryKey == current.registryKey && prev.table.tableName == current.table.tableName {
				return fmt.Errorf("%w: table %q (%s) was requested more than once",
					ErrDuplicateTableMapping, prev.table.tableName, prev.registryKey)
			}
			return fmt.Errorf("%w: table %q (%s) conflicts with %q (%s) under case-insensitive comparison",
				ErrDuplicateTableMapping,
				prev.table.tableName, prev.registryKey,
				current.table.tableName, current.registryKey)
		}
	}
	return nil
}

func (p *DB) registeredTablesForSQLGeneration() ([]sqlGenerationTable, error) {
	tables := make([]sqlGenerationTable, 0, len(p.Tables))
	for registryKey, table := range p.Tables {
		if table == nil {
			return nil, fmt.Errorf("registered table %s is nil", registryKey)
		}
		tables = append(tables, sqlGenerationTable{registryKey: registryKey, table: table})
	}
	if err := sortAndValidateTableMappings(tables); err != nil {
		return nil, err
	}
	return tables, nil
}

func (p *DB) validateMigrationMessageMappings(messages []proto.Message) error {
	tables := make([]sqlGenerationTable, 0, len(messages))
	for _, message := range messages {
		registryKey := GetTableName(message)
		table, ok := p.Tables[registryKey]
		if !ok || table == nil {
			return fmt.Errorf("%w: %s", ErrTableNotFound, registryKey)
		}
		tables = append(tables, sqlGenerationTable{registryKey: registryKey, table: table})
	}
	if err := sortAndValidateTableMappings(tables); err != nil {
		return err
	}
	for _, entry := range tables {
		if err := entry.table.validateSchemaDefinition(); err != nil {
			return err
		}
	}
	return nil
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
	tables, err := p.registeredTablesForSQLGeneration()
	if err != nil {
		return err
	}

	// 先把所有表校验完，再向 w 写一个字节。否则第 k 张表不支持时，前 k-1 张已经
	// 落进 schema.sql，调用方拿到 error 却留下了一份看似可执行的半套结构。
	for _, entry := range tables {
		if err := entry.table.validateSchemaDefinition(); err != nil {
			return err
		}
	}
	for _, entry := range tables {
		if _, err := fmt.Fprintln(w, entry.table.GetCreateTableSQL()); err != nil {
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
	return dumpSQLFileAtomic(path, p.WriteCreateTableSQL)
}

// GenerateMigrationSQL 生成把线上表结构对齐到 proto 定义所需的 SQL：
//   - 表不存在   → 返回 CREATE TABLE 语句
//   - 表已存在   → 返回 ALTER TABLE（新增字段 / 按字段号改名 / 类型对齐）语句
//   - 无任何差异 → 返回空串
//
// 需要已连库（读 information_schema 比对当前结构），且该消息对应表已 RegisterTable。
// 与 UpdateTableField 的区别：只产出 SQL 不执行，便于生成迁移文件供人工/CI 审核。
// ⚠️ 产出的可能是**两条** ALTER（列+索引 / 补主键），中间用换行分隔。
// 拆两条不是排版，是必须的执行顺序，理由见 schemaPlan 的注释。
//
// 与 DB.SyncAllTables 走**同一个** planSchemaAlignment：原先这里只调 buildAlterClauses，
// 也就是只规划列——索引、唯一键、补主键全都不在产出里。于是"生成迁移脚本交 CI 审核"
// 这条正路产出的 SQL，比进程自己 SyncAllTables 干的事**少一截**：人按脚本迁完，
// 进程一起来还是要自己再补索引和主键，而审核时谁也没看见那些语句。
func (p *DB) GenerateMigrationSQL(m proto.Message) (string, error) {
	tableName := GetTableName(m)
	table, ok := p.Tables[tableName]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrTableNotFound, tableName)
	}
	if _, err := p.registeredTablesForSQLGeneration(); err != nil {
		return "", err
	}
	if err := table.validateSchemaDefinition(); err != nil {
		return "", err
	}

	exists, err := p.IsTableExists(table.tableName)
	if err != nil {
		return "", fmt.Errorf("check table %s exists: %w", table.tableName, err)
	}
	if !exists {
		return table.GetCreateTableSQL(), nil
	}

	currentCols, indexes, indexesKnown, currentPK, err := p.readSchemaState(tableName, table)
	if err != nil {
		return "", err
	}
	plan, err := table.planSchemaAlignment(currentCols, indexes, indexesKnown, currentPK, p.ExpandOnly)
	if err != nil {
		return "", err
	}
	stmts := plan.statements(table.tableName)
	if len(stmts) == 0 {
		return "", nil
	}
	return strings.Join(stmts, ";\n") + ";", nil
}

// WriteMigrationSQL 依次为每个消息生成迁移 SQL 并写入 w（无差异的表自动跳过），需连库。
// 常用于生成一份 migrate.sql：把当前库结构对齐到最新 proto 定义。
func (p *DB) WriteMigrationSQL(w io.Writer, messages ...proto.Message) error {
	// 完整预检必须先于 GenerateMigrationSQL：后者第一步就会查 information_schema。
	// 若等遍历到第二个 message 才发现它和第一个映射同表，DB I/O 已发生，无法再称为
	// fail-closed 的离线生成。
	if err := p.validateMigrationMessageMappings(messages); err != nil {
		return err
	}
	statements := make([]string, 0, len(messages))
	for _, m := range messages {
		stmt, err := p.GenerateMigrationSQL(m)
		if err != nil {
			return err
		}
		if stmt == "" {
			continue
		}
		statements = append(statements, stmt)
	}

	// 只有全部消息都完成校验与 SQL 生成后才碰 writer。否则第 k 张表失败时，
	// 前 k-1 张已经写出，调用方拿到 error 却得到一份可被误执行的半套迁移。
	for _, stmt := range statements {
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
	return dumpSQLFileAtomic(path, func(w io.Writer) error {
		return p.WriteMigrationSQL(w, messages...)
	})
}

// dumpSQLFileAtomic 先在内存中完成校验和完整生成，再写同目录临时文件并 rename。
// 生成失败或临时文件写失败时，已有目标保持原字节且不会留下半套 SQL。
func dumpSQLFileAtomic(path string, generate func(io.Writer) error) error {
	var content bytes.Buffer
	if err := generate(&content); err != nil {
		return err
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return fmt.Errorf("create temporary SQL file for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(content.Bytes()); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary SQL file for %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temporary SQL file for %s: %w", path, err)
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return fmt.Errorf("chmod temporary SQL file for %s: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace SQL file %s: %w", path, err)
	}
	return nil
}
