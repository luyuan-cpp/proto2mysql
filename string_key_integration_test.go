package proto2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// 字符串键列在真 MySQL / TiDB 上的端到端行为（PROTO2MYSQL_INTEGRATION=1 才跑）。
//
// 单元测试只能证明发出去的 SQL 长什么样；排序规则是否区分大小写与尾部空格、information_schema
// 回读的形态、错误信息里的迁移 SQL 能不能真的执行，只有打到真库上才算数。

func openKeyProbeDB(t *testing.T, msg proto.Message, opts ...TableOption) (*DB, *sql.DB) {
	t.Helper()
	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	t.Cleanup(func() { closeTestDB(t, db) })
	pdb.RegisterTable(msg, opts...)
	return pdb, db
}

func dropKeyProbeTables(t *testing.T, db *sql.DB, tables ...string) {
	t.Helper()
	for _, table := range tables {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + escapeMySQLName(table)); err != nil {
			t.Fatalf("清理表 %s: %v", table, err)
		}
	}
}

// keyProbeSchema 表的列与索引在 information_schema 里的完整快照，用于断言"没有被改动"。
func keyProbeSchema(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	var b strings.Builder
	collect := func(query string) {
		rows, err := db.Query(query, table)
		if err != nil {
			t.Fatalf("读取表 %s 的结构: %v", table, err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			t.Fatalf("读取结果列: %v", err)
		}
		values := make([]sql.NullString, len(columns))
		targets := make([]interface{}, len(columns))
		for i := range values {
			targets[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(targets...); err != nil {
				t.Fatalf("扫描表 %s 的结构: %v", table, err)
			}
			for i, value := range values {
				if i > 0 {
					b.WriteString(" | ")
				}
				if value.Valid {
					b.WriteString(value.String)
				} else {
					b.WriteString("<NULL>")
				}
			}
			b.WriteString("\n")
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("遍历表 %s 的结构: %v", table, err)
		}
	}
	collect("SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLLATION_NAME, COLUMN_COMMENT " +
		"FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION")
	collect("SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART " +
		"FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY INDEX_NAME, SEQ_IN_INDEX")
	return b.String()
}

// assertSyncsWithoutDrift 同步两次：每次都成功，同步后 GenerateMigrationSQL 为空（零 ALTER、零漂移），
// 且第二次同步前后 information_schema 完全一致。
func assertSyncsWithoutDrift(t *testing.T, pdb *DB, db *sql.DB, msg proto.Message, table string) {
	t.Helper()
	if err := pdb.SyncAllTables(); err != nil {
		t.Fatalf("第一次同步: %v", err)
	}
	if stmt, err := pdb.GenerateMigrationSQL(msg); err != nil || stmt != "" {
		t.Fatalf("同步后必须零漂移、零 ALTER，GenerateMigrationSQL=%q err=%v", stmt, err)
	}
	before := keyProbeSchema(t, db, table)
	if err := pdb.SyncAllTables(); err != nil {
		t.Fatalf("第二次同步: %v", err)
	}
	if after := keyProbeSchema(t, db, table); after != before {
		t.Fatalf("第二次同步改动了表结构\n--- before ---\n%s--- after ---\n%s", before, after)
	}
	if stmt, err := pdb.GenerateMigrationSQL(msg); err != nil || stmt != "" {
		t.Fatalf("第二次同步后必须仍零漂移，GenerateMigrationSQL=%q err=%v", stmt, err)
	}
}

// assertKeyColumnShape 键列在 information_schema 里的回读形态：类型、NOT NULL、默认值（空串而非 NULL）、
// 排序规则，以及它所在索引是整列（SUB_PART 为 NULL）。
func assertKeyColumnShape(t *testing.T, db *sql.DB, table, column, wantType, wantCollation, indexName string) {
	t.Helper()
	var colType, isNullable string
	var colDefault, collation sql.NullString
	if err := db.QueryRow("SELECT COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLLATION_NAME FROM INFORMATION_SCHEMA.COLUMNS "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?", table, column).
		Scan(&colType, &isNullable, &colDefault, &collation); err != nil {
		t.Fatalf("读取列 %s.%s: %v", table, column, err)
	}
	if !strings.EqualFold(colType, wantType) || isNullable != "NO" || !colDefault.Valid || colDefault.String != "" {
		t.Fatalf("列 %s.%s 形态 = %s nullable=%s default=%+v，期望 %s NOT NULL DEFAULT ''",
			table, column, colType, isNullable, colDefault, wantType)
	}
	if wantCollation == "" {
		if collation.Valid {
			t.Fatalf("二进制列 %s.%s 不应有排序规则，实际 %q", table, column, collation.String)
		}
	} else if !strings.EqualFold(collation.String, wantCollation) {
		t.Fatalf("列 %s.%s 排序规则 = %q，期望 %s", table, column, collation.String, wantCollation)
	}
	var subPart sql.NullInt64
	if err := db.QueryRow("SELECT SUB_PART FROM INFORMATION_SCHEMA.STATISTICS "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ? AND COLUMN_NAME = ?",
		table, indexName, column).Scan(&subPart); err != nil {
		t.Fatalf("读取索引 %s 的列 %s: %v", indexName, column, err)
	}
	if subPart.Valid {
		t.Fatalf("索引 %s 在键列 %s 上必须是整列，实际前缀 %d", indexName, column, subPart.Int64)
	}
}

func countKeyProbeRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + escapeMySQLName(table)).Scan(&n); err != nil {
		t.Fatalf("统计 %s 行数: %v", table, err)
	}
	return n
}

func TestStringPrimaryKeyRealDatabase(t *testing.T) {
	md := keyProbeDescriptor(t, "string_pk_it_probe",
		keyProbeField{name: "sub", typ: probeString},
		keyProbeField{name: "note", typ: probeString},
	)
	const table = "p2m_string_pk_probe"
	pdb, db := openKeyProbeDB(t, dynamicpb.NewMessage(md),
		WithTableName(table), WithPrimaryKey("sub"), WithMaxLength("sub", 8))
	dropKeyProbeTables(t, db, table)
	t.Cleanup(func() { dropKeyProbeTables(t, db, table) })

	assertSyncsWithoutDrift(t, pdb, db, dynamicpb.NewMessage(md), table)
	assertKeyColumnShape(t, db, table, "sub", "varchar(8)", KeyStringCollation, "PRIMARY")

	row := func(sub, note string) *dynamicpb.Message {
		msg := dynamicpb.NewMessage(md)
		msg.Set(md.Fields().ByName("sub"), protoreflect.ValueOfString(sub))
		msg.Set(md.Fields().ByName("note"), protoreflect.ValueOfString(note))
		return msg
	}
	// 大小写不同、只差尾部空格、以及恰好 8 个字符（含 4 字节字符）的键必须各自成行
	keys := []string{"AbC", "abc", "abc ", "12345678", strings.Repeat("😀", 8)}
	for i, key := range keys {
		if err := pdb.Insert(row(key, fmt.Sprintf("row-%d", i))); err != nil {
			t.Fatalf("插入第 %d 个键: %v", i, err)
		}
	}
	for i, key := range keys {
		got := row(key, "")
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("按主键读回第 %d 个键: %v", i, err)
		}
		if sub := got.Get(md.Fields().ByName("sub")).String(); sub != key {
			t.Fatalf("第 %d 个键读回值不一致: %q", i, sub)
		}
		if note := got.Get(md.Fields().ByName("note")).String(); note != fmt.Sprintf("row-%d", i) {
			t.Fatalf("第 %d 个键命中了别的行: note=%q", i, note)
		}
	}
	if n := countKeyProbeRows(t, db, table); n != len(keys) {
		t.Fatalf("行数 = %d, want %d", n, len(keys))
	}

	// 按主键更新与删除只作用于逐字符相等的那一行
	if err := pdb.Save(row("abc ", "saved")); err != nil {
		t.Fatalf("Save 'abc ': %v", err)
	}
	if err := pdb.Delete(row("AbC", "")); err != nil {
		t.Fatalf("Delete 'AbC': %v", err)
	}
	lower := row("abc", "")
	if err := pdb.FindOneByPK(lower); err != nil {
		t.Fatalf("读回 'abc': %v", err)
	}
	if note := lower.Get(md.Fields().ByName("note")).String(); note != "row-1" {
		t.Fatalf("'abc' 被 'abc ' 的 Save 波及: note=%q", note)
	}
	if err := pdb.FindOneByPK(row("AbC", "")); !errors.Is(err, ErrNoRowsFound) {
		t.Fatalf("'AbC' 应已删除，实际: %v", err)
	}

	// 超过 max_length 一个字符的值在发出 SQL 之前被拒绝，库里什么也不多
	for _, key := range []string{"123456789", strings.Repeat("😀", 9)} {
		if err := pdb.Insert(row(key, "too-long")); !errors.Is(err, ErrInvalidKeyValue) {
			t.Fatalf("超长键应返回 ErrInvalidKeyValue，实际: %v", err)
		}
	}
	if n := countKeyProbeRows(t, db, table); n != len(keys)-1 {
		t.Fatalf("被拒绝的写入不得落库，行数 = %d, want %d", n, len(keys)-1)
	}
}

func TestBytesPrimaryKeyRealDatabase(t *testing.T) {
	md := keyProbeDescriptor(t, "bytes_pk_it_probe",
		keyProbeField{name: "bid", typ: probeBytes},
		keyProbeField{name: "note", typ: probeString},
	)
	const table = "p2m_bytes_pk_probe"
	pdb, db := openKeyProbeDB(t, dynamicpb.NewMessage(md), WithTableName(table), WithPrimaryKey("bid"))
	dropKeyProbeTables(t, db, table)
	t.Cleanup(func() { dropKeyProbeTables(t, db, table) })

	assertSyncsWithoutDrift(t, pdb, db, dynamicpb.NewMessage(md), table)
	assertKeyColumnShape(t, db, table, "bid", "varbinary(191)", "", "PRIMARY")

	row := func(bid []byte, note string) *dynamicpb.Message {
		msg := dynamicpb.NewMessage(md)
		msg.Set(md.Fields().ByName("bid"), protoreflect.ValueOfBytes(bid))
		msg.Set(md.Fields().ByName("note"), protoreflect.ValueOfString(note))
		return msg
	}
	keys := [][]byte{[]byte("a"), []byte("a "), []byte("a\x00"), []byte(strings.Repeat("x", DefaultKeyColumnLength))}
	for i, key := range keys {
		if err := pdb.Insert(row(key, fmt.Sprintf("row-%d", i))); err != nil {
			t.Fatalf("插入第 %d 个键: %v", i, err)
		}
	}
	for i, key := range keys {
		got := row(key, "")
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("按主键读回第 %d 个键: %v", i, err)
		}
		if bid := got.Get(md.Fields().ByName("bid")).Bytes(); string(bid) != string(key) {
			t.Fatalf("第 %d 个键读回字节不一致: %x", i, bid)
		}
		if note := got.Get(md.Fields().ByName("note")).String(); note != fmt.Sprintf("row-%d", i) {
			t.Fatalf("第 %d 个键命中了别的行: note=%q", i, note)
		}
	}
	if err := pdb.Insert(row([]byte(strings.Repeat("x", DefaultKeyColumnLength+1)), "too-long")); !errors.Is(err, ErrInvalidKeyValue) {
		t.Fatalf("超长 bytes 键应返回 ErrInvalidKeyValue，实际: %v", err)
	}
	if n := countKeyProbeRows(t, db, table); n != len(keys) {
		t.Fatalf("行数 = %d, want %d", n, len(keys))
	}
}

// TestLegacyUniqueKeyMigrationRealDatabase 旧版本本库为同一份 proto 建出的形态（MEDIUMTEXT 可空 +
// UNIQUE(col(191))）：同步必须拒绝且不改表；照错误信息里的 SQL 迁移之后，同步必须零漂移。
func TestLegacyUniqueKeyMigrationRealDatabase(t *testing.T) {
	md := keyProbeDescriptor(t, "legacy_uk_it_probe",
		keyProbeField{name: "id", typ: probeUint64},
		keyProbeField{name: "provider_id", typ: probeString},
	)
	const table = "p2m_legacy_uk_probe"
	msg := dynamicpb.NewMessage(md)
	pdb, db := openKeyProbeDB(t, msg, WithTableName(table), WithPrimaryKey("id"), WithUniqueKey("provider_id"))
	dropKeyProbeTables(t, db, table)
	t.Cleanup(func() { dropKeyProbeTables(t, db, table) })

	if _, err := db.Exec("CREATE TABLE `" + table + "` (\n" +
		"  `id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',\n" +
		"  `provider_id` MEDIUMTEXT COMMENT 'pb:2',\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  UNIQUE KEY `uk_" + table + "` (`provider_id`(191))\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='" + table + "'"); err != nil {
		t.Fatalf("建旧形态表: %v", err)
	}
	if _, err := db.Exec("INSERT INTO `" + table + "` (`id`, `provider_id`) VALUES (1, 'AbC'), (2, NULL)"); err != nil {
		t.Fatalf("写旧数据: %v", err)
	}

	migrateLegacyKeyTable(t, pdb, db, msg, table)
	assertKeyColumnShape(t, db, table, "provider_id", "varchar(191)", KeyStringCollation, "uk_"+table)

	var first, second string
	if err := db.QueryRow("SELECT `provider_id` FROM `"+table+"` WHERE `id` = ?", 1).Scan(&first); err != nil {
		t.Fatalf("读回 id=1: %v", err)
	}
	if err := db.QueryRow("SELECT `provider_id` FROM `"+table+"` WHERE `id` = ?", 2).Scan(&second); err != nil {
		t.Fatalf("读回 id=2: %v", err)
	}
	if first != "AbC" || second != "" {
		t.Fatalf("迁移后数据不对: id=1 %q, id=2 %q", first, second)
	}

	// 迁移后唯一键区分大小写：'abc' 与 'AbC' 可以共存
	row := dynamicpb.NewMessage(md)
	row.Set(md.Fields().ByName("id"), protoreflect.ValueOfUint64(3))
	row.Set(md.Fields().ByName("provider_id"), protoreflect.ValueOfString("abc"))
	if err := pdb.Insert(row); err != nil {
		t.Fatalf("迁移后 'abc' 应能与 'AbC' 共存: %v", err)
	}
}

// TestLegacyPrimaryKeyMigrationRealDatabase varchar(191) utf8mb4_unicode_ci 主键（手工或旧工具建出）：
// 同步拒绝且不改表；主键列只能按影子表重建，错误信息里的 SQL 必须在 MySQL 与 TiDB 上都能执行。
func TestLegacyPrimaryKeyMigrationRealDatabase(t *testing.T) {
	md := keyProbeDescriptor(t, "legacy_pk_it_probe",
		keyProbeField{name: "sub", typ: probeString},
		keyProbeField{name: "note", typ: probeString},
	)
	const table = "p2m_legacy_pk_probe"
	shadow, backup := table+"__p2m_new", table+"__p2m_old"
	msg := dynamicpb.NewMessage(md)
	pdb, db := openKeyProbeDB(t, msg, WithTableName(table), WithPrimaryKey("sub"))
	dropKeyProbeTables(t, db, table, shadow, backup)
	t.Cleanup(func() { dropKeyProbeTables(t, db, table, shadow, backup) })

	if _, err := db.Exec("CREATE TABLE `" + table + "` (" +
		"`sub` varchar(191) NOT NULL COMMENT 'pb:1', `note` MEDIUMTEXT COMMENT 'pb:2', PRIMARY KEY (`sub`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"); err != nil {
		t.Fatalf("建旧形态表: %v", err)
	}
	if _, err := db.Exec("INSERT INTO `" + table + "` (`sub`, `note`) VALUES ('AbC', 'x')"); err != nil {
		t.Fatalf("写旧数据: %v", err)
	}
	if _, err := db.Exec("INSERT INTO `" + table + "` (`sub`, `note`) VALUES ('abc', 'y')"); err == nil {
		t.Fatal("旧形态主键不区分大小写，'abc' 应与 'AbC' 撞主键——前提不成立，本用例失去意义")
	}

	migrateLegacyKeyTable(t, pdb, db, msg, table)
	assertKeyColumnShape(t, db, table, "sub", "varchar(191)", KeyStringCollation, "PRIMARY")

	row := func(sub, note string) *dynamicpb.Message {
		m := dynamicpb.NewMessage(md)
		m.Set(md.Fields().ByName("sub"), protoreflect.ValueOfString(sub))
		m.Set(md.Fields().ByName("note"), protoreflect.ValueOfString(note))
		return m
	}
	got := row("AbC", "")
	if err := pdb.FindOneByPK(got); err != nil {
		t.Fatalf("迁移后读回 'AbC': %v", err)
	}
	if note := got.Get(md.Fields().ByName("note")).String(); note != "x" {
		t.Fatalf("迁移后数据不对: note=%q", note)
	}
	if err := pdb.Insert(row("abc", "y")); err != nil {
		t.Fatalf("迁移后 'abc' 应能与 'AbC' 共存: %v", err)
	}
}

// migrateLegacyKeyTable 同步必须以 ErrLegacyKeyColumn 拒绝且不动表结构；随后逐条执行错误信息里的
// 迁移 SQL，再同步必须零漂移。
func migrateLegacyKeyTable(t *testing.T, pdb *DB, db *sql.DB, msg proto.Message, table string) {
	t.Helper()
	before := keyProbeSchema(t, db, table)
	err := pdb.SyncAllTables()
	if !errors.Is(err, ErrSchemaDrift) || !errors.Is(err, ErrLegacyKeyColumn) {
		t.Fatalf("旧形态键列必须以 ErrSchemaDrift + ErrLegacyKeyColumn 拒绝，实际: %v", err)
	}
	if after := keyProbeSchema(t, db, table); after != before {
		t.Fatalf("拒绝同步时不得改动表结构\n--- before ---\n%s--- after ---\n%s", before, after)
	}
	if _, genErr := pdb.GenerateMigrationSQL(msg); !errors.Is(genErr, ErrLegacyKeyColumn) {
		t.Fatalf("GenerateMigrationSQL 与同步走同一套规划，也必须拒绝，实际: %v", genErr)
	}

	statements := legacyStatements(t, err)
	t.Logf("执行错误信息里的迁移 SQL:\n%s;", strings.Join(statements, ";\n"))
	for _, stmt := range statements {
		if _, execErr := db.Exec(stmt); execErr != nil {
			t.Fatalf("执行错误信息里的迁移 SQL 失败: %v\nSQL: %s\n完整错误: %v", execErr, stmt, err)
		}
	}
	assertSyncsWithoutDrift(t, pdb, db, msg, table)
}
