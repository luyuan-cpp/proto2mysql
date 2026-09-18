package proto2mysql

import (
	"context"
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
//
// 旧表刻意带上两样影子表最容易弄丢的东西：本库未声明的唯一索引与普通索引（RENAME 之后没有第二次
// 机会），以及线上比 proto 更宽的列（bigint 存着超出 int32 的值，按 proto 类型重建会截断或失败）。
func TestLegacyPrimaryKeyMigrationRealDatabase(t *testing.T) {
	md := keyProbeDescriptor(t, "legacy_pk_it_probe",
		keyProbeField{name: "sub", typ: probeString},
		keyProbeField{name: "note", typ: probeString},
		keyProbeField{name: "count", typ: probeInt32},
		// nick 线上是 varchar(64)、索引是整列（SUB_PART 为 NULL），而它不在任何键里，
		// proto 映射成 MEDIUMTEXT——影子表这一列会被拓宽，索引照抄成裸列名的话建表报 Error 1170。
		keyProbeField{name: "nick", typ: probeString},
	)
	const table = "p2m_legacy_pk_probe"
	const wideValue int64 = 9999999999 // 超出 int32
	shadow, backup := table+"__p2m_new", table+"__p2m_old"
	msg := dynamicpb.NewMessage(md)
	pdb, db := openKeyProbeDB(t, msg, WithTableName(table), WithPrimaryKey("sub"))
	dropKeyProbeTables(t, db, table, shadow, backup)
	t.Cleanup(func() { dropKeyProbeTables(t, db, table, shadow, backup) })

	if _, err := db.Exec("CREATE TABLE `" + table + "` (" +
		"`sub` varchar(191) NOT NULL COMMENT 'pb:1', `note` MEDIUMTEXT COMMENT 'pb:2', " +
		"`count` bigint NOT NULL DEFAULT 0 COMMENT 'pb:3', `nick` varchar(64) NOT NULL DEFAULT '' COMMENT 'pb:4', " +
		"PRIMARY KEY (`sub`), " +
		"UNIQUE KEY `uk_manual_note` (`note`(191)), KEY `idx_manual_count` (`count`), " +
		"KEY `idx_manual_nick` (`nick`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"); err != nil {
		t.Fatalf("建旧形态表: %v", err)
	}
	// wide 行的 count 超出 int32，只能用裸 SQL 读回（本库解析 int32 字段时会拒绝它，这正是"线上更宽"的含义）
	if _, err := db.Exec("INSERT INTO `"+table+"` (`sub`, `note`, `count`, `nick`) "+
		"VALUES ('AbC', 'x', 7, 'n1'), ('wide', 'w', ?, 'n2')", wideValue); err != nil {
		t.Fatalf("写旧数据: %v", err)
	}
	if _, err := db.Exec("INSERT INTO `" + table + "` (`sub`, `note`) VALUES ('abc', 'y')"); err == nil {
		t.Fatal("旧形态主键不区分大小写，'abc' 应与 'AbC' 撞主键——前提不成立，本用例失去意义")
	}

	migrateLegacyKeyTable(t, pdb, db, msg, table)
	assertKeyColumnShape(t, db, table, "sub", "varchar(191)", KeyStringCollation, "PRIMARY")

	// 本库未声明的索引必须原样还在：定义（唯一性、列、前缀长度）也要对得上
	for index, want := range map[string]string{
		"uk_manual_note":   "UNIQUE(1:note(191))",
		"idx_manual_count": "INDEX(1:count)",
		// 线上是整列索引，影子表这一列被拓宽成 MEDIUMTEXT，所以必须补出 191 前缀才建得出表
		"idx_manual_nick": "INDEX(1:nick(191))",
	} {
		if got := keyProbeIndexDefinition(t, db, table, index); got != want {
			t.Errorf("迁移后索引 %s = %q, want %q", index, got, want)
		}
	}
	// 线上更宽的列不得被收窄：超出 int32 的值原样保留
	var count int64
	if err := db.QueryRow("SELECT `count` FROM `"+table+"` WHERE `sub` = ?", "wide").Scan(&count); err != nil {
		t.Fatalf("读回 count: %v", err)
	}
	if count != wideValue {
		t.Fatalf("线上 bigint 列被收窄了: count = %d, want %d", count, wideValue)
	}

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

// TestLegacyPrimaryKeyAutoIncrementMigrationRealDatabase 影子表必须继承旧表的自增计数器：
// 旧表删过尾部行时（计数器远高于 MAX(id)），不抬计数器会把删掉的 id 重新发一遍。
func TestLegacyPrimaryKeyAutoIncrementMigrationRealDatabase(t *testing.T) {
	md := keyProbeDescriptor(t, "legacy_pk_autoinc_it_probe",
		keyProbeField{name: "sub", typ: probeString},
		keyProbeField{name: "id", typ: probeUint64},
	)
	const table = "p2m_legacy_pk_autoinc_probe"
	shadow, backup := table+"__p2m_new", table+"__p2m_old"
	msg := dynamicpb.NewMessage(md)
	pdb, db := openKeyProbeDB(t, msg, WithTableName(table), WithPrimaryKey("sub"),
		WithIndexes("id"), WithAutoIncrementKey("id"))
	dropKeyProbeTables(t, db, table, shadow, backup)
	t.Cleanup(func() { dropKeyProbeTables(t, db, table, shadow, backup) })

	if _, err := db.Exec("CREATE TABLE `" + table + "` (" +
		"`sub` varchar(191) NOT NULL COMMENT 'pb:1', " +
		"`id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'pb:2', " +
		"PRIMARY KEY (`sub`), KEY `idx_" + table + "_0` (`id`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"); err != nil {
		t.Fatalf("建旧形态表: %v", err)
	}
	if _, err := db.Exec("INSERT INTO `" + table + "` (`sub`) VALUES ('AbC'), ('def')"); err != nil {
		t.Fatalf("写旧数据: %v", err)
	}
	// 先读一次 information_schema.TABLES.AUTO_INCREMENT，把服务端的统计缓存填上小值。
	//
	// 少了这一步，这个用例对迁移块里那条 `SET SESSION information_schema_stats_expiry = 0`
	// 是**看不见的**：MySQL 8 的缓存在首次读取时才填充，从没读过就天然新鲜，把那条语句删掉
	// 测试照样绿。真实场景里 DBA 迁移前多半已经看过表状态（缓存默认存 24 小时），
	// 那时读回的就是陈旧计数器，影子表水位会被定在远低于旧表的值上、RENAME 之后重发已用过的 id。
	var cachedAutoIncrement sql.NullInt64
	if err := db.QueryRow("SELECT AUTO_INCREMENT FROM information_schema.TABLES "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table).Scan(&cachedAutoIncrement); err != nil {
		t.Fatalf("预读旧表计数器（填充统计缓存）: %v", err)
	}
	// 把计数器抬高后再插一行、删掉它：这就是"尾部行被删过"的表，MAX(id) 远低于计数器
	if _, err := db.Exec("ALTER TABLE `" + table + "` AUTO_INCREMENT = 100001"); err != nil {
		t.Fatalf("抬高旧表计数器: %v", err)
	}
	if _, err := db.Exec("INSERT INTO `" + table + "` (`sub`) VALUES ('tail')"); err != nil {
		t.Fatalf("写尾部行: %v", err)
	}
	var tailID uint64
	if err := db.QueryRow("SELECT `id` FROM `"+table+"` WHERE `sub` = ?", "tail").Scan(&tailID); err != nil {
		t.Fatalf("读尾部行 id: %v", err)
	}
	if tailID < 100001 {
		t.Fatalf("抬高计数器没生效：尾部行 id = %d，前提不成立", tailID)
	}
	if _, err := db.Exec("DELETE FROM `"+table+"` WHERE `sub` = ?", "tail"); err != nil {
		t.Fatalf("删尾部行: %v", err)
	}

	migrateLegacyKeyTable(t, pdb, db, msg, table)
	assertKeyColumnShape(t, db, table, "sub", "varchar(191)", KeyStringCollation, "PRIMARY")

	row := dynamicpb.NewMessage(md)
	row.Set(md.Fields().ByName("sub"), protoreflect.ValueOfString("new"))
	if err := pdb.Insert(row); err != nil {
		t.Fatalf("迁移后插入新行: %v", err)
	}
	var newID uint64
	if err := db.QueryRow("SELECT `id` FROM `"+table+"` WHERE `sub` = ?", "new").Scan(&newID); err != nil {
		t.Fatalf("读回新行 id: %v", err)
	}
	if newID <= tailID {
		t.Fatalf("影子表没继承计数器：新行 id = %d，删掉的尾部行 id = %d，被重新发了一遍", newID, tailID)
	}
}

// TestLegacyRenamedKeyColumnMigrationRealDatabase 线上列名与 proto 字段名不同（按 COMMENT 'pb:N'
// 的字段号匹配）时的唯一键迁移：CHANGE COLUMN 改名与类型必须在同一步，NULL 也要先改成空串。
func TestLegacyRenamedKeyColumnMigrationRealDatabase(t *testing.T) {
	md := keyProbeDescriptor(t, "legacy_rename_it_probe",
		keyProbeField{name: "id", typ: probeUint64},
		keyProbeField{name: "provider_id", typ: probeString},
	)
	const table = "p2m_legacy_rename_probe"
	msg := dynamicpb.NewMessage(md)
	pdb, db := openKeyProbeDB(t, msg, WithTableName(table), WithPrimaryKey("id"), WithUniqueKey("provider_id"))
	dropKeyProbeTables(t, db, table)
	t.Cleanup(func() { dropKeyProbeTables(t, db, table) })

	// 线上列叫 pid，只有 COMMENT 'pb:2' 能证明它就是 provider_id
	if _, err := db.Exec("CREATE TABLE `" + table + "` (\n" +
		"  `id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',\n" +
		"  `pid` MEDIUMTEXT COMMENT 'pb:2',\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  UNIQUE KEY `uk_" + table + "` (`pid`(191))\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='" + table + "'"); err != nil {
		t.Fatalf("建旧形态表: %v", err)
	}
	if _, err := db.Exec("INSERT INTO `" + table + "` (`id`, `pid`) VALUES (1, 'AbC'), (2, NULL)"); err != nil {
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
		t.Fatalf("改名迁移后数据不对: id=1 %q, id=2 %q", first, second)
	}
	if _, err := db.Exec("SELECT `pid` FROM `" + table + "` LIMIT 1"); err == nil {
		t.Fatal("旧列名 pid 应已随 CHANGE COLUMN 改掉")
	}
}

// TestLegacyPrimaryKeyNullableColumnMigrationRealDatabase 线上可空、按 proto 在影子表里是 NOT NULL 的
// 非键列：本库不替它改写数据，但必须点名并给出核对查询——否则整表拷贝会在第一行 NULL 上报 Error 1048，
// 前面建表那几步全白做。人工回填之后，同一块 SQL 必须能一路执行到底。
func TestLegacyPrimaryKeyNullableColumnMigrationRealDatabase(t *testing.T) {
	md := keyProbeDescriptor(t, "legacy_pk_nullable_it_probe",
		keyProbeField{name: "sub", typ: probeString},
		keyProbeField{name: "count", typ: probeInt32}, // proto 目标是 int NOT NULL DEFAULT 0
	)
	const table = "p2m_legacy_pk_nullable_probe"
	shadow, backup := table+"__p2m_new", table+"__p2m_old"
	msg := dynamicpb.NewMessage(md)
	pdb, db := openKeyProbeDB(t, msg, WithTableName(table), WithPrimaryKey("sub"))
	dropKeyProbeTables(t, db, table, shadow, backup)
	t.Cleanup(func() { dropKeyProbeTables(t, db, table, shadow, backup) })

	if _, err := db.Exec("CREATE TABLE `" + table + "` (" +
		"`sub` varchar(191) NOT NULL COMMENT 'pb:1', `count` int DEFAULT NULL COMMENT 'pb:2', " +
		"PRIMARY KEY (`sub`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"); err != nil {
		t.Fatalf("建旧形态表: %v", err)
	}
	if _, err := db.Exec("INSERT INTO `" + table + "` (`sub`, `count`) VALUES ('AbC', 7), ('null_row', NULL)"); err != nil {
		t.Fatalf("写旧数据: %v", err)
	}

	syncErr := pdb.SyncAllTables()
	if !errors.Is(syncErr, ErrLegacyKeyColumn) {
		t.Fatalf("旧形态主键必须以 ErrLegacyKeyColumn 拒绝，实际: %v", syncErr)
	}
	if !strings.Contains(syncErr.Error(), "`count`") || !strings.Contains(syncErr.Error(), "Error 1048") {
		t.Fatalf("错误信息必须点名可空转 NOT NULL 的列与 1048 后果，实际:\n%v", syncErr)
	}

	// 核对查询要能查出这一列有几行 NULL：这正是人工决定怎么回填的依据
	var nullRows, rowsTotal int64
	found := false
	for _, query := range legacyChecks(t, syncErr) {
		if !strings.Contains(query, "count_null_rows") {
			continue
		}
		if err := db.QueryRow(query).Scan(&nullRows, &rowsTotal); err != nil {
			t.Fatalf("执行 NOT NULL 缺口核对查询: %v\nSQL: %s", err, query)
		}
		found = true
	}
	if !found {
		t.Fatalf("核对查询里必须有一条统计这些列 NULL 行数的，实际:\n%v", legacyChecks(t, syncErr))
	}
	if nullRows != 1 || rowsTotal != 2 {
		t.Fatalf("核对查询结果不对: null_rows=%d rows_total=%d, want 1 / 2", nullRows, rowsTotal)
	}

	// 不回填就照着执行：拷贝那一步必须失败（这就是"点名"要防的事），且影子表还没接客，丢弃安全
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("取专用连接: %v", err)
	}
	defer func() { _ = conn.Close() }()
	var copyErr error
	for _, stmt := range legacyStatements(t, syncErr) {
		if _, copyErr = conn.ExecContext(context.Background(), stmt); copyErr != nil {
			break
		}
	}
	if copyErr == nil {
		t.Fatal("没回填就迁移，INSERT ... SELECT 应当因 NOT NULL 而失败")
	}
	if _, err := conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+escapeMySQLName(shadow)); err != nil {
		t.Fatalf("丢弃未接客的影子表: %v", err)
	}

	// 按提示回填，再整块执行一遍：这次必须一路到底，且零漂移
	if _, err := db.Exec("UPDATE `" + table + "` SET `count` = 0 WHERE `count` IS NULL"); err != nil {
		t.Fatalf("人工回填: %v", err)
	}
	migrateLegacyKeyTable(t, pdb, db, msg, table)
	assertKeyColumnShape(t, db, table, "sub", "varchar(191)", KeyStringCollation, "PRIMARY")
	if n := countKeyProbeRows(t, db, table); n != 2 {
		t.Fatalf("迁移后行数 = %d, want 2", n)
	}
}

// TestLegacyPrimaryKeyFulltextIndexMigrationRealDatabase FULLTEXT 索引不能当普通索引重建：
// 本库只会按唯一性生成 INDEX / UNIQUE KEY，重建出来是 BTREE，RENAME 之后 MATCH ... AGAINST
// 报 Error 1191，而索引名已被占用、人工重建还要先改名。所以它必须被逐条点名、不进影子表。
func TestLegacyPrimaryKeyFulltextIndexMigrationRealDatabase(t *testing.T) {
	md := keyProbeDescriptor(t, "legacy_pk_fulltext_it_probe",
		keyProbeField{name: "sub", typ: probeString},
		keyProbeField{name: "note", typ: probeString},
	)
	const table = "p2m_legacy_pk_fulltext_probe"
	shadow, backup := table+"__p2m_new", table+"__p2m_old"
	msg := dynamicpb.NewMessage(md)
	pdb, db := openKeyProbeDB(t, msg, WithTableName(table), WithPrimaryKey("sub"))
	dropKeyProbeTables(t, db, table, shadow, backup)
	t.Cleanup(func() { dropKeyProbeTables(t, db, table, shadow, backup) })

	if _, err := db.Exec("CREATE TABLE `" + table + "` (" +
		"`sub` varchar(191) NOT NULL COMMENT 'pb:1', `note` MEDIUMTEXT COMMENT 'pb:2', " +
		"PRIMARY KEY (`sub`), FULLTEXT KEY `ft_manual_note` (`note`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"); err != nil {
		t.Fatalf("建旧形态表: %v", err)
	}
	// TiDB 会静默忽略 FULLTEXT（建表成功但 information_schema 里没有这条索引），此时本用例没有意义
	var fulltextCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
		table, "ft_manual_note").Scan(&fulltextCount); err != nil {
		t.Fatalf("回读 FULLTEXT 索引: %v", err)
	}
	if fulltextCount == 0 {
		t.Skip("这个后端不真正创建 FULLTEXT 索引（TiDB 会静默忽略），跳过")
	}
	if _, err := db.Exec("INSERT INTO `" + table + "` (`sub`, `note`) VALUES ('AbC', 'hello world')"); err != nil {
		t.Fatalf("写旧数据: %v", err)
	}

	syncErr := pdb.SyncAllTables()
	if !errors.Is(syncErr, ErrLegacyKeyColumn) {
		t.Fatalf("旧形态主键必须以 ErrLegacyKeyColumn 拒绝，实际: %v", syncErr)
	}
	if !strings.Contains(syncErr.Error(), "ft_manual_note") || !strings.Contains(syncErr.Error(), "FULLTEXT") {
		t.Fatalf("错误信息必须点名这条不还原的 FULLTEXT 索引，实际:\n%v", syncErr)
	}
	for _, stmt := range legacyStatements(t, syncErr) {
		if strings.Contains(stmt, "CREATE TABLE") && strings.Contains(stmt, "ft_manual_note") {
			t.Fatalf("FULLTEXT 索引不得进影子表建表语句:\n%s", stmt)
		}
	}

	migrateLegacyKeyTable(t, pdb, db, msg, table)
	assertKeyColumnShape(t, db, table, "sub", "varchar(191)", KeyStringCollation, "PRIMARY")
	// 迁移后这条索引确实没了（这正是要人工重建的东西），数据仍在
	if err := db.QueryRow("SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?",
		table, "ft_manual_note").Scan(&fulltextCount); err != nil {
		t.Fatalf("迁移后回读索引: %v", err)
	}
	if fulltextCount != 0 {
		t.Fatalf("FULLTEXT 索引不应被重建（哪怕降级成 BTREE），实际还在")
	}
	if n := countKeyProbeRows(t, db, table); n != 1 {
		t.Fatalf("迁移后行数 = %d, want 1", n)
	}
}

// migrateLegacyKeyTable 同步必须以 ErrLegacyKeyColumn 拒绝且不动表结构；随后先跑一遍人工核对查询
// （只验证它们在本后端上是合法 SQL，结果由人判断），再逐条执行迁移 SQL，最后同步必须零漂移。
//
// 迁移 SQL 必须钉在**同一条连接**上执行：块里有 SET SESSION、用户变量与 PREPARE，
// 而 *sql.DB 是连接池，逐条 db.Exec 可能落在不同连接上，严格模式与计数器那几步就白做了。
func migrateLegacyKeyTable(t *testing.T, pdb *DB, db *sql.DB, msg proto.Message, table string) {
	t.Helper()
	before := keyProbeSchema(t, db, table)
	syncErr := pdb.SyncAllTables()
	if !errors.Is(syncErr, ErrSchemaDrift) || !errors.Is(syncErr, ErrLegacyKeyColumn) {
		t.Fatalf("旧形态键列必须以 ErrSchemaDrift + ErrLegacyKeyColumn 拒绝，实际: %v", syncErr)
	}
	if after := keyProbeSchema(t, db, table); after != before {
		t.Fatalf("拒绝同步时不得改动表结构\n--- before ---\n%s--- after ---\n%s", before, after)
	}
	if _, genErr := pdb.GenerateMigrationSQL(msg); !errors.Is(genErr, ErrLegacyKeyColumn) {
		t.Fatalf("GenerateMigrationSQL 与同步走同一套规划，也必须拒绝，实际: %v", genErr)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("取迁移用的专用连接: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, query := range legacyChecks(t, syncErr) {
		rows, queryErr := conn.QueryContext(context.Background(), query)
		if queryErr != nil {
			t.Fatalf("执行错误信息里的核对查询失败: %v\nSQL: %s", queryErr, query)
		}
		_ = rows.Close()
	}

	statements := legacyStatements(t, syncErr)
	t.Logf("执行错误信息里的迁移 SQL:\n%s;", strings.Join(statements, ";\n"))
	for _, stmt := range statements {
		if _, execErr := conn.ExecContext(context.Background(), stmt); execErr != nil {
			t.Fatalf("执行错误信息里的迁移 SQL 失败: %v\nSQL: %s\n完整错误: %v", execErr, stmt, syncErr)
		}
	}
	assertSyncsWithoutDrift(t, pdb, db, msg, table)
}

// keyProbeIndexDefinition 从 information_schema 回读一条索引的定义（唯一性 + 按序列号排好的列与前缀长度）。
func keyProbeIndexDefinition(t *testing.T, db *sql.DB, table, index string) string {
	t.Helper()
	rows, err := db.Query("SELECT NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME, SUB_PART FROM INFORMATION_SCHEMA.STATISTICS "+
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ? ORDER BY SEQ_IN_INDEX", table, index)
	if err != nil {
		t.Fatalf("读取索引 %s.%s: %v", table, index, err)
	}
	defer rows.Close()

	unique := ""
	var parts []string
	for rows.Next() {
		var nonUnique, sequence int
		var column string
		var subPart sql.NullInt64
		if err := rows.Scan(&nonUnique, &sequence, &column, &subPart); err != nil {
			t.Fatalf("扫描索引 %s.%s: %v", table, index, err)
		}
		unique = "INDEX"
		if nonUnique == 0 {
			unique = "UNIQUE"
		}
		if subPart.Valid {
			column = fmt.Sprintf("%s(%d)", column, subPart.Int64)
		}
		parts = append(parts, fmt.Sprintf("%d:%s", sequence, column))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历索引 %s.%s: %v", table, index, err)
	}
	if unique == "" {
		return ""
	}
	return fmt.Sprintf("%s(%s)", unique, strings.Join(parts, ","))
}
