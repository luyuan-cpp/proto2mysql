package proto2mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// 本文件是 2026-08-26 那轮审计确认发现的**回归网**。
//
// 每条测试都对着一个具体的失效模式，注释里写清"修复前它是怎么坏的"——
// 不写的话，下一个人只会看到一堆断言，不知道哪条是能删的、哪条是命根子。

// ---------------------------------------------------------------------------
// P0-1 Save 必须只按主键识别目标行
// ---------------------------------------------------------------------------

// TestSaveODKUGuardsEveryUpdateByPrimaryKey 低层单语句 helper 仍使用 ODKU，
// 但每个非主键赋值都必须带完整主键守卫。否则撞到备用 UNIQUE 时，即使不再改主键，
// 仍会把别人的非键列覆盖掉。
func TestSaveODKUGuardsEveryUpdateByPrimaryKey(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))

	stmt, err := table.GetSaveSQLWithArgs(&testpb.GolangTest{Id: 2, Ip: "10.0.0.1"})
	if err != nil {
		t.Fatalf("GetSaveSQLWithArgs: %v", err)
	}
	if strings.Contains(stmt.Sql, "`id` = VALUES(`id`)") {
		t.Errorf("主键不该出现在 ODKU 的 SET 里: %s", stmt.Sql)
	}
	if !strings.Contains(stmt.Sql, "`ip` = IF(`id` <=> VALUES(`id`), VALUES(`ip`), `ip`)") {
		t.Errorf("非主键列必须受主键身份守卫保护: %s", stmt.Sql)
	}

	batch, err := table.GetBatchSaveSQLWithArgs([]proto.Message{&testpb.GolangTest{Id: 2}})
	if err != nil {
		t.Fatalf("GetBatchSaveSQLWithArgs: %v", err)
	}
	if strings.Contains(batch.Sql, "`id` = VALUES(`id`)") {
		t.Errorf("批量路径同样不许带主键: %s", batch.Sql)
	}
	if !strings.Contains(batch.Sql, "`port` = IF(`id` <=> VALUES(`id`), VALUES(`port`), `port`)") {
		t.Errorf("批量路径同样必须按主键守卫每个赋值: %s", batch.Sql)
	}
}

func TestLegacyInsertOnDupSQLGuardsAlternateUniqueConflicts(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))
	stmt, err := table.GetInsertOnDupUpdateSQLWithArgs(&testpb.GolangTest{Id: 2, Ip: "same"})
	if err != nil {
		t.Fatalf("GetInsertOnDupUpdateSQLWithArgs: %v", err)
	}
	if strings.Contains(stmt.Sql, "`id` = ?") {
		t.Fatalf("低层 upsert 不得改写主键: %s", stmt.Sql)
	}
	if !strings.Contains(stmt.Sql, "`ip` = IF(`id` <=> VALUES(`id`), ?, `ip`)") {
		t.Fatalf("低层 upsert 的非主键更新必须带完整主键守卫: %s", stmt.Sql)
	}

	noop, err := table.GetInsertOnDupKeyForPrimaryKeyWithArgs(&testpb.GolangTest{Id: 2, Ip: "same"})
	if err != nil {
		t.Fatalf("GetInsertOnDupKeyForPrimaryKeyWithArgs: %v", err)
	}
	if !strings.Contains(noop.Sql, "ON DUPLICATE KEY UPDATE `id` = `id`") ||
		strings.Contains(noop.Sql, "ON DUPLICATE KEY UPDATE `id` = ?") {
		t.Fatalf("冲突保持旧行的 helper 不得把入参主键写进另一行: %s", noop.Sql)
	}
}

// TestSaveWithoutPrimaryKeyFailsClosed 没有主键就没有“有则更新”的稳定身份。
// 用备用 UNIQUE 猜目标行会重演改错行，因此必须拒绝并要求调用方显式选 Insert/Update。
func TestSaveWithoutPrimaryKeyFailsClosed(t *testing.T) {
	// GolangTest 的 .proto 里声明了 primary_key/auto_increment_key，得显式清掉才能造出"无主键表"
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey(), WithAutoIncrementKey(""))
	if _, err := table.GetSaveSQLWithArgs(&testpb.GolangTest{Id: 1}); !errors.Is(err, ErrPrimaryKeyNotFound) {
		t.Fatalf("无主键 Save 必须 fail-closed，实际: %v", err)
	}
}

// TestSaveStartsWithPrimaryKeyUpdate DB.Save 的安全语义不能靠 ODKU 猜冲突来源：
// 它必须先按完整主键 UPDATE，只有目标主键不存在时才 INSERT。
func TestSaveStartsWithPrimaryKeyUpdate(t *testing.T) {
	pdb, conn := newFakeDB(t)
	if err := pdb.Save(&testpb.GolangTest{Id: 7, Ip: "new", Port: 3}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	all := conn.sqls()
	if len(all) != 1 || !strings.HasPrefix(all[0], "UPDATE ") {
		t.Fatalf("已存在行应只发一条按主键 UPDATE，实际: %v", all)
	}
	if strings.Contains(all[0], "ON DUPLICATE KEY") {
		t.Fatalf("DB.Save 不得再依赖任意唯一键触发的 ODKU: %s", all[0])
	}
}

// TestSaveODKUAllColumnsArePrimaryKey 整张表只有主键列时，SET 会是空的，
// 而 `ON DUPLICATE KEY UPDATE ` 后面没内容是语法错。必须退化成合法 no-op。
func TestSaveODKUAllColumnsArePrimaryKey(t *testing.T) {
	all := []string{"id", "ip", "port", "group_id", "player", "player_id"}
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey(all...))

	stmt, err := table.GetSaveSQLWithArgs(&testpb.GolangTest{Id: 1})
	if err != nil {
		t.Fatalf("GetSaveSQLWithArgs: %v", err)
	}
	if strings.HasSuffix(stmt.Sql, "ON DUPLICATE KEY UPDATE ") {
		t.Fatalf("不能拼出空的 ODKU（语法错）: %s", stmt.Sql)
	}
	if !strings.Contains(stmt.Sql, "ON DUPLICATE KEY UPDATE `id` = `id`") {
		t.Errorf("应退化成合法 no-op: %s", stmt.Sql)
	}
}

// ---------------------------------------------------------------------------
// P0-3 真实 oneof 必须被挡在建表之前
// ---------------------------------------------------------------------------

// oneofMessage 动态造一个带真实 oneof 的 message：本仓库的 .proto 里一个 oneof 都没有，
// 而这条路径正是"没人用过所以没人发现坏了"。
func oneofMessage(t *testing.T) proto.Message {
	t.Helper()
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("oneof_probe.proto"),
		Package: proto.String("probe"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("OneofProbe"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("id"), Number: proto.Int32(1),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name: proto.String("as_text"), Number: proto.Int32(2),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					// 真实 oneof：OneofIndex 指向下面声明的 payload
					OneofIndex: proto.Int32(0),
				},
				{
					Name: proto.String("as_number"), Number: proto.Int32(3),
					Type:       descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					OneofIndex: proto.Int32(0),
				},
			},
			OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("payload")}},
		}},
	}
	fdesc, err := protodesc.NewFile(fd, nil)
	if err != nil {
		t.Fatalf("构造 oneof 描述符: %v", err)
	}
	return dynamicpb.NewMessage(fdesc.Messages().Get(0))
}

// TestRealOneofRejected 写入时未选中的成员照零值落列、读取时按声明顺序逐个 Set
// 让**最后声明的成员恒胜**——oneof 的真实选择被静默抹掉，而且是双向的：
// 库里那一行看不出错，只是 as_text 永远丢。列模型表达不了"未选中"，只能 fail-closed。
func TestRealOneofRejected(t *testing.T) {
	msg := oneofMessage(t)

	err := ValidateTableMessage(msg, WithTableName("oneof_probe"), WithPrimaryKey("id"))
	if !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("真实 oneof 必须在建表前被拒，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "payload") {
		t.Errorf("错误信息里必须点名是哪个 oneof，便于定位: %v", err)
	}
}

// TestProto3OptionalNotRejected proto3 的 optional 在描述符里也是 oneof（synthetic），
// 但它只是"带 has 位的普通字段"，往返完全正常。误伤它会让一大批正常 proto 建不了表。
func TestProto3OptionalNotRejected(t *testing.T) {
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("optional_probe.proto"),
		Package: proto.String("probe"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("OptionalProbe"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("id"), Number: proto.Int32(1),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name: proto.String("nickname"), Number: proto.Int32(2),
					Type:           descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Label:          descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					Proto3Optional: proto.Bool(true),
					OneofIndex:     proto.Int32(0),
				},
			},
			OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("_nickname")}},
		}},
	}
	fdesc, err := protodesc.NewFile(fd, nil)
	if err != nil {
		t.Fatalf("构造 optional 描述符: %v", err)
	}
	msg := dynamicpb.NewMessage(fdesc.Messages().Get(0))

	if err := ValidateTableMessage(msg, WithTableName("optional_probe"), WithPrimaryKey("id")); err != nil {
		t.Fatalf("proto3 optional（synthetic oneof）不该被拒: %v", err)
	}
}

// ---------------------------------------------------------------------------
// P0-4 永不收窄的两条绕过路径
// ---------------------------------------------------------------------------

// TestCommentBackfillNeverNarrows 线上 bigint、proto 要 int 时 isTypeMatch 判"兼容"，
// 但只要这列还缺 pb:N 注释，就会因为**回填注释**顺带被 MODIFY 成 int，
// bigint 里超出 int 的值当场被吃掉。老表恰恰普遍没有 pb:N（那是后加的机制）。
func TestCommentBackfillNeverNarrows(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"))

	clauses, err := table.buildAlterClauses(map[string]columnMeta{
		"id":        {colType: "int unsigned", fieldNum: 1},
		"ip":        {colType: "mediumtext", fieldNum: 2},
		"port":      {colType: "int unsigned", fieldNum: 3},
		"group_id":  {colType: "int unsigned", fieldNum: 4},
		"player":    {colType: "mediumblob", fieldNum: 5},
		"player_id": {colType: "bigint unsigned"}, // 宽，但**没有** pb:6 注释
	}, false)
	if err != nil {
		t.Fatalf("buildAlterClauses: %v", err)
	}

	joined := strings.Join(clauses, " | ")
	if !strings.Contains(joined, "MODIFY COLUMN `player_id`") {
		t.Fatalf("缺注释的列仍应 MODIFY 回填注释: %s", joined)
	}
	if !strings.Contains(joined, "COMMENT 'pb:6'") {
		t.Errorf("必须把字段号注释补上: %s", joined)
	}
	if !strings.Contains(joined, "MODIFY COLUMN `player_id` bigint unsigned") {
		t.Errorf("回填注释时必须保留线上的宽类型，不能收窄: %s", joined)
	}
}

// TestRenameNeverNarrows isRenameConvertible 只判同族、不判宽窄，
// 于是 bigint 改名成 int 被当成"可改名"，CHANGE COLUMN 连名带类型一起改，
// 数据在隐式转换里被截断。改名要保留，收窄不许发生。
func TestRenameNeverNarrows(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"))

	clauses, err := table.buildAlterClauses(map[string]columnMeta{
		"id":       {colType: "int unsigned", fieldNum: 1},
		"ip":       {colType: "mediumtext", fieldNum: 2},
		"port":     {colType: "int unsigned", fieldNum: 3},
		"group_id": {colType: "int unsigned", fieldNum: 4},
		"player":   {colType: "mediumblob", fieldNum: 5},
		// 旧列名 + 更宽的类型，靠 pb:6 认出这是改名
		"old_player_id": {colType: "bigint unsigned", fieldNum: 6},
	}, false)
	if err != nil {
		t.Fatalf("buildAlterClauses: %v", err)
	}

	joined := strings.Join(clauses, " | ")
	if !strings.Contains(joined, "CHANGE COLUMN `old_player_id` `player_id`") {
		t.Fatalf("应按字段号识别为改名: %s", joined)
	}
	if !strings.Contains(joined, "`player_id` bigint unsigned") {
		t.Errorf("改名时必须保留线上的宽类型: %s", joined)
	}
}

// TestAlignedColumnTypeKeepsTargetAttributes 换类型本体、留目标属性——
// NOT NULL / DEFAULT / AUTO_INCREMENT 是 proto 侧的决定，
// 而 information_schema 的 COLUMN_TYPE 里根本没有它们。
func TestAlignedColumnTypeKeepsTargetAttributes(t *testing.T) {
	cases := []struct {
		current, target, want string
	}{
		{"bigint", "int NOT NULL DEFAULT 0", "bigint NOT NULL DEFAULT 0"},
		{"bigint unsigned", "int unsigned NOT NULL DEFAULT 0", "bigint unsigned NOT NULL DEFAULT 0"},
		{"varchar(100)", "varchar(50) NOT NULL", "varchar(100) NOT NULL"},
		// 目标更宽时原样返回，不许反向拓宽成线上的窄类型
		{"int", "bigint NOT NULL DEFAULT 0", "bigint NOT NULL DEFAULT 0"},
		// 有无符号两边都可能装不下对方，纯 helper 也必须保留线上类型；
		// 真正的迁移规划会返回 ErrUnsafeSchemaConversion。
		{"int", "int unsigned NOT NULL DEFAULT 0", "int NOT NULL DEFAULT 0"},
		// 键列收窄被挡下时同样只换类型本体：字符集、COLLATE、NOT NULL DEFAULT '' 都是键列形态的一部分，
		// 丢了 COLLATE 会退回表默认的 utf8mb4_unicode_ci，唯一性变成大小写不敏感。
		{"varchar(255)",
			"VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''",
			"varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''"},
		{"varbinary(255)", "VARBINARY(191) NOT NULL DEFAULT ''", "varbinary(255) NOT NULL DEFAULT ''"},
		{"varbinary(64)", "VARBINARY(191) NOT NULL DEFAULT ''", "VARBINARY(191) NOT NULL DEFAULT ''"},
	}
	for _, c := range cases {
		if got := alignedColumnType(c.current, c.target); got != c.want {
			t.Errorf("alignedColumnType(%q, %q) = %q, want %q", c.current, c.target, got, c.want)
		}
	}
}

// TestSignednessChangeFailsClosed int 与 int unsigned 的值域互不包含：前者有负数，
// 后者有更大的正数。无论哪个方向自动 ALTER 都可能截断存量数据，必须交人工迁移。
func TestSignednessChangeFailsClosed(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithAutoIncrementKey(""))
	current := map[string]columnMeta{
		"id":        {colType: "int unsigned", fieldNum: 1},
		"ip":        {colType: "mediumtext", fieldNum: 2},
		"port":      {colType: "int", fieldNum: 3}, // proto 目标是 int unsigned
		"group_id":  {colType: "int unsigned", fieldNum: 4},
		"player":    {colType: "mediumblob", fieldNum: 5},
		"player_id": {colType: "bigint unsigned", fieldNum: 6},
	}
	if _, err := table.buildAlterClauses(current, false); !errors.Is(err, ErrUnsafeSchemaConversion) {
		t.Fatalf("signed/unsigned 自动转换必须 fail-closed，实际: %v", err)
	}
}

// ---------------------------------------------------------------------------
// P0-5 ExpandOnly 安全闸
// ---------------------------------------------------------------------------

// TestExpandOnlySurvivesDerivedInstances ExpandOnly 是安全闸不是普通配置：
// 漏拷一次，`db.WithContext(ctx).SyncAllTables()` 就在一个自以为开着闸的进程里
// 照常执行 MODIFY / CHANGE——闸门关着，但不生效，且没有任何提示。
func TestExpandOnlySurvivesDerivedInstances(t *testing.T) {
	pdb := NewDB()
	pdb.ExpandOnly = true

	if !pdb.WithContext(context.Background()).ExpandOnly {
		t.Error("WithContext 必须把 ExpandOnly 带过去")
	}

	pdb2, conn := newFakeDB(t)
	pdb2.ExpandOnly = true
	conn.queueRows(nil) // BEGIN 之类的都由假驱动兜着
	var innerExpandOnly bool
	if err := pdb2.RunInTransaction(func(tx *DB) error {
		innerExpandOnly = tx.ExpandOnly
		return nil
	}); err != nil {
		t.Fatalf("RunInTransaction: %v", err)
	}
	if !innerExpandOnly {
		t.Error("RunInTransaction 的 tx 实例必须把 ExpandOnly 带过去")
	}
}

// TestExpandOnlyCoversPrimaryKeyStatement ExpandOnly 的判定原先写在 buildAlterClauses
// 末尾，只看得见列对齐那一批；而补主键那条 ALTER 是在它之后拼出来的，
// 里头夹带的 MODIFY COLUMN 从来没过过这道闸。
func TestExpandOnlyCoversPrimaryKeyStatement(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{},
		WithPrimaryKey("id"), WithAutoIncrementKey("id"))

	// 线上 id 是 smallint unsigned（同整数族的安全拓宽，必然要 MODIFY），且整张表没有主键。
	// → 计划里会出现 MODIFY COLUMN `id` ... AUTO_INCREMENT，而它会被挪进补主键那条 ALTER。
	//   ExpandOnly 必须看得见它。
	cols := map[string]columnMeta{
		"id":        {colType: "smallint unsigned", fieldNum: 1},
		"ip":        {colType: "mediumtext", fieldNum: 2},
		"port":      {colType: "int unsigned", fieldNum: 3},
		"group_id":  {colType: "int unsigned", fieldNum: 4},
		"player":    {colType: "mediumblob", fieldNum: 5},
		"player_id": {colType: "bigint unsigned", fieldNum: 6},
	}

	if _, err := table.planSchemaAlignment(cols, nil, false, nil, false); err != nil {
		t.Fatalf("ExpandOnly 关闭时不该报错: %v", err)
	}
	_, err := table.planSchemaAlignment(cols, nil, false, nil, true)
	if !errors.Is(err, ErrExpandOnlyViolation) {
		t.Fatalf("ExpandOnly 必须拦下补主键那条里夹带的 MODIFY，实际: %v", err)
	}
}

// ---------------------------------------------------------------------------
// P1-11 补主键时该跟着走的列变更（工作区改动引入的 Error 1075 回归）
// ---------------------------------------------------------------------------

// TestAutoIncrementColumnMovesWithPrimaryKey MySQL 要求自增列必须是键。
// 拆两条 ALTER 时如果只搬 "MODIFY COLUMN <pk>" 前缀的子句，
// 这两种形态会留在第一条里，在主键还不存在时带着 AUTO_INCREMENT 下发 → Error 1075，
// 整条 ALTER 连同全部 ADD COLUMN 一起失败：
//
//	ADD COLUMN `id` ... AUTO_INCREMENT          ← 新建自增主键列
//	CHANGE COLUMN `old` `id` ... AUTO_INCREMENT ← 改名成自增主键列
func TestAutoIncrementColumnMovesWithPrimaryKey(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{},
		WithPrimaryKey("id"), WithAutoIncrementKey("id"))

	t.Run("ADD COLUMN", func(t *testing.T) {
		// 线上完全没有 id 这一列，也没有主键
		plan, err := table.planSchemaAlignment(map[string]columnMeta{
			"ip": {colType: "mediumtext", fieldNum: 2},
		}, nil, false, nil, false)
		if err != nil {
			t.Fatalf("planSchemaAlignment: %v", err)
		}
		assertAutoIncRidesWithPK(t, plan, "ADD COLUMN `id`")
	})

	t.Run("CHANGE COLUMN", func(t *testing.T) {
		// 线上有一列 old_id 带 pb:1 注释 → 会被识别成改名成 id
		plan, err := table.planSchemaAlignment(map[string]columnMeta{
			"old_id": {colType: "int unsigned", fieldNum: 1},
			"ip":     {colType: "mediumtext", fieldNum: 2},
		}, nil, false, nil, false)
		if err != nil {
			t.Fatalf("planSchemaAlignment: %v", err)
		}
		assertAutoIncRidesWithPK(t, plan, "CHANGE COLUMN `old_id` `id`")
	})
}

func assertAutoIncRidesWithPK(t *testing.T, plan schemaPlan, wantPrefix string) {
	t.Helper()

	cols := strings.Join(plan.columns, " | ")
	pk := strings.Join(plan.primaryKey, " | ")

	if strings.Contains(strings.ToUpper(cols), "AUTO_INCREMENT") {
		t.Errorf("带 AUTO_INCREMENT 的子句不能留在第一条 ALTER 里（主键还不存在 → Error 1075）: %s", cols)
	}
	if !strings.Contains(pk, wantPrefix) {
		t.Errorf("自增主键列的 %s 必须挪到补主键那条 ALTER 里: %s", wantPrefix, pk)
	}
	if !strings.Contains(pk, "ADD PRIMARY KEY (`id`)") {
		t.Errorf("补主键语句本身也要在这一条里: %s", pk)
	}
}

// TestNonAutoIncrementPKColumnStaysInFirstAlter 不带 AUTO_INCREMENT 的主键列变更
// **不用挪**：MODIFY 一个普通列不要求它是键，留在第一条反而更好——
// 补主键那条失败时它已经生效了。
func TestNonAutoIncrementPKColumnStaysInFirstAlter(t *testing.T) {
	// 清掉 proto 里声明的 auto_increment_key：这条测的是**不带自增**的主键列
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithAutoIncrementKey(""))

	plan, err := table.planSchemaAlignment(map[string]columnMeta{
		"id": {colType: "int unsigned"}, // 类型对、但缺 pb:1 注释 → MODIFY 回填
		"ip": {colType: "mediumtext", fieldNum: 2},
	}, nil, false, nil, false)
	if err != nil {
		t.Fatalf("planSchemaAlignment: %v", err)
	}
	if !strings.Contains(strings.Join(plan.columns, " | "), "MODIFY COLUMN `id`") {
		t.Errorf("不带自增的主键列变更应留在第一条: %v", plan.columns)
	}
}

// ---------------------------------------------------------------------------
// P1-13 三条迁移路径必须规划同一批语句
// ---------------------------------------------------------------------------

// TestMigrationSQLIncludesIndexesAndPrimaryKey 原先 GenerateMigrationSQL 只调
// buildAlterClauses（只规划列）：人按生成的脚本迁完，进程一起来还要自己再补索引和主键，
// 而审核时谁也没看见那些语句。
func TestMigrationSQLIncludesIndexesAndPrimaryKey(t *testing.T) {
	sqlDB, conn := openFakeDB()
	t.Cleanup(func() { _ = sqlDB.Close() })

	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(&testpb.GolangTest{},
		WithPrimaryKey("id"), WithIndexes("player_id"), WithUniqueKey("ip"))

	conn.queueRows(
		rows(row(int64(1))),              // 表存在
		golangTestAlignedColsWithIPKey(), // 列已对齐（ip 在唯一键里，线上已是 varchar 键列）
		nil,                              // 既有索引：一个都没有
		nil,                              // 主键：没有
	)

	stmt, err := pdb.GenerateMigrationSQL(&testpb.GolangTest{})
	if err != nil {
		t.Fatalf("GenerateMigrationSQL: %v", err)
	}
	if !strings.Contains(stmt, "ADD INDEX `idx_golang_test_0` (`player_id`)") {
		t.Errorf("迁移脚本必须包含补索引: %s", stmt)
	}
	if !strings.Contains(stmt, "ADD UNIQUE KEY `uk_golang_test` (`ip`)") {
		t.Errorf("迁移脚本必须包含补唯一键（键列 VARCHAR 整列，不带前缀）: %s", stmt)
	}
	if !strings.Contains(stmt, "ADD PRIMARY KEY (`id`)") {
		t.Errorf("迁移脚本必须包含补主键: %s", stmt)
	}
}

// ---------------------------------------------------------------------------
// P0-9 数值快捷接口不许对非数值列做算术
// ---------------------------------------------------------------------------

// TestIncrRejectsNonNumericColumn MySQL 对非数值列做算术不报错：
// 先把内容按数值解析（解析不出算 0）再写回，于是 MEDIUMTEXT 的 "abc" + 1 变成 "1"。
// 语句成功、RowsAffected=1、非严格模式下零 Warning。
func TestIncrRejectsNonNumericColumn(t *testing.T) {
	pdb, _ := newFakeDB(t)
	msg := &testpb.GolangTest{Id: 1}

	if err := pdb.IncrByPK(msg, "ip", 1); !errors.Is(err, ErrFieldNotFound) {
		t.Errorf("IncrByPK 对 MEDIUMTEXT 列必须拒绝，实际: %v", err)
	}
	if _, err := pdb.DecrByPKIfEnough(msg, "ip", 1); !errors.Is(err, ErrFieldNotFound) {
		t.Errorf("DecrByPKIfEnough 对 MEDIUMTEXT 列必须拒绝，实际: %v", err)
	}
	if err := pdb.IncrByPK(msg, "player", 1); !errors.Is(err, ErrFieldNotFound) {
		t.Errorf("IncrByPK 对子消息（MEDIUMBLOB）列必须拒绝，实际: %v", err)
	}

	// 数值列照常放行，别把正常用法误伤了
	if err := pdb.IncrByPK(msg, "port", 1); err != nil {
		t.Errorf("数值列不该被拒: %v", err)
	}
}

// TestSQLBuilderArithmeticRejectsNonNumericColumn SQLBuilder 那条路同样要挡。
// UpsertAdd 早就在做这个把关，AddCol/SubCol 这批一直漏着。
func TestSQLBuilderArithmeticRejectsNonNumericColumn(t *testing.T) {
	b := NewSQLBuilder(&testpb.GolangTest{}, WithPrimaryKey("id"))
	msg := &testpb.GolangTest{Id: 1}

	if _, err := b.IncrByPK(msg, "ip", 1); !errors.Is(err, ErrFieldNotFound) {
		t.Errorf("SQLBuilder.IncrByPK 必须拒绝非数值列，实际: %v", err)
	}
	if _, err := b.DecrByPKIfEnough(msg, "ip", 1); !errors.Is(err, ErrFieldNotFound) {
		t.Errorf("SQLBuilder.DecrByPKIfEnough 必须拒绝非数值列，实际: %v", err)
	}
	// SetCol 是普通赋值，不是算术，不该被这条规则误伤
	if _, err := b.UpdateAssignsByPK(msg, SetCol("ip", "x")); err != nil {
		t.Errorf("普通赋值不该被数值校验拦下: %v", err)
	}
	if _, err := b.UpsertWith(msg, SetNewIfZero("ip")); !errors.Is(err, ErrFieldNotFound) {
		t.Errorf("SetNewIfZero 必须拒绝 string 列，实际: %v", err)
	}

	listBuilder := NewSQLBuilder(&testpb.GolangTestList{})
	if _, err := listBuilder.UpsertWith(&testpb.GolangTestList{}, SetNewIfZero("test_list")); !errors.Is(err, ErrFieldNotFound) {
		t.Errorf("SetNewIfZero 必须拒绝 repeated/list 列，实际: %v", err)
	}
}

// ---------------------------------------------------------------------------
// P0-7 复合主键下的批量接口
// ---------------------------------------------------------------------------

// TestFindAllByPKInRejectsCompositeKey primaryKeyField 只是 primaryKey[0]，
// 于是 `WHERE pk0 IN (...)` 只按第一个分量过滤：调用方以为按主键取 N 行，
// 实际拿回"第一分量等于这些值的**全部**行"，而且没有任何迹象表明范围被放大了。
// 这个接口的签名（[]interface{}）本身就表达不了复合主键，只能 fail-closed。
func TestFindAllByPKInRejectsCompositeKey(t *testing.T) {
	pdb, _ := newFakeDB(t)
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id", "port"))

	var list testpb.GolangTestList
	err := pdb.FindAllByPKIn(&list, []interface{}{1, 2})
	if !errors.Is(err, ErrPrimaryKeyNotFound) {
		t.Fatalf("复合主键必须 fail-closed，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "FindMultiByWhereWithArgs") {
		t.Errorf("错误信息要给出可照做的替代写法: %v", err)
	}
}

// ---------------------------------------------------------------------------
// P0-6 缓存 key 的分隔符必须转义
// ---------------------------------------------------------------------------

// TestCacheKeyNoCollisionOnComposite ("x:y","z") 与 ("x","y:z") 原先拼出同一个 key，
// 命中时返回的是**另一行的整条 protobuf**，而且两边互相投毒。
// 库里数据自始至终是对的，零日志零异常——只能靠对账发现。
func TestCacheKeyNoCollisionOnComposite(t *testing.T) {
	if a, b := escapeCacheKeyPart("x:y")+":"+escapeCacheKeyPart("z"),
		escapeCacheKeyPart("x")+":"+escapeCacheKeyPart("y:z"); a == b {
		t.Fatalf("两个不同的复合主键塌成了同一个 key: %s", a)
	}
}

// TestCacheKeyByteCompatibleForOrdinaryValues 转义只动 '%' 和 ':'，
// 不含这两个字符的主键（整数、绝大多数字符串）产出的 key 必须与旧版**逐字节相同**——
// 否则升级会让存量缓存整体失效，也会和共用同一个 Redis 的 Python 版分叉。
func TestCacheKeyByteCompatibleForOrdinaryValues(t *testing.T) {
	for _, s := range []string{"", "123", "player_42", "a-b_c.d", "中文昵称"} {
		if got := escapeCacheKeyPart(s); got != s {
			t.Errorf("普通值不该被改写: %q → %q", s, got)
		}
	}
	if got := escapeCacheKeyPart("a:b%c"); got != "a%3Ab%25c" {
		t.Errorf("escapeCacheKeyPart(\"a:b%%c\") = %q", got)
	}
}

// ---------------------------------------------------------------------------
// P1-16a 事务旁路
// ---------------------------------------------------------------------------

// TestFindMultiByWhereClausesUsesTransaction 全库唯一一处写死 p.DB 的数据路径：
// 事务内调用会读到事务外的快照——"先改后查"查不到自己刚写的值，
// 而且这条查询还持着另一条连接，与事务本身互相等锁。
// 判据是"把连接池限到 1 条"：事务开着的时候那一条被事务占着，
// 谁要是绕过事务去池里另借，就只能干等 —— 正是这条发现描述的真实死锁形态。
// 用带超时的 context 把"干等"变成一个可断言的错误。
func TestFindMultiByWhereClausesUsesTransaction(t *testing.T) {
	sqlDB, conn := openFakeDB()
	t.Cleanup(func() { _ = sqlDB.Close() })
	sqlDB.SetMaxOpenConns(1)

	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))

	// BEGIN 不消费结果集槽位（假驱动只在 Exec/Query 时弹），所以这里只排一条查询结果
	conn.queueRows(
		rows(row(int64(1), "ip", int64(2), int64(3), []byte(nil), int64(4))),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := pdb.WithContext(ctx).RunInTransaction(func(tx *DB) error {
		return tx.FindMultiByWhereClauses([]MultiQuery{{
			Message:     &testpb.GolangTest{},
			WhereClause: "`id` = ?",
			WhereArgs:   []interface{}{1},
		}})
	})
	if err != nil {
		t.Fatalf("事务内的多表查询必须落在事务上（绕回连接池会在只有一条连接时干等）: %v", err)
	}
}

// TestOpenDBUsesServerCaseSensitivity 数据库名在 lower_case_table_names=0 时区分大小写。
// EqualFold 会把 DSN 实际连接的 Game 与调用方要求的 game 当成同一个库，随后元数据查询
// 却拿错误名字过滤 INFORMATION_SCHEMA，形成“CRUD 在一库、同步看另一库”的分叉。
func TestOpenDBUsesServerCaseSensitivity(t *testing.T) {
	t.Run("case-sensitive rejects mismatch", func(t *testing.T) {
		sqlDB, conn := openFakeDB()
		t.Cleanup(func() { _ = sqlDB.Close() })
		conn.queueRows(rows(row("Game")), rows(row(int64(0))))
		pdb := NewDB()
		if err := pdb.OpenDB(sqlDB, "game"); err == nil {
			t.Fatal("lower_case_table_names=0 时必须拒绝大小写不同的库名")
		}
	})

	t.Run("case-insensitive accepts canonical name", func(t *testing.T) {
		sqlDB, conn := openFakeDB()
		t.Cleanup(func() { _ = sqlDB.Close() })
		conn.queueRows(rows(row("Game")), rows(row(int64(1))))
		pdb := NewDB()
		if err := pdb.OpenDB(sqlDB, "game"); err != nil {
			t.Fatalf("lower_case_table_names=1 应接受大小写差异: %v", err)
		}
		if pdb.DBName != "Game" {
			t.Fatalf("后续元数据与锁名应使用服务端返回的规范名，实际 %q", pdb.DBName)
		}
	})
}

// ---------------------------------------------------------------------------
// P2-23 索引名长度
// ---------------------------------------------------------------------------

// TestIndexNameFitsMySQLLimit 表名本身合法（≤64），但 "idx_" + 表名 + "_0"
// 之后就可能不是了——一个 62 字符的合法表名产出 68 字符的索引名，建表直接 Error 1059。
// 截断必须**确定性**：建表分支和补索引分支要叫出同一个名字，否则补索引永远比不中，
// 每次启动都试着再加一遍然后撞 Error 1061。
func TestIndexNameFitsMySQLLimit(t *testing.T) {
	long := strings.Repeat("a", 62)
	table := newMessageTable(&testpb.GolangTest{},
		WithTableName(long), WithIndexes("player_id"), WithUniqueKey("ip"))

	idx := table.indexNameFor(0)
	uk := table.uniqueKeyName()
	for _, name := range []string{idx, uk} {
		if n := len([]rune(name)); n > MySQLMaxIdentifierLength {
			t.Errorf("索引名 %q 有 %d 字符，超过上限 %d", name, n, MySQLMaxIdentifierLength)
		}
	}
	if idx != table.indexNameFor(0) || uk != table.uniqueKeyName() {
		t.Error("截断必须是确定性的：同一个输入永远得到同一个输出")
	}
	// 不同表名不能截出同一个名字，否则第二张表建索引撞 Error 1061
	other := newMessageTable(&testpb.GolangTest{},
		WithTableName(long+"b"), WithIndexes("player_id"))
	if other.indexNameFor(0) == idx {
		t.Error("两个不同的长表名截出了同一个索引名")
	}

	// 建表分支与补索引分支必须用同一个名字
	create := table.GetCreateTableSQL()
	if !strings.Contains(create, "`"+idx+"`") || !strings.Contains(create, "`"+uk+"`") {
		t.Errorf("CREATE TABLE 里的索引名与 indexNameFor/uniqueKeyName 对不上: %s", create)
	}
	missing := table.missingIndexClauses(map[string][]string{})
	joined := strings.Join(missing, " | ")
	if !strings.Contains(joined, "`"+idx+"`") || !strings.Contains(joined, "`"+uk+"`") {
		t.Errorf("补索引分支的名字与建表分支对不上: %s", joined)
	}
}

// ---------------------------------------------------------------------------
// P1-18c 表注释转义
// ---------------------------------------------------------------------------

// TestEscapeMySQLCommentEscapesBackslash 表名不是绑定参数、是直接拼进 DDL 的。
// 只转引号不转反斜杠时，一个以反斜杠结尾的表名就能把字符串提前闭合：
// COMMENT 'evil\' 里的 \' 被 MySQL 读成转义的引号，后面的 SQL 全被吃进字符串。
// 连接默认开着 MultiStatements（FindMultiByWhereClauses 依赖它），缝隙足以追加语句。
func TestEscapeMySQLCommentIsSafeAcrossSQLModes(t *testing.T) {
	cases := map[string]string{
		`evil\`:                        `evil\\`,
		`a'b`:                          `a''b`,
		`a\b`:                          `a\\b`,
		"a\nb":                         "a b",
		`x\' ; DROP TABLE victim; -- `: `x\\'' ; DROP TABLE victim; -- `,
	}
	for in, want := range cases {
		if got := escapeMySQLComment(in); got != want {
			t.Errorf("escapeMySQLComment(%q) = %q, want %q", in, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// P1-14 不支持的字段类型必须挡在建表之前
// ---------------------------------------------------------------------------

// TestUnsupportedKindRejectedBeforeCreate 原先这道校验只在 buildAlterClauses 里，
// 而它跑在 CREATE TABLE 之后：一个没有 MySQL 映射的字段会先按 TEXT 建出一列，
// 然后函数才返回错误。表已经在库里了，事后改回正确类型是跨族 MODIFY，会把数据吃成 0。
func TestUnsupportedKindRejectedBeforeCreate(t *testing.T) {
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("unsupported_probe.proto"),
		Package: proto.String("probe"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("UnsupportedProbe"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("id"), Number: proto.Int32(1),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name: proto.String("zig"), Number: proto.Int32(2),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_SINT64.Enum(), // 无映射
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
			},
		}},
	}
	fdesc, err := protodesc.NewFile(fd, nil)
	if err != nil {
		t.Fatalf("构造描述符: %v", err)
	}
	msg := dynamicpb.NewMessage(fdesc.Messages().Get(0))

	if err := ValidateTableMessage(msg, WithTableName("unsupported_probe")); !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("不支持的字段类型必须在建表前被拒，实际: %v", err)
	}

	sqlDB, conn := openFakeDB()
	t.Cleanup(func() { _ = sqlDB.Close() })
	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(msg, WithTableName("unsupported_probe"))
	conn.queueRows(rows(row(int64(0)))) // 表不存在

	if err := pdb.SyncAllTables(); !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("SyncAllTables 应在建表前就拒绝: %v", err)
	}
	if got := conn.findSQL("CREATE TABLE"); got != "" {
		t.Errorf("报错前不该已经把错误的表建出来: %s", got)
	}
}

// TestOverlongTableNameRejected 没声明 table_name 时表名退化成 proto full name（含 package），
// 很容易超过 MySQL 的 64 字符上限 → Error 1059。这条以前是到了库里才报。
func TestOverlongTableNameRejected(t *testing.T) {
	err := ValidateTableMessage(&testpb.GolangTest{}, WithTableName(strings.Repeat("t", 65)))
	if !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("超长表名必须被拒，实际: %v", err)
	}
}

// ---------------------------------------------------------------------------
// P0-2 容器跨行串数据（pbconv 侧的对照断言在 pbconv 包内，这里测 DB 读取链路）
// ---------------------------------------------------------------------------

// TestReusedMessageDoesNotLeakContainers FindOneByPK 的入参即出参，
// 复用同一个 message 连读两行是本 API 的天然用法。
// 修复前：上一行的 map/list 内容会留在这一行里（空容器早退 + map 只合并不清键）。
func TestReusedMessageDoesNotLeakContainers(t *testing.T) {
	pdb, conn := newFakeDB(t)

	// 第一行 player 有内容，第二行是空的
	first, err := proto.Marshal(&testpb.Player{PlayerId: 1, Name: "alice"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	conn.queueRows(
		rows(row(int64(1), "ip1", int64(1), int64(1), first, int64(1))),
		rows(row(int64(2), "ip2", int64(2), int64(2), []byte{}, int64(2))),
	)

	out := &testpb.GolangTest{Id: 1}
	if err := pdb.FindOneByPK(out); err != nil {
		t.Fatalf("第一次 FindOneByPK: %v", err)
	}
	if out.GetPlayer() == nil {
		t.Fatal("第一行应当读到 player 子消息")
	}

	out.Id = 2
	if err := pdb.FindOneByPK(out); err != nil {
		t.Fatalf("第二次 FindOneByPK: %v", err)
	}
	if out.GetPlayer().GetPlayerId() == 1 {
		t.Error("第二行的 player 是空的，却读到了第一行的内容——跨行串数据")
	}
}

var _ = driver.Value(nil)
var _ protoreflect.Message

// ---------------------------------------------------------------------------
// P0-1 真库版：ODKU 撞次级唯一键时不得改写命中行的主键
// ---------------------------------------------------------------------------

// TestSaveDoesNotHijackRowOnUniqueConflict 这条必须打到真 MySQL 才有意义——
// 「ODKU 在任意唯一键冲突时触发」是 MySQL 的行为，不是本库能靠字符串断言证明的。
//
// 修复前：库里有 (id=1, ip=X)，执行 Save(id=2, ip=X) 之后 id=1 **消失**，
// 只剩一行 id=2——那不是更新，是身份被顶替，而调用方看到的是一次成功的 upsert。
func TestSaveDoesNotHijackRowOnUniqueConflict(t *testing.T) {
	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	const table = "odku_hijack_probe"
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + table + "`"); err != nil {
		t.Fatalf("清理探针表: %v", err)
	}
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS `" + table + "`") }()

	// PK=id，另有一个作用在 ip 上的唯一键——正是会让 ODKU 命中"别人那一行"的形状
	pdb.RegisterTable(&testpb.GolangTest{},
		WithTableName(table), WithPrimaryKey("id"), WithUniqueKey("ip"),
		WithAutoIncrementKey("")) // 自增会干扰这条断言，关掉
	if err := pdb.SyncAllTables(); err != nil {
		t.Fatalf("建表: %v", err)
	}

	if err := pdb.Insert(&testpb.GolangTest{Id: 1, Ip: "10.0.0.1", Port: 999}); err != nil {
		t.Fatalf("插入基准行: %v", err)
	}

	// 与 id=1 撞 ip 唯一键的一次 Save：必须报可识别的重复键，且两行都不能被改。
	if err := pdb.Save(&testpb.GolangTest{Id: 2, Ip: "10.0.0.1", Port: 1}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("Save 应返回 ErrDuplicateKey，实际: %v", err)
	}

	var id1Port, id1Count, id2Count int
	if err := db.QueryRow("SELECT COUNT(*) FROM `" + table + "` WHERE `id` = 1").Scan(&id1Count); err != nil {
		t.Fatalf("回读 id=1: %v", err)
	}
	if id1Count != 1 {
		t.Fatalf("id=1 那一行必须还在（它的主键被改成 2 就是身份被顶替），实际 count=%d", id1Count)
	}
	if err := db.QueryRow("SELECT `port` FROM `" + table + "` WHERE `id` = 1").Scan(&id1Port); err != nil {
		t.Fatalf("回读 id=1 的 port: %v", err)
	}
	if id1Port != 999 {
		t.Errorf("备用唯一键命中的另一行绝不能被改写，port=%d want 999", id1Port)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM `" + table + "` WHERE `id` = 2").Scan(&id2Count); err != nil {
		t.Fatalf("回读 id=2: %v", err)
	}
	if id2Count != 0 {
		t.Errorf("冲突的 id=2 不得被插入，实际 count=%d", id2Count)
	}

	// 历史显式 upsert 入口必须与 Save 使用同一套主键精确语义，不能留下一个仍会
	// 通过备用 UNIQUE 劫持别人的旁路。
	if err := pdb.InsertOnDupUpdate(&testpb.GolangTest{Id: 2, Ip: "10.0.0.1", Port: 1}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("InsertOnDupUpdate 应返回 ErrDuplicateKey，实际: %v", err)
	}
	if err := db.QueryRow("SELECT `port` FROM `" + table + "` WHERE `id` = 1").Scan(&id1Port); err != nil {
		t.Fatalf("upsert 后回读 id=1 的 port: %v", err)
	}
	if id1Port != 999 {
		t.Errorf("InsertOnDupUpdate 不得改写备用唯一键命中的另一行，port=%d want 999", id1Port)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM `" + table + "` WHERE `id` = 2").Scan(&id2Count); err != nil {
		t.Fatalf("upsert 后回读 id=2: %v", err)
	}
	if id2Count != 0 {
		t.Errorf("InsertOnDupUpdate 冲突的 id=2 不得被插入，实际 count=%d", id2Count)
	}
}

// TestSaveIdempotentFloatDoesNotMisclassifyDuplicate 覆盖 MySQL FLOAT 的比较提升规则。
// go-sql-driver 收到的是十进制字符串；若分类查询直接写 `score <=> ?`，MySQL 8.4 会让
// FLOAT 列与更高精度参数比较，`CAST(0.1 AS FLOAT) <=> '0.1'` 结果为 0。第二次完全相同
// 的 Save 因而会被误报成 ErrDuplicateKey。参数必须在 Go 侧还原成 float32 精度后再以
// driver 支持的 float64 绑定；不能用 MySQL 8.0.17 才支持的 CAST(... AS FLOAT)，因为
// 本库仍兼容 MySQL 5.7。
func TestSaveIdempotentFloatDoesNotMisclassifyDuplicate(t *testing.T) {
	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	const tableName = "save_float_match_probe"
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + tableName + "`"); err != nil {
		t.Fatalf("清理 float 探针表: %v", err)
	}
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS `" + tableName + "`") }()

	md := saveFloatProbeDescriptor(t)
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(7))
	msg.Set(md.Fields().ByName("score"), protoreflect.ValueOfFloat32(float32(0.1)))
	pdb.RegisterTable(msg, WithTableName(tableName), WithPrimaryKey("id"))
	if err := pdb.SyncAllTables(); err != nil {
		t.Fatalf("建 float 探针表: %v", err)
	}

	if err := pdb.Save(msg); err != nil {
		t.Fatalf("首次 Save: %v", err)
	}
	if err := pdb.Save(msg); err != nil {
		t.Fatalf("同 PK、同 FLOAT 值的幂等 Save 不应误报唯一键冲突: %v", err)
	}
}

func TestSaveCurrentRowMatchNormalizesFloatParameter(t *testing.T) {
	md := saveFloatProbeDescriptor(t)
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfInt64(7))
	msg.Set(md.Fields().ByName("score"), protoreflect.ValueOfFloat32(float32(0.1)))
	table := newMessageTable(msg, WithPrimaryKey("id"))
	update, err := table.getSaveUpdateSQLWithArgs(msg)
	if err != nil {
		t.Fatalf("getSaveUpdateSQLWithArgs: %v", err)
	}
	match, err := table.getSaveCurrentRowMatchSQLWithArgs(update)
	if err != nil {
		t.Fatalf("getSaveCurrentRowMatchSQLWithArgs: %v", err)
	}
	if strings.Contains(strings.ToUpper(match.Sql), "CAST(? AS FLOAT)") ||
		!strings.Contains(match.Sql, "`score` <=> ?") {
		t.Fatalf("FLOAT 分类必须保留 MySQL 5.7 可执行的普通参数谓词，SQL=%s", match.Sql)
	}
	if got, ok := match.Args[1].(float64); !ok || got != float64(float32(0.1)) {
		t.Fatalf("FLOAT 分类参数必须规范到真实 float32 存储值，arg=%T(%v)", match.Args[1], match.Args[1])
	}
}

func TestNumericPredicatesBindExactDriverTypes(t *testing.T) {
	const high = uint64(9007199254740993) // 2^53+1；转成 DOUBLE 会与前一个整数碰撞
	msg := &testpb.Player{PlayerId: high, Name: "high"}
	table := newMessageTable(msg, WithTableName("uint64_predicate_probe"), WithPrimaryKey("player_id"))

	serialized, err := table.primaryKeySerializedValues(msg)
	if err != nil {
		t.Fatalf("primaryKeySerializedValues: %v", err)
	}
	if got, ok := serialized[0].(string); !ok || got != "9007199254740993" {
		t.Fatalf("缓存 canonical 值必须保持旧版十进制字符串，got=%#v (%T)", serialized[0], serialized[0])
	}

	values, err := table.primaryKeyValues(msg)
	if err != nil {
		t.Fatalf("primaryKeyValues: %v", err)
	}
	if got, ok := values[0].(uint64); !ok || got != high {
		t.Fatalf("SQL 主键参数必须保持完整 uint64，got=%#v (%T)", values[0], values[0])
	}

	stmt, err := table.GetSelectSQLByKVWithArgs("player_id", "9007199254740993")
	if err != nil {
		t.Fatalf("GetSelectSQLByKVWithArgs: %v", err)
	}
	if got, ok := stmt.Args[0].(uint64); !ok || got != high {
		t.Fatalf("公开 KV helper 也必须绑定 uint64，got=%#v (%T)", stmt.Args[0], stmt.Args[0])
	}

	builder := NewSQLBuilder(msg, WithTableName("uint64_predicate_probe"), WithPrimaryKey("player_id"))
	stmt, err = builder.SelectByPKIn([]interface{}{"9007199254740992", "9007199254740993"}, QueryOptions{})
	if err != nil {
		t.Fatalf("SelectByPKIn: %v", err)
	}
	for i, want := range []uint64{9007199254740992, high} {
		if got, ok := stmt.Args[i].(uint64); !ok || got != want {
			t.Fatalf("IN arg[%d] 必须是精确 uint64，got=%#v (%T) want=%d", i, stmt.Args[i], stmt.Args[i], want)
		}
	}
}

func TestCacheKeyKeepsCanonicalBoolEncoding(t *testing.T) {
	md := scalarProbeDescriptor(t, "BoolPKProbe", descriptorpb.FieldDescriptorProto_TYPE_BOOL)
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfBool(true))
	table := newMessageTable(msg, WithTableName("bool_pk_probe"), WithPrimaryKey("id"))

	key, err := cacheKeyFor(table, msg)
	if err != nil {
		t.Fatalf("cacheKeyFor: %v", err)
	}
	if key != "pb:bool_pk_probe:1" {
		t.Fatalf("typed SQL 参数不得改变缓存 canonical key，got=%q want=%q", key, "pb:bool_pk_probe:1")
	}
}

func TestFloatPrimaryKeyFailsClosed(t *testing.T) {
	md := scalarProbeDescriptor(t, "FloatPKProbe", descriptorpb.FieldDescriptorProto_TYPE_FLOAT)
	msg := dynamicpb.NewMessage(md)
	if err := ValidateTableMessage(msg, WithTableName("float_pk_probe"), WithPrimaryKey("id")); !errors.Is(err, ErrInvalidTableOption) {
		t.Fatalf("FLOAT 主键必须在 DDL 前 fail-closed，实际: %v", err)
	}
}

func TestInvalidTableShapesFailBeforeDDL(t *testing.T) {
	t.Run("empty message", func(t *testing.T) {
		fd := &descriptorpb.FileDescriptorProto{
			Name:    proto.String("empty_table_probe.proto"),
			Package: proto.String("auditprobe"),
			Syntax:  proto.String("proto3"),
			MessageType: []*descriptorpb.DescriptorProto{{
				Name: proto.String("EmptyTableProbe"),
			}},
		}
		desc, err := protodesc.NewFile(fd, nil)
		if err != nil {
			t.Fatalf("构造 empty message: %v", err)
		}
		msg := dynamicpb.NewMessage(desc.Messages().Get(0))
		if err := ValidateTableMessage(msg, WithTableName("empty_table_probe")); !errors.Is(err, ErrUnsupportedFieldKind) {
			t.Fatalf("0 字段 message 必须在 DDL 前失败，实际: %v", err)
		}
	})

	t.Run("column name too long", func(t *testing.T) {
		fieldName := strings.Repeat("a", MySQLMaxIdentifierLength+1)
		fd := &descriptorpb.FileDescriptorProto{
			Name:    proto.String("long_column_probe.proto"),
			Package: proto.String("auditprobe"),
			Syntax:  proto.String("proto3"),
			MessageType: []*descriptorpb.DescriptorProto{{
				Name: proto.String("LongColumnProbe"),
				Field: []*descriptorpb.FieldDescriptorProto{{
					Name:   proto.String(fieldName),
					Number: proto.Int32(1),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				}},
			}},
		}
		desc, err := protodesc.NewFile(fd, nil)
		if err != nil {
			t.Fatalf("构造 long column message: %v", err)
		}
		msg := dynamicpb.NewMessage(desc.Messages().Get(0))
		if err := ValidateTableMessage(msg, WithTableName("long_column_probe")); !errors.Is(err, ErrUnsupportedFieldKind) {
			t.Fatalf("超长列名必须在 DDL 前失败，实际: %v", err)
		}
	})
}

func scalarProbeDescriptor(t *testing.T, name string, kind descriptorpb.FieldDescriptorProto_Type) protoreflect.MessageDescriptor {
	t.Helper()
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String(strings.ToLower(name) + ".proto"),
		Package: proto.String("auditprobe"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String(name),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String("id"),
				Number: proto.Int32(1),
				Type:   kind.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			}},
		}},
	}
	desc, err := protodesc.NewFile(fd, nil)
	if err != nil {
		t.Fatalf("构造 scalar 探针描述符: %v", err)
	}
	return desc.Messages().Get(0)
}

// TestUint64PredicatesDoNotCollapseAboveDoublePrecision 是真实 MySQL/TiDB 回归。
// 修复前 WHERE bigint_unsigned = 'decimal string' 会经 DOUBLE 比较，让 2^53 与 2^53+1
// 命中同一行；Find/Save/Delete 都可能作用到相邻账号。
func TestUint64PredicatesDoNotCollapseAboveDoublePrecision(t *testing.T) {
	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	const tableName = "uint64_predicate_probe"
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + tableName + "`"); err != nil {
		t.Fatalf("清理 uint64 探针表: %v", err)
	}
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS `" + tableName + "`") }()

	pdb.RegisterTable(&testpb.Player{}, WithTableName(tableName), WithPrimaryKey("player_id"))
	if err := pdb.SyncAllTables(); err != nil {
		t.Fatalf("建 uint64 探针表: %v", err)
	}

	const low = uint64(9007199254740992)
	const high = uint64(9007199254740993)
	if err := pdb.Insert(&testpb.Player{PlayerId: low, Name: "low"}); err != nil {
		t.Fatalf("插入 low: %v", err)
	}
	if err := pdb.Insert(&testpb.Player{PlayerId: high, Name: "high"}); err != nil {
		t.Fatalf("插入 high: %v", err)
	}

	byPK := &testpb.Player{PlayerId: high}
	if err := pdb.FindOneByPK(byPK); err != nil {
		t.Fatalf("按 high 主键读取: %v", err)
	}
	if byPK.Name != "high" {
		t.Fatalf("按 high 主键选错相邻行，name=%q", byPK.Name)
	}

	byKV := &testpb.Player{}
	if err := pdb.FindOneByKV(byKV, "player_id", "9007199254740993"); err != nil {
		t.Fatalf("按字符串 KV 读取 high: %v", err)
	}
	if byKV.PlayerId != high || byKV.Name != "high" {
		t.Fatalf("KV helper 选错相邻行: %+v", byKV)
	}

	updated := &testpb.Player{PlayerId: high, Name: "high-updated"}
	if err := pdb.Save(updated); err != nil {
		t.Fatalf("更新 high: %v", err)
	}
	if err := pdb.Save(updated); err != nil {
		t.Fatalf("幂等 Save high 不应误分类: %v", err)
	}

	var lowName, highName string
	if err := db.QueryRow("SELECT `name` FROM `"+tableName+"` WHERE `player_id` = ?", low).Scan(&lowName); err != nil {
		t.Fatalf("回读 low: %v", err)
	}
	if err := db.QueryRow("SELECT `name` FROM `"+tableName+"` WHERE `player_id` = ?", high).Scan(&highName); err != nil {
		t.Fatalf("回读 high: %v", err)
	}
	if lowName != "low" || highName != "high-updated" {
		t.Fatalf("更新命中错误：low=%q high=%q", lowName, highName)
	}

	if err := pdb.DeleteByKV(&testpb.Player{}, "player_id", "9007199254740993"); err != nil {
		t.Fatalf("删除 high: %v", err)
	}
	var lowCount, highCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM `"+tableName+"` WHERE `player_id` = ?", low).Scan(&lowCount); err != nil {
		t.Fatalf("统计 low: %v", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM `"+tableName+"` WHERE `player_id` = ?", high).Scan(&highCount); err != nil {
		t.Fatalf("统计 high: %v", err)
	}
	if lowCount != 1 || highCount != 0 {
		t.Fatalf("删除命中错误：low=%d high=%d", lowCount, highCount)
	}
}

func saveFloatProbeDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("save_float_match_probe.proto"),
		Package: proto.String("auditprobe"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("SaveFloatMatchProbe"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name:   proto.String("id"),
					Number: proto.Int32(1),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name:   proto.String("score"),
					Number: proto.Int32(2),
					Type:   descriptorpb.FieldDescriptorProto_TYPE_FLOAT.Enum(),
					Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
			},
		}},
	}
	desc, err := protodesc.NewFile(fd, nil)
	if err != nil {
		t.Fatalf("构造 float 探针描述符: %v", err)
	}
	return desc.Messages().Get(0)
}

func TestCoreInsertOnDupUpdateUsesPrimaryKeyExactSave(t *testing.T) {
	pdb, conn := newFakeDB(t)
	if err := pdb.InsertOnDupUpdate(&testpb.GolangTest{Id: 7, Ip: "x"}); err != nil {
		t.Fatalf("InsertOnDupUpdate: %v", err)
	}
	sqls := conn.sqls()
	if len(sqls) == 0 || !strings.HasPrefix(sqls[0], "UPDATE ") {
		t.Fatalf("显式 upsert 必须先按完整主键 UPDATE，SQL=%v", sqls)
	}
	for _, stmt := range sqls {
		if strings.Contains(strings.ToUpper(stmt), "ON DUPLICATE KEY UPDATE") {
			t.Fatalf("显式 upsert 不得再把冲突来源交给 ODKU 猜测: %s", stmt)
		}
	}
}

func TestCoreInsertIgnoreOnlySuppressesDuplicateKey(t *testing.T) {
	t.Run("duplicate", func(t *testing.T) {
		pdb, conn := newFakeDB(t)
		conn.failNext = &mysqldriver.MySQLError{Number: 1062, Message: "duplicate PRIMARY"}

		inserted, err := pdb.InsertIgnore(&testpb.GolangTest{Id: 1})
		if err != nil || inserted {
			t.Fatalf("1062 应只表示未插入: inserted=%v err=%v", inserted, err)
		}
		if stmt := conn.findSQL("INSERT"); strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "INSERT IGNORE") {
			t.Fatalf("不得使用会吞真实数据错误的 INSERT IGNORE: %s", stmt)
		}
	})

	t.Run("data too long", func(t *testing.T) {
		pdb, conn := newFakeDB(t)
		conn.failNext = &mysqldriver.MySQLError{Number: 1406, Message: "data too long"}
		if _, err := pdb.InsertIgnore(&testpb.GolangTest{Id: 1}); err == nil {
			t.Fatal("非重复键错误必须返回")
		}
	})
}

func TestCoreDecrByPKIfEnoughRequiresPositiveDelta(t *testing.T) {
	for _, delta := range []int64{0, -1} {
		t.Run(fmt.Sprintf("delta=%d", delta), func(t *testing.T) {
			pdb, conn := newFakeDB(t)
			updated, err := pdb.DecrByPKIfEnough(&testpb.GolangTest{Id: 1}, "port", delta)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "positive") {
				t.Fatalf("delta=%d 必须返回 positive 参数错误，updated=%v err=%v", delta, updated, err)
			}
			if got := conn.sqls(); len(got) != 0 {
				t.Fatalf("非法 delta 不得下发 SQL，delta=%d SQL=%v", delta, got)
			}
		})
	}
}

func TestAllExportedCreateTableEntrypointsFailClosed(t *testing.T) {
	msg := dynamicpb.NewMessage(unsupportedKindDescriptor(t))
	opts := []TableOption{WithTableName("unsupported_probe")}
	table := newMessageTable(msg, opts...)
	if got := table.GetCreateTableSQL(); got != "" {
		t.Fatalf("MessageTable.GetCreateTableSQL 必须 fail-closed，实际:\n%s", got)
	}

	pdb := NewDB()
	pdb.RegisterTable(msg, opts...)
	if got := pdb.GetCreateTableSQL(msg); got != "" {
		t.Fatalf("DB.GetCreateTableSQL 必须 fail-closed，实际:\n%s", got)
	}
	gdb := NewGormDB(nil, "")
	gdb.RegisterTable(msg, opts...)
	if got := gdb.GetCreateTableSQL(msg); got != "" {
		t.Fatalf("GormDB.GetCreateTableSQL 必须 fail-closed，实际:\n%s", got)
	}
	if got := NewSQLBuilder(msg, opts...).CreateTable(); got != "" {
		t.Fatalf("SQLBuilder.CreateTable 必须 fail-closed，实际:\n%s", got)
	}
}

func TestSyncAllTablesPrevalidatesWholeBatchBeforeDatabaseIO(t *testing.T) {
	pdb, conn := newFakeDB(t)
	invalid := dynamicpb.NewMessage(unsupportedKindDescriptor(t))
	pdb.RegisterTable(invalid, WithTableName("z_invalid"))

	if err := pdb.SyncAllTables(); !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("整批同步应在任何数据库 I/O 前拒绝非法表，实际: %v", err)
	}
	if got := conn.sqls(); len(got) != 0 {
		t.Fatalf("预检失败前不得获取锁或下发前面合法表的 DDL，SQL=%v", got)
	}
}

func TestRuntimeSchemaSyncRejectsDuplicatePhysicalTableMappingsBeforeIO(t *testing.T) {
	pdb, conn := newFakeDB(t)
	pdb.RegisterTable(&testpb.GolangTest{}, WithTableName("shared_runtime_table"), WithPrimaryKey("id"))
	pdb.RegisterTable(&testpb.GolangTest1{}, WithTableName("SHARED_RUNTIME_TABLE"), WithPrimaryKey("id"))

	if err := pdb.SyncAllTables(); !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("整批 runtime sync 必须拒绝物理表冲突，实际: %v", err)
	}
	if got := conn.sqls(); len(got) != 0 {
		t.Fatalf("映射冲突必须在咨询锁/元数据 I/O 前失败，SQL=%v", got)
	}

	if err := pdb.UpdateTableField(&testpb.GolangTest{}); !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("单表 runtime sync 也不能绕过 registry 冲突，实际: %v", err)
	}
	if got := conn.sqls(); len(got) != 0 {
		t.Fatalf("单表映射冲突也不得触发 I/O，SQL=%v", got)
	}
	if err := pdb.Insert(&testpb.GolangTest{Id: 1}); !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("使用预建表时 core DML 也必须拒绝冲突 registry，实际: %v", err)
	}
	if _, err := pdb.CacheKey(&testpb.GolangTest{Id: 1}); !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("缓存 key 不得跨 descriptor 共享同一物理表，实际: %v", err)
	}
	if err := pdb.FindAll(&testpb.GolangTestList{}); !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("列表查询入口也必须拒绝冲突 registry，实际: %v", err)
	}
	if got := conn.sqls(); len(got) != 0 {
		t.Fatalf("DML/cache 映射冲突必须在数据库 I/O 前失败，SQL=%v", got)
	}

	gdb := NewGormDB(nil, "")
	gdb.RegisterTable(&testpb.GolangTest{}, WithTableName("shared_runtime_table"), WithPrimaryKey("id"))
	gdb.RegisterTable(&testpb.GolangTest1{}, WithTableName("SHARED_RUNTIME_TABLE"), WithPrimaryKey("id"))
	if err := gdb.CreateOrUpdateTable(&testpb.GolangTest{}); !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("GORM schema bridge 必须先拒绝完整 registry 冲突，实际: %v", err)
	}
	if err := gdb.Insert(&testpb.GolangTest{Id: 1}); !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("使用预建表时 GORM DML 也必须拒绝冲突 registry，实际: %v", err)
	}
}
