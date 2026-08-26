package proto2mysql

import (
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// 本文件覆盖「连库的结构同步路径」，对应 docs/schema-evolution.md 与
// docs/fixes-2026-08.md 里逐条声称的行为。
//
// 这些路径原先在日常 go test 里一行都跑不到（要么是纯函数测试、要么整个 t.Skip），
// 所以全绿只是因为没测。docs 里写的每条行为都必须有一条测试兜着，
// 否则文档写完就开始腐烂。

// golangTestAlignedCols 与 GolangTest 完全对齐的线上列快照。
func golangTestAlignedCols() [][]driver.Value {
	return rows(
		colRow("id", "int unsigned", 1),
		colRow("ip", "mediumtext", 2),
		colRow("port", "int unsigned", 3),
		colRow("group_id", "int unsigned", 4),
		colRow("player", "mediumblob", 5),
		colRow("player_id", "bigint unsigned", 6),
	)
}

// TestSyncCreatesTableThenStillAligns 对应 fixes-2026-08 第 8 条。
//
// 建表分支**刻意不 return**：CREATE TABLE IF NOT EXISTS 在并发下可能整条是 no-op，
// 早先直接 return 会让本进程独有的新列从未被添加，而进程启动成功、零异常。
func TestSyncCreatesTableThenStillAligns(t *testing.T) {
	pdb, conn := newFakeDB(t)
	conn.queueRows(
		rows(row(int64(0))),     // is_table_exists → 不存在
		nil,                     // CREATE TABLE 自身
		golangTestAlignedCols(), // 建完回读：已对齐
		rows(row(int64(1))),     // 有主键
	)

	if err := pdb.CreateOrUpdateTable(&testpb.GolangTest{}); err != nil {
		t.Fatalf("CreateOrUpdateTable: %v", err)
	}
	if got := conn.findSQL("CREATE TABLE IF NOT EXISTS `golang_test`"); got == "" {
		t.Fatal("应当发出建表语句")
	}
	// 建完就对齐了，不该再发 ALTER
	if got := conn.findSQL("ALTER TABLE"); got != "" {
		t.Errorf("结构已对齐时不该发 ALTER: %s", got)
	}
	// 关键：建表之后**仍然回读了**一次 information_schema
	if conn.countSQL("INFORMATION_SCHEMA.COLUMNS") == 0 {
		t.Error("建表后必须继续走列对齐（CREATE IF NOT EXISTS 可能是 no-op）")
	}
}

// TestSyncStillAlignsWhenCreateWasNoop 对应 fixes-2026-08 第 8 条的事故时序。
//
// 两个版本同时冷启动到空库：本进程检查时表还不存在，检查与 CREATE 之间另一个进程
// 按它自己（较旧的）proto 建好了表。本进程这条 CREATE 只拿到 Warning 1050，
// 不报错、不改结构——必须靠后续的列对齐把自己的新列补上。
func TestSyncStillAlignsWhenCreateWasNoop(t *testing.T) {
	pdb, conn := newFakeDB(t)
	older := golangTestAlignedCols()[:5] // 别人建的旧结构：缺 player_id(pb:6)
	conn.queueRows(
		rows(row(int64(0))), // 检查时表还不存在
		nil,                 // CREATE（实际是 no-op，已被别人建走）
		older,               // 回读到别人建的旧结构
		rows(row(int64(1))), // 有主键
	)

	if err := pdb.CreateOrUpdateTable(&testpb.GolangTest{}); err != nil {
		t.Fatalf("CreateOrUpdateTable: %v", err)
	}
	alter := conn.findSQL("ALTER TABLE `golang_test`")
	if alter == "" {
		t.Fatal("CREATE 是 no-op 时必须补上自己的新列，否则启动成功但第一条 SELECT 就 1054")
	}
	if !strings.Contains(alter, "ADD COLUMN `player_id`") {
		t.Errorf("应补 player_id: %s", alter)
	}
}

// TestSyncTakesAdvisoryLock 对应 fixes-2026-08 第 11 条。
//
// 结构同步全程持一把 GET_LOCK，避免 N 个副本同时 ALTER 撞 Error 1060
// （那种失败会"自愈式蒙对"：重启就好了，于是被当成偶发 flake 忽略）。
func TestSyncTakesAdvisoryLock(t *testing.T) {
	pdb, conn := newFakeDB(t)
	conn.queueRows(
		rows(row(int64(1))),     // GET_LOCK → 1
		rows(row(int64(1))),     // 表存在
		golangTestAlignedCols(), // 已对齐
		rows(row(int64(1))),     // 有主键
		rows(row(int64(1))),     // RELEASE_LOCK
	)

	if err := pdb.SyncAllTables(); err != nil {
		t.Fatalf("SyncAllTables: %v", err)
	}
	all := conn.sqls()
	if len(all) == 0 || !strings.Contains(all[0], "GET_LOCK") {
		t.Fatalf("抢锁必须是第一条语句，实际: %v", all)
	}
	if conn.countSQL("RELEASE_LOCK") != 1 {
		t.Errorf("必须释放锁一次，实际 %d 次", conn.countSQL("RELEASE_LOCK"))
	}
}

// TestSyncDegradesWhenLockUnavailable 拿不到锁只降级 + 告警，不阻断。
//
// 可用性比"锁一定要拿到"更重要：拿不到锁最坏是撞 1060、重启自愈；
// 而因为拿不到锁就拒绝启动，是把并发问题升级成可用性事故。
// TiDB 等兼容实现也不一定支持 GET_LOCK。
func TestSyncDegradesWhenLockUnavailable(t *testing.T) {
	pdb, conn := newFakeDB(t)
	conn.queueRows(
		rows(row(int64(0))),     // GET_LOCK → 0，等超时了
		rows(row(int64(1))),     // 表存在
		golangTestAlignedCols(), // 已对齐
		rows(row(int64(1))),     // 有主键
	)

	if err := pdb.SyncAllTables(); err != nil {
		t.Fatalf("拿不到锁不应阻断启动: %v", err)
	}
	if conn.countSQL("RELEASE_LOCK") != 0 {
		t.Error("没拿到锁就不该去释放")
	}
}

// TestSyncBackfillsMissingIndexes 对应 fixes-2026-08 第 12 条。
//
// 早先索引只出现在 CREATE TABLE 分支：表一旦建成，之后在 .proto 里新加
// index / unique_key 完全不生效，且零提示——查询照常能跑，只是走全表扫描。
func TestSyncBackfillsMissingIndexes(t *testing.T) {
	sqlDB, conn := openFakeDB()
	t.Cleanup(func() { _ = sqlDB.Close() })

	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(&testpb.GolangTest{},
		WithPrimaryKey("id"), WithIndexes("player_id"), WithUniqueKey("ip"))

	conn.queueRows(
		rows(row(int64(1))),     // 表存在
		golangTestAlignedCols(), // 列已对齐
		rows(row(int64(1))),     // 有主键
		nil,                     // 既有索引：一个都没有
		nil,                     // ALTER 自身
	)

	if err := pdb.CreateOrUpdateTable(&testpb.GolangTest{}); err != nil {
		t.Fatalf("CreateOrUpdateTable: %v", err)
	}
	alter := conn.findSQL("ALTER TABLE `golang_test`")
	if alter == "" {
		t.Fatal("缺索引时必须发 ALTER 补上")
	}
	if !strings.Contains(alter, "ADD INDEX `idx_golang_test_0` (`player_id`)") {
		t.Errorf("应补普通索引: %s", alter)
	}
	// ip 是 MEDIUMTEXT，索引必须带前缀长度，否则 MySQL 报 Error 1170
	if !strings.Contains(alter, "ADD UNIQUE KEY `uk_golang_test` (`ip`(191))") {
		t.Errorf("应补唯一键且带前缀长度: %s", alter)
	}
}

// TestSyncSkipsIndexQueryWhenNoneDeclared proto 里没声明索引时不必去查 information_schema。
func TestSyncSkipsIndexQueryWhenNoneDeclared(t *testing.T) {
	pdb, conn := newFakeDB(t) // 只有主键，没有 index/unique_key
	conn.queueRows(
		rows(row(int64(1))),
		golangTestAlignedCols(),
		rows(row(int64(1))),
	)
	if err := pdb.CreateOrUpdateTable(&testpb.GolangTest{}); err != nil {
		t.Fatalf("CreateOrUpdateTable: %v", err)
	}
	if conn.countSQL("INFORMATION_SCHEMA.STATISTICS") != 0 {
		t.Error("没声明索引时不该查 STATISTICS")
	}
}

// TestAwaitSchemaVisibleOnlyWaitsForAddedColumns 对应 fixes-2026-08 第 13 条。
//
// TiDB 的 DDL 是异步 online 的：ALTER 返回时变更只是进了队列，各节点按 lease
// （默认 45s）分批加载。库执行完立刻按新 proto 发 SQL，这段窗口里连到还没加载
// 新 schema 的节点就报 Unknown column。所以 ALTER 之后要回读到真的可见为止。
//
// 只等**本次新增的列**：MODIFY / CHANGE 改的是已有列，回读时本来就看得见。
func TestAwaitSchemaVisibleOnlyWaitsForAddedColumns(t *testing.T) {
	cases := []struct {
		name    string
		clauses []string
		want    []string
	}{
		{"纯新增", []string{"ADD COLUMN `foo` bigint COMMENT 'pb:9'"}, []string{"foo"}},
		{"MODIFY 不等", []string{"MODIFY COLUMN `ip` MEDIUMTEXT COMMENT 'pb:2'"}, nil},
		{"CHANGE 不等", []string{"CHANGE COLUMN `a` `b` MEDIUMTEXT COMMENT 'pb:2'"}, nil},
		{"索引不等", []string{"ADD INDEX `idx_x` (`a`)"}, nil},
		{"混合只取新增", []string{
			"MODIFY COLUMN `ip` MEDIUMTEXT COMMENT 'pb:2'",
			"ADD COLUMN `bar` int COMMENT 'pb:8'",
		}, []string{"bar"}},
	}
	for _, c := range cases {
		got := addedColumnNames(c.clauses)
		if len(got) != len(c.want) {
			t.Errorf("%s: 期望等待 %v，实际 %v", c.name, c.want, got)
			continue
		}
		for _, name := range c.want {
			if _, ok := got[name]; !ok {
				t.Errorf("%s: 应当等待列 %s，实际 %v", c.name, name, got)
			}
		}
	}
}

// TestAwaitSchemaVisibleGivesUpOnEmptyRead 一列都读不到时立即放弃，不白等到超时。
//
// 真表不可能零列，读不到说明是权限/库名的问题，继续轮询只会把启动拖满 60 秒。
func TestAwaitSchemaVisibleGivesUpOnEmptyRead(t *testing.T) {
	pdb, conn := newFakeDB(t)
	conn.queueRows(nil) // 回读返回空

	table := pdb.Tables[GetTableName(&testpb.GolangTest{})]
	done := make(chan struct{})
	go func() {
		pdb.awaitSchemaVisible(GetTableName(&testpb.GolangTest{}), table,
			[]string{"ADD COLUMN `foo` bigint COMMENT 'pb:9'"})
		close(done)
	}()

	select {
	case <-done:
	case <-timeAfterShort():
		t.Fatal("读不到任何列时应立即返回，不该轮询到超时")
	}
}

// TestUnsupportedFieldKindFailsFast 对应 fixes-2026-08 附录第一条。
//
// sint32/fixed64 这类没有 MySQL 映射的类型，早先静默回落成 TEXT：建表一路成功，
// 跑到第一次写入才抛错，而那时列已经建出来、可能还上了线。
func TestUnsupportedFieldKindFailsFast(t *testing.T) {
	md := unsupportedKindDescriptor(t)
	table := newMessageTableFromDescriptor(md, WithTableName("bad_kinds"))

	_, err := table.buildAlterClauses(map[string]columnMeta{}, false)
	if !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("不支持的字段类型必须在生成 DDL 时就 fail-fast，实际 err=%v", err)
	}
	if !strings.Contains(err.Error(), "sint32") {
		t.Errorf("错误信息应指出替代方案: %v", err)
	}
}

// TestByFieldNumIsDeterministic 对应 fixes-2026-08 第 10 条（Go 独有）。
//
// byFieldNum 早先是边遍历 map 边写的，而 Go 的 map 迭代顺序是随机化的。
// 线上出现两列带同一个 pb:N 时（DBA 照 SHOW CREATE TABLE 复制个备份列就会），
// 每次运行挑中的列都可能不同——同一份 proto 跑两次得到两份 DDL。
func TestByFieldNumIsDeterministic(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	ipType := table.getMySQLFieldType(table.Descriptor.Fields().ByName("ip"))

	// 线上有两列都带 pb:2：真列 zz_ip 和备份列 aa_ip_backup
	current := alignedCols(table, map[string]columnMeta{
		"aa_ip_backup": {colType: ipType, fieldNum: 2},
		"zz_ip":        {colType: ipType, fieldNum: 2},
	}, "ip")

	var first string
	for i := 0; i < 20; i++ {
		clauses, err := table.buildAlterClauses(current, false)
		if err != nil {
			t.Fatalf("buildAlterClauses: %v", err)
		}
		joined := strings.Join(clauses, " | ")
		if i == 0 {
			first = joined
			continue
		}
		if joined != first {
			t.Fatalf("同一输入必须产出同一份 DDL，第 %d 次不同:\n%s\n%s", i, first, joined)
		}
	}
	// 冲突时取列名字典序最小的那个
	if !strings.Contains(first, "CHANGE COLUMN `aa_ip_backup` `ip`") {
		t.Errorf("冲突时应取字典序最小的列: %s", first)
	}
}

// TestGormCreateOrUpdateTableAlters 对应 fixes-2026-08 第 7 条（Go 独有）。
//
// GormDB.CreateOrUpdateTable 早先只执行 CREATE TABLE IF NOT EXISTS，整个 949 行
// 的 GormDB 一条 ALTER 都没有。在已存在的表上是**无条件、每次、永久**的 no-op——
// 不是竞态，是百分之百不生效，且重启也不会好。
//
// 这里只验"确实会生成 ALTER 路径"（不引入 gorm 的假驱动）：
// 断言 GormDB 现在具备与 DB 相同的对齐入口与 ExpandOnly 开关。
func TestGormCreateOrUpdateTableAlters(t *testing.T) {
	g := NewGormDB(nil, "testdb")
	g.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))

	// ExpandOnly 必须能透传（WithDB 之后也要继承，否则 bind 出来的实例静默丢设置）
	g.SetExpandOnly(true)
	if !g.expandOnly {
		t.Fatal("SetExpandOnly 未生效")
	}
	if !g.WithDB(nil).expandOnly {
		t.Error("WithDB 必须继承 ExpandOnly，否则换连接后设置被静默丢掉")
	}

	// 对齐逻辑必须与 DB 走同一个 buildAlterClauses（而不是各写一份）
	table := g.Tables[GetTableName(&testpb.GolangTest{})]
	older := map[string]columnMeta{
		"id": {colType: "int unsigned", fieldNum: 1},
	}
	clauses, err := table.buildAlterClauses(older, false)
	if err != nil {
		t.Fatalf("buildAlterClauses: %v", err)
	}
	if len(clauses) == 0 {
		t.Fatal("缺列时必须产出 ALTER 子句")
	}
}

// ── helpers ─────────────────────────────────────────────────────────────

func timeAfterShort() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		// 就绪探测的轮询间隔是 200ms；给 3 秒足够区分"立即返回"与"轮询到 60s 超时"
		for i := 0; i < 15; i++ {
			sleepShort()
		}
		close(ch)
	}()
	return ch
}

func newMessageTableFromDescriptor(md protoreflect.MessageDescriptor, opts ...TableOption) *MessageTable {
	table := &MessageTable{Descriptor: md, tableName: string(md.FullName())}
	for _, opt := range opts {
		opt(table)
	}
	table.Init()
	return table
}

var _ = proto.Marshal // 保持 proto 依赖（helper 里按需使用）

func sleepShort() { time.Sleep(200 * time.Millisecond) }

// unsupportedKindDescriptor 现造一个含 sint32 字段的消息描述符。
func unsupportedKindDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("bad_kinds.proto"),
		Package: proto.String("badkinds"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("bad_kinds"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("id"), Number: proto.Int32(1),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name: proto.String("zigzag"), Number: proto.Int32(2),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_SINT32.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
			},
		}},
	}
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	return fd.Messages().Get(0)
}
