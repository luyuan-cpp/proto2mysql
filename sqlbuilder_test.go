package proto2mysql

import (
	"errors"
	"testing"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
)

// 本文件全部为离线单测：SQLBuilder 只产 SQL 不连库，无需 PROTO2MYSQL_INTEGRATION。

const (
	testAllCols   = "`id`, `ip`, `port`, `group_id`, `player`, `player_id`"
	testSelectAll = "SELECT " + testAllCols + " FROM `golang_test`"
)

func newTestBuilder() *SQLBuilder {
	return NewSQLBuilder(&testpb.GolangTest{})
}

func checkStmt(t *testing.T, stmt *SqlWithArgs, err error, wantSQL string, wantArgs []interface{}) {
	t.Helper()
	if err != nil {
		t.Fatalf("生成SQL失败: %v", err)
	}
	if stmt.Sql != wantSQL {
		t.Errorf("SQL不符\n实际: %s\n期望: %s", stmt.Sql, wantSQL)
	}
	if len(stmt.Args) != len(wantArgs) {
		t.Fatalf("参数个数不符: 实际%d 期望%d (实际=%v)", len(stmt.Args), len(wantArgs), stmt.Args)
	}
	for i := range wantArgs {
		if stmt.Args[i] != wantArgs[i] {
			t.Errorf("第%d个参数不符: 实际%v 期望%v", i, stmt.Args[i], wantArgs[i])
		}
	}
}

func TestSQLBuilderInsertVariants(t *testing.T) {
	b := newTestBuilder()
	msg := &testpb.GolangTest{Id: 7, Ip: "10.0.0.1", Port: 8080}

	stmt, err := b.Insert(msg)
	checkStmt(t, stmt, err,
		"INSERT INTO `golang_test` ("+testAllCols+") VALUES (?, ?, ?, ?, ?, ?)",
		[]interface{}{"7", "10.0.0.1", "8080", "0", "", "0"})

	// 列子集：proto3 零值字段（group_id/player_id）视为未赋值，交给列默认值
	stmt, err = b.InsertSetFields(msg)
	checkStmt(t, stmt, err,
		"INSERT INTO `golang_test` (`id`, `ip`, `port`) VALUES (?, ?, ?)",
		[]interface{}{"7", "10.0.0.1", "8080"})

	stmt, err = b.InsertIgnore(msg)
	checkStmt(t, stmt, err,
		"INSERT INTO `golang_test` ("+testAllCols+") VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id` = `id`",
		[]interface{}{"7", "10.0.0.1", "8080", "0", "", "0"})

	stmt, err = b.InsertIgnoreSetFields(msg)
	checkStmt(t, stmt, err,
		"INSERT INTO `golang_test` (`id`, `ip`, `port`) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `id` = `id`",
		[]interface{}{"7", "10.0.0.1", "8080"})

	// 自增主键未赋值时，列子集写法会整列省略，由 MySQL 发号
	stmt, err = b.InsertSetFields(&testpb.GolangTest{Ip: "10.0.0.2"})
	checkStmt(t, stmt, err,
		"INSERT INTO `golang_test` (`ip`) VALUES (?)",
		[]interface{}{"10.0.0.2"})

	if _, err := b.InsertSetFields(&testpb.GolangTest{}); !errors.Is(err, ErrNoFieldsSet) {
		t.Errorf("空消息应返回ErrNoFieldsSet，实际: %v", err)
	}
}

func TestSQLBuilderBatchInsert(t *testing.T) {
	b := newTestBuilder()
	msgs := []proto.Message{
		&testpb.GolangTest{Id: 1, Ip: "a"},
		&testpb.GolangTest{Id: 2, Ip: "b"},
	}

	stmt, err := b.BatchInsertIgnore(msgs)
	checkStmt(t, stmt, err,
		"INSERT INTO `golang_test` ("+testAllCols+") VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id` = `id`",
		[]interface{}{"1", "a", "0", "0", "", "0", "2", "b", "0", "0", "", "0"})

	stmt, err = b.BatchUpsert(msgs, "ip")
	checkStmt(t, stmt, err,
		"INSERT INTO `golang_test` ("+testAllCols+") VALUES (?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?)"+
			" ON DUPLICATE KEY UPDATE `ip` = VALUES(`ip`)",
		[]interface{}{"1", "a", "0", "0", "", "0", "2", "b", "0", "0", "", "0"})
}

func TestSQLBuilderUpsertSemantics(t *testing.T) {
	b := newTestBuilder()
	msg := &testpb.GolangTest{Id: 7, Ip: "10.0.0.1", Port: 8080}
	insertPart := "INSERT INTO `golang_test` (" + testAllCols + ") VALUES (?, ?, ?, ?, ?, ?)"
	insertArgs := []interface{}{"7", "10.0.0.1", "8080", "0", "", "0"}

	// 缺省覆盖全部非主键列
	stmt, err := b.Upsert(msg)
	checkStmt(t, stmt, err,
		insertPart+" ON DUPLICATE KEY UPDATE `ip` = VALUES(`ip`), `port` = VALUES(`port`),"+
			" `group_id` = VALUES(`group_id`), `player` = VALUES(`player`), `player_id` = VALUES(`player_id`)",
		insertArgs)

	stmt, err = b.Upsert(msg, "ip", "port")
	checkStmt(t, stmt, err,
		insertPart+" ON DUPLICATE KEY UPDATE `ip` = VALUES(`ip`), `port` = VALUES(`port`)",
		insertArgs)

	// 累加语义：冲突时把本次的值加到旧值上
	stmt, err = b.UpsertAdd(msg, "port")
	checkStmt(t, stmt, err,
		insertPart+" ON DUPLICATE KEY UPDATE `port` = `port` + VALUES(`port`)",
		insertArgs)
	if _, err := b.UpsertAdd(msg); !errors.Is(err, ErrNoAssigns) {
		t.Errorf("累加未指定列应返回ErrNoAssigns，实际: %v", err)
	}
	if _, err := b.UpsertAdd(msg, "ip"); err == nil {
		t.Error("累加非数值列应报错")
	}

	// 插入或占位：不改数据，只在冲突时持有行锁
	stmt, err = b.UpsertKeepOld(msg)
	checkStmt(t, stmt, err,
		insertPart+" ON DUPLICATE KEY UPDATE `id` = `id`",
		insertArgs)

	// 混合语义：代次自增 + 新值覆盖 + 取最值 + 首写生效
	stmt, err = b.UpsertWith(msg,
		SetColExpr("group_id", "`group_id` + 1"),
		SetNew("ip"),
		MinNew("port"),
		MaxNew("player_id"),
		SetNewIfZero("player_id"),
		SetCol("port", 3306))
	checkStmt(t, stmt, err,
		insertPart+" ON DUPLICATE KEY UPDATE `group_id` = `group_id` + 1, `ip` = VALUES(`ip`),"+
			" `port` = LEAST(`port`, VALUES(`port`)), `player_id` = GREATEST(`player_id`, VALUES(`player_id`)),"+
			" `player_id` = IF(`player_id` = 0, VALUES(`player_id`), `player_id`), `port` = ?",
		append(append([]interface{}{}, insertArgs...), 3306))
}

func TestSQLBuilderSelect(t *testing.T) {
	b := newTestBuilder()
	msg := &testpb.GolangTest{Id: 7}

	stmt, err := b.SelectByPK(msg)
	checkStmt(t, stmt, err, testSelectAll+" WHERE `id` = ?", []interface{}{uint64(7)})

	stmt, err = b.SelectByPKForUpdate(msg)
	checkStmt(t, stmt, err, testSelectAll+" WHERE `id` = ? FOR UPDATE", []interface{}{uint64(7)})

	got := b.SelectWhere("`port` > ?", []interface{}{80}, QueryOptions{OrderBy: "`id` DESC", Limit: 20, Offset: 40})
	checkStmt(t, got, nil,
		testSelectAll+" WHERE `port` > ? ORDER BY `id` DESC LIMIT 20 OFFSET 40",
		[]interface{}{80})

	// 空条件退化为恒真，便于“全表 + 排序分页”
	got = b.SelectWhere("", nil, QueryOptions{Limit: 1})
	checkStmt(t, got, nil, testSelectAll+" WHERE 1=1 LIMIT 1", nil)

	// 事务内只读一列并加锁
	stmt, err = b.SelectColumns([]string{"port"}, "`id` = ?", []interface{}{7}, QueryOptions{ForUpdate: true})
	checkStmt(t, stmt, err,
		"SELECT `port` FROM `golang_test` WHERE `id` = ? FOR UPDATE",
		[]interface{}{7})

	stmt, err = b.SelectByKVIn("player_id", []interface{}{1, 2, 3}, QueryOptions{})
	checkStmt(t, stmt, err,
		testSelectAll+" WHERE `player_id` IN (?, ?, ?)",
		[]interface{}{1, 2, 3})

	stmt, err = b.SelectByPKIn([]interface{}{9, 10}, QueryOptions{OrderBy: "`id` ASC"})
	checkStmt(t, stmt, err,
		testSelectAll+" WHERE `id` IN (?, ?) ORDER BY `id` ASC",
		[]interface{}{9, 10})

	checkStmt(t, b.Count("`port` = ?", []interface{}{80}), nil,
		"SELECT COUNT(*) FROM `golang_test` WHERE `port` = ?", []interface{}{80})

	checkStmt(t, b.Exists("`ip` = ?", []interface{}{"a"}), nil,
		"SELECT 1 FROM `golang_test` WHERE `ip` = ? LIMIT 1", []interface{}{"a"})

	stmt, err = b.ExistsByPKForUpdate(msg)
	checkStmt(t, stmt, err,
		"SELECT 1 FROM `golang_test` WHERE `id` = ? LIMIT 1 FOR UPDATE", []interface{}{uint64(7)})
}

func TestSQLBuilderUpdate(t *testing.T) {
	b := newTestBuilder()
	msg := &testpb.GolangTest{Id: 7, Ip: "10.0.0.1", Port: 8080}

	stmt, err := b.UpdateByPK(msg)
	checkStmt(t, stmt, err,
		"UPDATE `golang_test` SET `id` = ?, `ip` = ?, `port` = ? WHERE `id` = ?",
		[]interface{}{"7", "10.0.0.1", "8080", uint64(7)})

	// CAS：状态符合预期才允许写；若新旧值可能相同，RowsAffected=0 不能单独证明守卫失败
	stmt, err = b.UpdateByPKIf(msg, "`group_id` = ?", []interface{}{3})
	checkStmt(t, stmt, err,
		"UPDATE `golang_test` SET `id` = ?, `ip` = ?, `port` = ? WHERE `id` = ? AND `group_id` = ?",
		[]interface{}{"7", "10.0.0.1", "8080", uint64(7), 3})

	// 指定列：即使是 proto3 零值也照写（清零场景）
	stmt, err = b.UpdateFieldsByPK(msg, "port", "group_id")
	checkStmt(t, stmt, err,
		"UPDATE `golang_test` SET `port` = ?, `group_id` = ? WHERE `id` = ?",
		[]interface{}{"8080", "0", uint64(7)})

	stmt, err = b.UpdateAssignsByPK(msg, AddCol("port", 1), SetColExpr("ip", "CONCAT(`ip`, ?)", "-x"))
	checkStmt(t, stmt, err,
		"UPDATE `golang_test` SET `port` = `port` + ?, `ip` = CONCAT(`ip`, ?) WHERE `id` = ?",
		[]interface{}{1, "-x", uint64(7)})

	stmt, err = b.IncrByPK(msg, "port", 5)
	checkStmt(t, stmt, err,
		"UPDATE `golang_test` SET `port` = `port` + ? WHERE `id` = ?",
		[]interface{}{int64(5), uint64(7)})

	// 扣减守卫：够才扣，不会扣成负数
	stmt, err = b.DecrByPKIfEnough(msg, "port", 5)
	checkStmt(t, stmt, err,
		"UPDATE `golang_test` SET `port` = `port` - ? WHERE `id` = ? AND `port` >= ?",
		[]interface{}{int64(5), uint64(7), int64(5)})

	// 负数扣减会让守卫恒真、扣减变累加，必须拒绝
	if _, err := b.DecrByPKIfEnough(msg, "port", -1); err == nil {
		t.Error("负数 delta 应报错")
	}

	stmt, err = b.UpdateAssignsWhere([]Assign{SubCol("port", 2)}, "`group_id` = ? AND `port` >= ?", []interface{}{1, 2})
	checkStmt(t, stmt, err,
		"UPDATE `golang_test` SET `port` = `port` - ? WHERE `group_id` = ? AND `port` >= ?",
		[]interface{}{2, 1, 2})

	stmt, err = b.UpdateWhere(msg, "`group_id` = ?", []interface{}{1})
	checkStmt(t, stmt, err,
		"UPDATE `golang_test` SET `id` = ?, `ip` = ?, `port` = ? WHERE `group_id` = ?",
		[]interface{}{"7", "10.0.0.1", "8080", 1})
}

func TestSQLBuilderDelete(t *testing.T) {
	b := newTestBuilder()
	msg := &testpb.GolangTest{Id: 7}

	stmt, err := b.DeleteByPK(msg)
	checkStmt(t, stmt, err, "DELETE FROM `golang_test` WHERE `id` = ?", []interface{}{uint64(7)})

	stmt, err = b.DeleteByPKIf(msg, "`group_id` = ?", []interface{}{3})
	checkStmt(t, stmt, err,
		"DELETE FROM `golang_test` WHERE `id` = ? AND `group_id` = ?",
		[]interface{}{uint64(7), 3})

	stmt, err = b.DeleteWhere("`port` = ?", []interface{}{80})
	checkStmt(t, stmt, err,
		"DELETE FROM `golang_test` WHERE `port` = ?", []interface{}{80})

	// 保留期清理：小批量有界删除
	stmt, err = b.DeleteWhereLimit("`player_id` < ?", []interface{}{100}, "`id` ASC", 500)
	checkStmt(t, stmt, err,
		"DELETE FROM `golang_test` WHERE `player_id` < ? ORDER BY `id` ASC LIMIT 500",
		[]interface{}{100})

	stmt, err = b.DeleteWhereLimit("`player_id` < ?", []interface{}{100}, "", 500)
	checkStmt(t, stmt, err,
		"DELETE FROM `golang_test` WHERE `player_id` < ? LIMIT 500",
		[]interface{}{100})

	stmt, err = b.DeleteByPKIn([]interface{}{1, 2})
	checkStmt(t, stmt, err, "DELETE FROM `golang_test` WHERE `id` IN (?, ?)", []interface{}{1, 2})
}

func TestSQLBuilderErrors(t *testing.T) {
	b := newTestBuilder()
	msg := &testpb.GolangTest{Id: 7}

	if _, err := b.Upsert(msg, "no_such_col"); !errors.Is(err, ErrFieldNotFound) {
		t.Errorf("未知列应返回ErrFieldNotFound，实际: %v", err)
	}
	if _, err := b.SelectColumns([]string{"no_such_col"}, "1=1", nil, QueryOptions{}); !errors.Is(err, ErrFieldNotFound) {
		t.Errorf("未知列应返回ErrFieldNotFound，实际: %v", err)
	}
	if _, err := b.SelectByKVIn("ip", nil, QueryOptions{}); !errors.Is(err, ErrEmptyValues) {
		t.Errorf("空取值集合应返回ErrEmptyValues，实际: %v", err)
	}
	if _, err := b.UpdateAssignsByPK(msg); !errors.Is(err, ErrNoAssigns) {
		t.Errorf("无赋值子句应返回ErrNoAssigns，实际: %v", err)
	}
	if _, err := b.UpdateFieldsByPK(msg); !errors.Is(err, ErrNoAssigns) {
		t.Errorf("未指定列应返回ErrNoAssigns，实际: %v", err)
	}
	if _, err := b.DeleteWhereLimit("1=1", nil, "", 0); err == nil {
		t.Error("limit<=0 应报错")
	}
	if _, err := b.DecrByPKIfEnough(msg, "port", 0); err == nil {
		t.Error("delta=0 应报错，避免 RowsAffected 语义含糊")
	}

	// UPDATE/DELETE 不接受空条件：确需整表操作必须显式写 "1=1"，让危险意图在评审里可见
	if _, err := b.DeleteWhere("", nil); !errors.Is(err, ErrEmptyWhereClause) {
		t.Errorf("DeleteWhere 空条件应返回 ErrEmptyWhereClause，实际: %v", err)
	}
	if _, err := b.DeleteWhere("   ", nil); !errors.Is(err, ErrEmptyWhereClause) {
		t.Errorf("DeleteWhere 全空白条件应返回 ErrEmptyWhereClause，实际: %v", err)
	}
	if _, err := b.DeleteWhereLimit("", nil, "", 10); !errors.Is(err, ErrEmptyWhereClause) {
		t.Errorf("DeleteWhereLimit 空条件应返回 ErrEmptyWhereClause，实际: %v", err)
	}
	if _, err := b.UpdateWhere(msg, "", nil); !errors.Is(err, ErrEmptyWhereClause) {
		t.Errorf("UpdateWhere 空条件应返回 ErrEmptyWhereClause，实际: %v", err)
	}
	if _, err := b.UpdateAssignsWhere([]Assign{AddCol("port", 1)}, "", nil); !errors.Is(err, ErrEmptyWhereClause) {
		t.Errorf("UpdateAssignsWhere 空条件应返回 ErrEmptyWhereClause，实际: %v", err)
	}
	// 显式 "1=1" 应放行
	if _, err := b.DeleteWhere("1=1", nil); err != nil {
		t.Errorf("显式 1=1 应放行，实际: %v", err)
	}

	// 未声明主键的消息：按主键的语句必须明确失败，而不是产出无 WHERE 的全表语句
	noPK := NewSQLBuilder(&testpb.Player{})
	if _, err := noPK.SelectByPK(&testpb.Player{PlayerId: 1}); !errors.Is(err, ErrPrimaryKeyNotFound) {
		t.Errorf("无主键应返回ErrPrimaryKeyNotFound，实际: %v", err)
	}
	if _, err := noPK.DeleteByPKIn([]interface{}{1}); !errors.Is(err, ErrPrimaryKeyNotFound) {
		t.Errorf("无主键应返回ErrPrimaryKeyNotFound，实际: %v", err)
	}
	if _, err := noPK.UpsertKeepOld(&testpb.Player{PlayerId: 1}); !errors.Is(err, ErrPrimaryKeyNotFound) {
		t.Errorf("无主键应返回ErrPrimaryKeyNotFound，实际: %v", err)
	}

	// 描述符不匹配：不能拿别的消息往这张表上塞
	if _, err := b.Insert(&testpb.GolangTest1{Id: 1}); err == nil {
		t.Error("消息类型不匹配应报错")
	}
	wrong := &testpb.GolangTest1{Id: 1}
	descriptorChecks := []struct {
		name string
		call func() error
	}{
		{"replace", func() error { _, err := b.Replace(wrong); return err }},
		{"update by pk", func() error { _, err := b.UpdateByPK(wrong); return err }},
		{"update by pk if", func() error { _, err := b.UpdateByPKIf(wrong, "1=1", nil); return err }},
		{"update fields by pk", func() error { _, err := b.UpdateFieldsByPK(wrong, "port"); return err }},
		{"update where", func() error { _, err := b.UpdateWhere(wrong, "1=1", nil); return err }},
	}
	for _, tc := range descriptorChecks {
		t.Run(tc.name+" rejects mismatched descriptor", func(t *testing.T) {
			if err := tc.call(); err == nil {
				t.Error("消息类型不匹配应返回错误")
			}
		})
	}
}

// TestSQLBuilderTableOptionOverride 代码传入的 TableOption 覆盖 proto 里的声明
func TestSQLBuilderTableOptionOverride(t *testing.T) {
	b := NewSQLBuilder(&testpb.GolangTest{}, WithTableName("shard_1"), WithPrimaryKey("player_id"))
	if b.TableName() != "shard_1" {
		t.Fatalf("表名覆盖失败: %s", b.TableName())
	}
	stmt, err := b.SelectByPK(&testpb.GolangTest{PlayerId: 42})
	checkStmt(t, stmt, err,
		"SELECT "+testAllCols+" FROM `shard_1` WHERE `player_id` = ?",
		[]interface{}{uint64(42)})
}

// TestDBSQLBuilder DB 上的入口复用已注册表的配置，且不需要连库
func TestDBSQLBuilder(t *testing.T) {
	pdb := NewDB()
	pdb.RegisterTable(&testpb.GolangTest{})

	b, err := pdb.SQLBuilder(&testpb.GolangTest{})
	if err != nil {
		t.Fatalf("获取SQLBuilder失败: %v", err)
	}
	stmt, err := b.SelectByPK(&testpb.GolangTest{Id: 1})
	checkStmt(t, stmt, err, testSelectAll+" WHERE `id` = ?", []interface{}{uint64(1)})

	if _, err := pdb.SQLBuilder(&testpb.GolangTest2{}); !errors.Is(err, ErrTableNotFound) {
		t.Errorf("未注册表应返回ErrTableNotFound，实际: %v", err)
	}
}

// TestSQLBuilderArgsNotAliased 生成的参数切片互不共享底层数组，
// 防止两条语句的参数互相踩踏
func TestSQLBuilderArgsNotAliased(t *testing.T) {
	b := newTestBuilder()
	msg := &testpb.GolangTest{Id: 7, Ip: "a"}

	first, err := b.Upsert(msg, "ip")
	if err != nil {
		t.Fatalf("生成失败: %v", err)
	}
	second, err := b.UpsertAdd(msg, "port")
	if err != nil {
		t.Fatalf("生成失败: %v", err)
	}
	second.Args[0] = "changed"
	if first.Args[0] != "7" {
		t.Errorf("参数切片被复用污染: %v", first.Args)
	}
}
