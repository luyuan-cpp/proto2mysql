package proto2mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// SaveConditional 的单测：用 gorm_audit_fixes_test.go 的审计假驱动，逐个结局断言
// 「发了哪几条语句、带什么参数」，core 与 gorm 两个后端各跑一遍。真库行为见
// conditional_save_integration_test.go。

type conditionalSaver interface {
	SaveConditional(proto.Message, SaveOptions) (ConditionalSaveResult, error)
}

// forEachConditionalBackend 为每个后端新建一套假连接，表注册与 newCoreAuditDB 一致（主键 id、唯一键 ip）。
func forEachConditionalBackend(t *testing.T, body func(t *testing.T, saver conditionalSaver, conn *gormAuditConn)) {
	t.Helper()
	for _, backend := range []string{"core", "gorm"} {
		t.Run(backend, func(t *testing.T) {
			if backend == "core" {
				pdb, conn := newCoreAuditDB(t)
				body(t, pdb, conn)
				return
			}
			gdb, conn := newGormAuditDB(t)
			gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))
			body(t, gdb, conn)
		})
	}
}

var conditionalFenceColumns = []string{"group_id", "player_id"}

func conditionalFence() SaveOptions {
	return SaveOptions{FenceFields: []string{"group_id", "player_id"}}
}

// conditionalMsg 的栅栏是 (group_id, player_id) = (5, 9)。
func conditionalMsg() *testpb.GolangTest {
	return &testpb.GolangTest{Id: 2, Ip: "a", Port: 7, GroupId: 5, PlayerId: 9}
}

func fenceRow(values ...driver.Value) gormAuditQuery {
	return gormAuditQuery{columns: conditionalFenceColumns, rows: [][]driver.Value{values}}
}

func noFenceRow() gormAuditQuery { return gormAuditQuery{columns: conditionalFenceColumns} }

func existsRow() gormAuditQuery {
	return gormAuditQuery{columns: []string{"1"}, rows: [][]driver.Value{{int64(1)}}}
}

func noMatchRow() gormAuditQuery { return gormAuditQuery{columns: []string{"1"}} }

func duplicateKeyErr() error {
	return &mysqldriver.MySQLError{Number: 1062, Message: "Duplicate entry for key 'PRIMARY'"}
}

func requireSQLCount(t *testing.T, conn *gormAuditConn, want int) {
	t.Helper()
	if got := conn.sqls(); len(got) != want {
		t.Fatalf("期望发出 %d 条语句，实际 %d 条: %v", want, len(got), got)
	}
}

func TestFencePredicateGolden(t *testing.T) {
	fields := (&testpb.GolangTest{}).ProtoReflect().Descriptor().Fields()
	groupID, playerID, port := fields.ByName("group_id"), fields.ByName("player_id"), fields.ByName("port")
	cases := []struct {
		name       string
		fields     []protoreflect.FieldDescriptor
		values     []uint64
		allowEqual bool
		wantSQL    string
		wantArgs   []interface{}
	}{
		{
			name: "一列严格", fields: []protoreflect.FieldDescriptor{groupID}, values: []uint64{5},
			wantSQL: "(`group_id` < ?)", wantArgs: []interface{}{uint64(5)},
		},
		{
			name: "两列严格", fields: []protoreflect.FieldDescriptor{groupID, playerID}, values: []uint64{5, 9},
			wantSQL:  "(`group_id` < ? OR (`group_id` = ? AND (`player_id` < ?)))",
			wantArgs: []interface{}{uint64(5), uint64(5), uint64(9)},
		},
		{
			name: "三列允许相等", fields: []protoreflect.FieldDescriptor{groupID, playerID, port}, values: []uint64{5, 9, math.MaxUint64},
			allowEqual: true,
			wantSQL:    "(`group_id` < ? OR (`group_id` = ? AND (`player_id` < ? OR (`player_id` = ? AND (`port` <= ?)))))",
			wantArgs:   []interface{}{uint64(5), uint64(5), uint64(9), uint64(9), uint64(math.MaxUint64)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotSQL, gotArgs := fencePredicate(tc.fields, tc.values, tc.allowEqual)
			if gotSQL != tc.wantSQL {
				t.Errorf("谓词\n got %s\nwant %s", gotSQL, tc.wantSQL)
			}
			// DeepEqual 区分 uint64 与 int/字符串：栅栏参数必须以 uint64 绑定，否则 MySQL 按 DOUBLE 比较会丢精度。
			if !reflect.DeepEqual(gotArgs, tc.wantArgs) {
				t.Errorf("参数\n got %#v\nwant %#v", gotArgs, tc.wantArgs)
			}
		})
	}
}

func TestSaveConditionalUpdatedInOneStatement(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		conn.queueExec(gormAuditExec{affected: 1})
		result, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		if err != nil {
			t.Fatalf("SaveConditional: %v", err)
		}
		if result.Outcome != SaveUpdated || result.Stored != nil || !result.Applied() {
			t.Fatalf("结果 = %+v，期望 SaveUpdated 且 Stored 为 nil", result)
		}
		requireSQLCount(t, conn, 1)
		update := conn.sqls()[0]
		const wantWhere = "WHERE `id` = ? AND (`group_id` < ? OR (`group_id` = ? AND (`player_id` < ?)))"
		if !strings.HasPrefix(update, "UPDATE ") || !strings.Contains(update, wantWhere) {
			t.Fatalf("条件 UPDATE 形状不对: %s", update)
		}
		for _, column := range []string{"`ip` = ?", "`port` = ?", "`group_id` = ?", "`player` = ?", "`player_id` = ?"} {
			if !strings.Contains(update, column) {
				t.Errorf("整行语义要求写全部非主键列，缺少 %s: %s", column, update)
			}
		}
		args := conn.argsAt(0)
		if len(args) != 9 { // 5 个非主键列 + 1 个主键 + 3 个栅栏参数
			t.Fatalf("参数个数 = %d，期望 9: %#v", len(args), args)
		}
		if want := []driver.Value{uint64(5), uint64(5), uint64(9)}; !reflect.DeepEqual(args[6:], want) {
			t.Fatalf("栅栏参数必须以 uint64 到达驱动，got %#v want %#v", args[6:], want)
		}
	})
}

func TestSaveConditionalRejectedReadsOnlyFenceColumns(t *testing.T) {
	cases := []struct {
		name   string
		stored []driver.Value
		want   []uint64
	}{
		{name: "库内更新", stored: []driver.Value{int64(5), int64(10)}, want: []uint64{5, 10}},
		{name: "同版本重放", stored: []driver.Value{int64(5), int64(9)}, want: []uint64{5, 9}},
		{name: "第一位更大", stored: []driver.Value{int64(6), int64(0)}, want: []uint64{6, 0}},
		// go-sql-driver 文本协议（InterpolateParams=true）对 BIGINT UNSIGNED 直接给 uint64。
		{name: "文本协议原样返回 uint64", stored: []driver.Value{uint64(math.MaxUint64), int64(0)}, want: []uint64{math.MaxUint64, 0}},
		// 二进制协议（预处理语句）里大于 MaxInt64 的无符号值以十进制字节串返回。
		{name: "二进制协议：超过 MaxInt64 的字节串", stored: []driver.Value{[]byte("18446744073709551615"), int64(0)}, want: []uint64{math.MaxUint64, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
				conn.queueExec(gormAuditExec{affected: 0})
				conn.queueQuery(fenceRow(tc.stored...))
				result, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
				if err != nil {
					t.Fatalf("SaveConditional: %v", err)
				}
				if result.Outcome != SaveRejected || result.Applied() || !reflect.DeepEqual(result.Stored, tc.want) {
					t.Fatalf("结果 = %+v，期望 SaveRejected、Stored=%v", result, tc.want)
				}
				requireSQLCount(t, conn, 2)
				state := conn.sqls()[1]
				if !strings.HasPrefix(state, "SELECT `group_id`, `player_id` FROM ") || !strings.Contains(state, "WHERE `id` = ?") {
					t.Fatalf("状态查询只应取栅栏列: %s", state)
				}
				// autocommit 下也必须 FOR UPDATE：强制 current read，并让读写分离层留在主库。
				if !strings.HasSuffix(state, " FOR UPDATE") {
					t.Fatalf("状态查询必须带 FOR UPDATE: %s", state)
				}
				if conn.countSQLPrefix("INSERT") != 0 {
					t.Fatalf("被拒绝时不得尝试 INSERT: %v", conn.sqls())
				}
			})
		})
	}
}

func TestSaveConditionalInsertsMissingRow(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		conn.queueExec(gormAuditExec{affected: 0}) // 条件 UPDATE；随后的 INSERT 取默认的成功
		conn.queueQuery(noFenceRow())
		result, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		if err != nil {
			t.Fatalf("SaveConditional: %v", err)
		}
		if result.Outcome != SaveInserted || !result.Applied() {
			t.Fatalf("结果 = %+v，期望 SaveInserted", result)
		}
		requireSQLCount(t, conn, 3)
		if !strings.HasPrefix(conn.sqls()[2], "INSERT") {
			t.Fatalf("第三条应为 INSERT: %v", conn.sqls())
		}
	})
}

func TestSaveConditionalInsertRaceThenRejected(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{err: duplicateKeyErr()})
		conn.queueQuery(noFenceRow(), fenceRow(int64(6), int64(0)))
		result, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		if err != nil {
			t.Fatalf("并发写者先插入了更新的版本，应判为被拒绝而不是报错: %v", err)
		}
		if result.Outcome != SaveRejected || !reflect.DeepEqual(result.Stored, []uint64{6, 0}) {
			t.Fatalf("结果 = %+v", result)
		}
		requireSQLCount(t, conn, 4)
	})
}

func TestSaveConditionalAlternateUniqueConflict(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{err: duplicateKeyErr()})
		conn.queueQuery(noFenceRow(), noFenceRow())
		result, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		if !errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("按主键查不到行却撞 1062，应返回 ErrDuplicateKey，实际 %v", err)
		}
		if result.Outcome != SaveOutcomeUnknown {
			t.Fatalf("出错时 Outcome 必须是 Unknown: %+v", result)
		}
	})
}

func TestSaveConditionalInsertErrorPassesThrough(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{err: &mysqldriver.MySQLError{Number: 1406, Message: "data too long"}})
		conn.queueQuery(noFenceRow())
		_, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		var mysqlErr *mysqldriver.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1406 || errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("非 1062 的 INSERT 错误必须原样返回，实际 %v", err)
		}
		requireSQLCount(t, conn, 3)
	})
}

func TestSaveConditionalAllowEqualUnchanged(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		opts := conditionalFence()
		opts.AllowEqual = true
		conn.queueExec(gormAuditExec{affected: 0})
		conn.queueQuery(fenceRow(int64(5), int64(9)), existsRow())
		result, err := saver.SaveConditional(conditionalMsg(), opts)
		if err != nil {
			t.Fatalf("SaveConditional: %v", err)
		}
		if result.Outcome != SaveUnchanged || !result.Applied() || !reflect.DeepEqual(result.Stored, []uint64{5, 9}) {
			t.Fatalf("结果 = %+v，期望 SaveUnchanged", result)
		}
		requireSQLCount(t, conn, 3)
		if !strings.Contains(conn.sqls()[0], "(`player_id` <= ?)") {
			t.Fatalf("AllowEqual 的最后一位应为 <=: %s", conn.sqls()[0])
		}
		match := conn.sqls()[2]
		for _, part := range []string{"SELECT 1 FROM ", "`id` = ?", "CAST(`ip` AS BINARY) <=> CAST(? AS BINARY)", "`group_id` <=> ?", "`player_id` <=> ?"} {
			if !strings.Contains(match, part) {
				t.Errorf("目标值比较查询缺少 %q: %s", part, match)
			}
		}
		if !strings.HasSuffix(match, " FOR UPDATE") {
			t.Fatalf("比较查询必须带 FOR UPDATE: %s", match)
		}
		// 参数：[id, ip, port, group_id, player, player_id]，数值列必须以 uint64 到达驱动
		// （以十进制字符串下发会让 MySQL 按 DOUBLE 比较）。
		args := conn.argsAt(2)
		if len(args) != 6 {
			t.Fatalf("比较查询参数个数 = %d，期望 6: %#v", len(args), args)
		}
		for i, want := range map[int]driver.Value{0: uint64(2), 2: uint64(7), 3: uint64(5), 5: uint64(9)} {
			if !reflect.DeepEqual(args[i], want) {
				t.Errorf("比较查询第 %d 个参数 = %#v，期望 %#v", i, args[i], want)
			}
		}
	})
}

func TestSaveConditionalAllowEqualRejectsNewer(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		opts := conditionalFence()
		opts.AllowEqual = true
		conn.queueExec(gormAuditExec{affected: 0})
		conn.queueQuery(fenceRow(int64(5), int64(10)))
		result, err := saver.SaveConditional(conditionalMsg(), opts)
		if err != nil || result.Outcome != SaveRejected {
			t.Fatalf("库内更新时 AllowEqual 也必须拒绝: %+v, %v", result, err)
		}
		requireSQLCount(t, conn, 2)
	})
}

func TestSaveConditionalRaceAfterTwoRounds(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		// 严格模式：两轮读到的库内栅栏都比消息旧（条件成立），但条件 UPDATE 两次都没改到行，
		// 且两次状态不同——期间有并发写者在改这一行。
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{affected: 0})
		conn.queueQuery(fenceRow(int64(4), int64(0)), fenceRow(int64(4), int64(1)))
		result, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		if !errors.Is(err, ErrConditionalSaveRace) || errors.Is(err, ErrDuplicateKey) || errors.Is(err, ErrSaveMatchMismatch) {
			t.Fatalf("期望只命中 ErrConditionalSaveRace，实际 %v", err)
		}
		if result.Outcome != SaveOutcomeUnknown || !reflect.DeepEqual(result.Stored, []uint64{4, 1}) {
			t.Fatalf("Race 时 Stored 应为最后一次读到的值: %+v", result)
		}
		requireSQLCount(t, conn, 4) // UPDATE, SELECT, UPDATE, SELECT —— 栅栏不同，不发目标值比较
	})
}

func TestSaveConditionalStrictSameStateTwiceIsRaceNotMismatch(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		// 严格模式两轮读到同一个更旧的栅栏：没有做过整行比较，不能断言「比较规则不一致」，只能报 Race。
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{affected: 0})
		conn.queueQuery(fenceRow(int64(4), int64(0)), fenceRow(int64(4), int64(0)))
		_, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		if !errors.Is(err, ErrConditionalSaveRace) || errors.Is(err, ErrSaveMatchMismatch) {
			t.Fatalf("期望 ErrConditionalSaveRace，实际 %v", err)
		}
		requireSQLCount(t, conn, 4)
	})
}

func TestSaveConditionalInsertRaceThenSecondRoundUpdates(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		// 真实并发里最常见的一条：行不存在 → INSERT 撞 1062（别人刚插了更旧的版本）→ 重读条件成立 → 第二轮 UPDATE 改到。
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{err: duplicateKeyErr()}, gormAuditExec{affected: 1})
		conn.queueQuery(noFenceRow(), fenceRow(int64(4), int64(0)))
		result, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		if err != nil || result.Outcome != SaveUpdated {
			t.Fatalf("结果 = %+v, %v，期望 SaveUpdated", result, err)
		}
		requireSQLCount(t, conn, 5) // UPDATE, SELECT, INSERT, SELECT, UPDATE
		if n := conn.countSQLPrefix("INSERT"); n != 1 {
			t.Fatalf("INSERT 只应发一次，实际 %d: %v", n, conn.sqls())
		}
	})
}

func TestSaveConditionalRowDisappearsAfterDuplicateRetry(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		// 1062 后重读到这一行，第二轮却又没了：只能是被物理删除。不再插第二次，错误带着原始 1062。
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{err: duplicateKeyErr()}, gormAuditExec{affected: 0})
		conn.queueQuery(noFenceRow(), fenceRow(int64(4), int64(0)), noFenceRow())
		_, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		var mysqlErr *mysqldriver.MySQLError
		if !errors.Is(err, ErrDuplicateKey) || !errors.As(err, &mysqlErr) || mysqlErr.Number != 1062 {
			t.Fatalf("期望带原始 1062 的 ErrDuplicateKey，实际 %v", err)
		}
		if !strings.Contains(err.Error(), "row disappeared") {
			t.Fatalf("错误信息应说明行被删除，而不是撞到别的唯一键: %v", err)
		}
		requireSQLCount(t, conn, 6) // UPDATE, SELECT, INSERT, SELECT, UPDATE, SELECT
		if n := conn.countSQLPrefix("INSERT"); n != 1 {
			t.Fatalf("INSERT 只应发一次，实际 %d", n)
		}
	})
}

func TestSaveConditionalAllowEqualInsertRaceThenUnchanged(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		// 同版本的两个写者并发首写：一个插入成功，另一个撞 1062 后重读到相同栅栏、整行也相同 → Unchanged。
		opts := conditionalFence()
		opts.AllowEqual = true
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{err: duplicateKeyErr()})
		conn.queueQuery(noFenceRow(), fenceRow(int64(5), int64(9)), existsRow())
		result, err := saver.SaveConditional(conditionalMsg(), opts)
		if err != nil || result.Outcome != SaveUnchanged || !reflect.DeepEqual(result.Stored, []uint64{5, 9}) {
			t.Fatalf("结果 = %+v, %v，期望 SaveUnchanged", result, err)
		}
		requireSQLCount(t, conn, 5) // UPDATE, SELECT, INSERT, SELECT, 比较
	})
}

func TestSaveConditionalAllowEqualOlderStoredSkipsComparison(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		// AllowEqual 下库内栅栏严格更旧：栅栏列本身就不同，行不可能已是目标值，不发整行比较，直接下一轮。
		opts := conditionalFence()
		opts.AllowEqual = true
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{affected: 1})
		conn.queueQuery(fenceRow(int64(5), int64(8)))
		result, err := saver.SaveConditional(conditionalMsg(), opts)
		if err != nil || result.Outcome != SaveUpdated {
			t.Fatalf("结果 = %+v, %v，期望 SaveUpdated", result, err)
		}
		requireSQLCount(t, conn, 3) // UPDATE, SELECT, UPDATE —— 中间没有 SELECT 1
	})
}

func TestSaveConditionalMatchMismatchIsNotRetryable(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		opts := conditionalFence()
		opts.AllowEqual = true
		conn.queueExec(gormAuditExec{affected: 0}, gormAuditExec{affected: 0})
		conn.queueQuery(fenceRow(int64(5), int64(9)), noMatchRow(), fenceRow(int64(5), int64(9)), noMatchRow())
		_, err := saver.SaveConditional(conditionalMsg(), opts)
		if !errors.Is(err, ErrSaveMatchMismatch) || errors.Is(err, ErrConditionalSaveRace) {
			t.Fatalf("两轮读到同一状态应返回 ErrSaveMatchMismatch，实际 %v", err)
		}
		requireSQLCount(t, conn, 6)
	})
}

func TestSaveConditionalNullFenceIsSchemaDrift(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		conn.queueExec(gormAuditExec{affected: 0})
		conn.queueQuery(fenceRow(nil, int64(0)))
		_, err := saver.SaveConditional(conditionalMsg(), conditionalFence())
		if !errors.Is(err, ErrSchemaDrift) {
			t.Fatalf("库内栅栏为 NULL 必须明确报错，实际 %v", err)
		}
	})
}

func TestSaveConditionalWithoutOptionsMatchesSaveShape(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		conn.queueExec(gormAuditExec{affected: 1})
		result, err := saver.SaveConditional(conditionalMsg(), SaveOptions{})
		if err != nil || result.Outcome != SaveUpdated {
			t.Fatalf("零值选项应等同 Save: %+v, %v", result, err)
		}
		requireSQLCount(t, conn, 1)
		update := conn.sqls()[0]
		if !strings.HasSuffix(update, "WHERE `id` = ?") {
			t.Fatalf("不带栅栏时 WHERE 只有主键: %s", update)
		}
	})
}

func TestSaveConditionalPreserveFieldsKeepColumnAlignment(t *testing.T) {
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		// 保留列 port 位于非主键列的中间：比较查询的参数必须按列逐个对齐，不能错位到别的列上。
		conn.queueExec(gormAuditExec{affected: 0})
		conn.queueQuery(existsRow(), existsRow())
		result, err := saver.SaveConditional(conditionalMsg(), SaveOptions{PreserveFields: []string{"port"}})
		if err != nil || result.Outcome != SaveUnchanged || result.Stored != nil {
			t.Fatalf("结果 = %+v, %v，期望 SaveUnchanged 且无 Stored", result, err)
		}
		requireSQLCount(t, conn, 3)
		update := conn.sqls()[0]
		if strings.Contains(update, "`port`") {
			t.Fatalf("保留列不得出现在 UPDATE 里: %s", update)
		}
		if !strings.Contains(update, "SET `ip` = ?, `group_id` = ?, `player` = ?, `player_id` = ? WHERE `id` = ?") {
			t.Fatalf("UPDATE 列不对: %s", update)
		}
		if state := conn.sqls()[1]; !strings.HasPrefix(state, "SELECT 1 FROM ") || strings.Contains(state, "<=>") {
			t.Fatalf("不带栅栏时状态查询只探存在性: %s", state)
		}
		match := conn.sqls()[2]
		if strings.Contains(match, "`port`") {
			t.Fatalf("保留列不参与目标值比较: %s", match)
		}
		args := conn.argsAt(2) // [id, ip, group_id, player, player_id]
		if len(args) != 5 {
			t.Fatalf("比较查询参数个数 = %d，期望 5: %#v", len(args), args)
		}
		// 强类型比较：错位会在 group_id 位置拿到 port=7；忘了归一化会拿到字符串 "5"。
		for i, want := range map[int]driver.Value{0: uint64(2), 2: uint64(5), 4: uint64(9)} {
			if !reflect.DeepEqual(args[i], want) {
				t.Errorf("比较查询第 %d 个参数 = %#v，期望 %#v", i, args[i], want)
			}
		}
	})
}

func TestSaveConditionalInvalidOptionsSendNoSQL(t *testing.T) {
	zeroFence := conditionalMsg()
	zeroFence.GroupId, zeroFence.PlayerId = 0, 0
	cases := []struct {
		name string
		msg  *testpb.GolangTest
		opts SaveOptions
	}{
		{name: "栅栏列不存在", opts: SaveOptions{FenceFields: []string{"nope"}}},
		{name: "栅栏列是主键", opts: SaveOptions{FenceFields: []string{"id"}}},
		{name: "栅栏列是字符串", opts: SaveOptions{FenceFields: []string{"ip"}}},
		{name: "栅栏列是消息", opts: SaveOptions{FenceFields: []string{"player"}}},
		{name: "栅栏列重复", opts: SaveOptions{FenceFields: []string{"group_id", "group_id"}}},
		{name: "严格模式栅栏全 0", msg: zeroFence, opts: SaveOptions{FenceFields: []string{"group_id", "player_id"}}},
		{name: "保留列不存在", opts: SaveOptions{PreserveFields: []string{"nope"}}},
		{name: "保留列是主键", opts: SaveOptions{PreserveFields: []string{"id"}}},
		{name: "保留列与栅栏列重叠", opts: SaveOptions{FenceFields: []string{"group_id"}, PreserveFields: []string{"group_id"}}},
		{name: "保留列重复", opts: SaveOptions{PreserveFields: []string{"port", "port"}}},
		{name: "保留列覆盖全部非主键列", opts: SaveOptions{PreserveFields: []string{"ip", "port", "group_id", "player", "player_id"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
				msg := tc.msg
				if msg == nil {
					msg = conditionalMsg()
				}
				result, err := saver.SaveConditional(msg, tc.opts)
				if !errors.Is(err, ErrInvalidSaveOptions) {
					t.Fatalf("期望 ErrInvalidSaveOptions，实际 %v", err)
				}
				if result.Outcome != SaveOutcomeUnknown {
					t.Fatalf("出错时 Outcome 必须是 Unknown: %+v", result)
				}
				requireSQLCount(t, conn, 0)
			})
		})
	}
}

func TestSaveConditionalAllowEqualAcceptsZeroFence(t *testing.T) {
	// 不认识栅栏的旧写入方（栅栏全 0）用 AllowEqual：条件变成「库内栅栏也全 0」，合法。
	forEachConditionalBackend(t, func(t *testing.T, saver conditionalSaver, conn *gormAuditConn) {
		msg := conditionalMsg()
		msg.GroupId, msg.PlayerId = 0, 0
		opts := conditionalFence()
		opts.AllowEqual = true
		conn.queueExec(gormAuditExec{affected: 1})
		if result, err := saver.SaveConditional(msg, opts); err != nil || result.Outcome != SaveUpdated {
			t.Fatalf("结果 = %+v, %v", result, err)
		}
	})
}

func TestSaveConditionalNullableFenceRejected(t *testing.T) {
	conn := &gormAuditConn{}
	sqlDB := sql.OpenDB(&gormAuditDriver{conn: conn})
	t.Cleanup(func() { _ = sqlDB.Close() })
	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithNullableFields("group_id"))

	_, err := pdb.SaveConditional(conditionalMsg(), conditionalFence())
	if !errors.Is(err, ErrInvalidSaveOptions) {
		t.Fatalf("nullable 栅栏列必须拒绝，实际 %v", err)
	}
	requireSQLCount(t, conn, 0)
}

func TestSaveConditionalCanceledContextSendsNoSQL(t *testing.T) {
	t.Run("core", func(t *testing.T) {
		pdb, conn := newCoreAuditDB(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := pdb.WithContext(ctx).SaveConditional(conditionalMsg(), conditionalFence())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ctx 已取消时应返回 context.Canceled，实际 %v", err)
		}
		requireSQLCount(t, conn, 0)
	})
	t.Run("gorm", func(t *testing.T) {
		gdb, conn := newGormAuditDB(t)
		gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := gdb.WithDB(gdb.DB.WithContext(ctx)).SaveConditional(conditionalMsg(), conditionalFence())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ctx 已取消时应返回 context.Canceled，实际 %v", err)
		}
		requireSQLCount(t, conn, 0)
	})
}

func TestSaveConditionalInTransaction(t *testing.T) {
	t.Run("core", func(t *testing.T) {
		pdb, conn := newCoreAuditDB(t)
		conn.queueExec(gormAuditExec{affected: 0})
		conn.queueQuery(fenceRow(int64(5), int64(10)))
		var result ConditionalSaveResult
		err := pdb.RunInTransaction(func(tx *DB) error {
			var saveErr error
			result, saveErr = tx.SaveConditional(conditionalMsg(), conditionalFence())
			return saveErr
		})
		if err != nil || result.Outcome != SaveRejected {
			t.Fatalf("结果 = %+v, %v", result, err)
		}
		requireSQLCount(t, conn, 2)
		if state := conn.sqls()[1]; !strings.HasSuffix(state, " FOR UPDATE") {
			t.Fatalf("事务内状态查询必须是 current read: %s", state)
		}
	})
	t.Run("gorm", func(t *testing.T) {
		gdb, conn := newGormAuditDB(t)
		gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))
		conn.queueExec(gormAuditExec{affected: 0})
		conn.queueQuery(fenceRow(int64(5), int64(9)), existsRow())
		var result ConditionalSaveResult
		err := gdb.Transaction(func(tx *GormDB) error {
			opts := conditionalFence()
			opts.AllowEqual = true
			var saveErr error
			result, saveErr = tx.SaveConditional(conditionalMsg(), opts)
			return saveErr
		})
		if err != nil || result.Outcome != SaveUnchanged {
			t.Fatalf("结果 = %+v, %v", result, err)
		}
		requireSQLCount(t, conn, 3)
		for _, query := range conn.sqls()[1:3] {
			if !strings.HasSuffix(query, " FOR UPDATE") {
				t.Fatalf("事务内的状态与比较查询都必须是 current read: %s", query)
			}
		}
	})
}

func TestSaveOutcomeAppliedAndString(t *testing.T) {
	var zero ConditionalSaveResult
	if zero.Outcome != SaveOutcomeUnknown || zero.Applied() || zero.Outcome.String() != "unknown" {
		t.Fatalf("零值必须是 Unknown 且未生效: %+v", zero)
	}
	cases := []struct {
		outcome SaveOutcome
		applied bool
		text    string
	}{
		{SaveUpdated, true, "updated"},
		{SaveInserted, true, "inserted"},
		{SaveUnchanged, true, "unchanged"},
		{SaveRejected, false, "rejected"},
		{SaveOutcome(200), false, "unknown"},
	}
	for _, tc := range cases {
		result := ConditionalSaveResult{Outcome: tc.outcome}
		if result.Applied() != tc.applied || tc.outcome.String() != tc.text {
			t.Errorf("%d: Applied=%v String=%q，期望 %v %q", tc.outcome, result.Applied(), tc.outcome.String(), tc.applied, tc.text)
		}
	}
}
