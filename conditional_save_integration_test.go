package proto2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
)

// conditionalSaveRetryable 调用方对哪些错误应整体重试这一次 SaveConditional：
// 库报的并发窗口，以及服务端的写冲突（TiDB 乐观 autocommit 的 9007、死锁 1213、TiDB 8002）。
func conditionalSaveRetryable(err error) bool {
	if errors.Is(err, ErrConditionalSaveRace) {
		return true
	}
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && (mysqlErr.Number == 9007 || mysqlErr.Number == 1213 || mysqlErr.Number == 8002)
}

// SaveConditional 在真 MySQL / TiDB 上的端到端行为（PROTO2MYSQL_INTEGRATION=1 才跑）。
// 栅栏是 (group_id, player_id)；port 用来看整行是否被某个版本写入。

func openConditionalSaveTable(t *testing.T, table string) (*DB, *sql.DB) {
	t.Helper()
	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	t.Cleanup(func() { closeTestDB(t, db) })
	if _, err := db.Exec("DROP TABLE IF EXISTS `" + table + "`"); err != nil {
		t.Fatalf("清理表 %s: %v", table, err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS `" + table + "`") })

	pdb.RegisterTable(&testpb.GolangTest{},
		WithTableName(table), WithPrimaryKey("id"), WithUniqueKey("ip"),
		WithAutoIncrementKey("")) // 自增会让「插入恰好一次」的断言失去意义，关掉
	if err := pdb.SyncAllTables(); err != nil {
		t.Fatalf("建表 %s: %v", table, err)
	}
	return pdb, db
}

type conditionalRow struct {
	port, groupID uint32
	playerID      uint64
}

func readConditionalRow(t *testing.T, db *sql.DB, table string, id uint32) (conditionalRow, bool) {
	t.Helper()
	var row conditionalRow
	err := db.QueryRow("SELECT `port`, `group_id`, `player_id` FROM `"+table+"` WHERE `id` = ?", id).
		Scan(&row.port, &row.groupID, &row.playerID)
	if errors.Is(err, sql.ErrNoRows) {
		return conditionalRow{}, false
	}
	if err != nil {
		t.Fatalf("回读 id=%d: %v", id, err)
	}
	return row, true
}

func TestSaveConditionalRealDatabaseVersionSequence(t *testing.T) {
	const table = "cond_save_sequence_probe"
	pdb, db := openConditionalSaveTable(t, table)
	strict := SaveOptions{FenceFields: []string{"group_id", "player_id"}}
	allowEqual := SaveOptions{FenceFields: []string{"group_id", "player_id"}, AllowEqual: true}
	msg := func(port, group uint32, player uint64) *testpb.GolangTest {
		return &testpb.GolangTest{Id: 1, Ip: "seq", Port: port, GroupId: group, PlayerId: player,
			Player: &testpb.Player{PlayerId: player, Name: fmt.Sprintf("v%d", port)}}
	}
	// 「条件成立但整行没变」只有在 clientFoundRows=false（go-sql-driver 默认）时才报 Unchanged；
	// DSN 里开了 clientFoundRows 时 UPDATE 返回匹配行数 1，结局是 Updated，正确性不受影响。
	sameRowOutcome, sameRowStored := SaveUnchanged, []uint64{2, 0}
	if cfg := GetMysqlConfig(); cfg != nil && cfg.ClientFoundRows {
		sameRowOutcome, sameRowStored = SaveUpdated, nil
	}
	steps := []struct {
		name       string
		message    *testpb.GolangTest
		opts       SaveOptions
		want       SaveOutcome
		wantStored []uint64
		wantRow    conditionalRow
	}{
		{"首次写入", msg(10, 1, 1), strict, SaveInserted, nil, conditionalRow{10, 1, 1}},
		{"同 epoch 更大 seq", msg(11, 1, 2), strict, SaveUpdated, nil, conditionalRow{11, 1, 2}},
		{"旧 seq 被拒", msg(99, 1, 1), strict, SaveRejected, []uint64{1, 2}, conditionalRow{11, 1, 2}},
		{"同版本重放被拒", msg(11, 1, 2), strict, SaveRejected, []uint64{1, 2}, conditionalRow{11, 1, 2}},
		{"新 epoch 从 seq 0 开始", msg(12, 2, 0), strict, SaveUpdated, nil, conditionalRow{12, 2, 0}},
		{"旧 epoch 的大 seq 被拒", msg(98, 1, 999), strict, SaveRejected, []uint64{2, 0}, conditionalRow{12, 2, 0}},
		{"允许相等且整行相同", msg(12, 2, 0), allowEqual, sameRowOutcome, sameRowStored, conditionalRow{12, 2, 0}},
		{"允许相等且内容不同", msg(13, 2, 0), allowEqual, SaveUpdated, nil, conditionalRow{13, 2, 0}},
		{"2^53 处相邻的 seq 仍可区分（参数不按 DOUBLE 比较）", msg(14, 3, 1<<53), strict, SaveUpdated, nil, conditionalRow{14, 3, 1 << 53}},
		{"2^53+1 比 2^53 新", msg(15, 3, 1<<53+1), strict, SaveUpdated, nil, conditionalRow{15, 3, 1<<53 + 1}},
		{"MaxUint64-1", msg(16, 3, math.MaxUint64-1), strict, SaveUpdated, nil, conditionalRow{16, 3, math.MaxUint64 - 1}},
		{"MaxUint64", msg(17, 3, math.MaxUint64), strict, SaveUpdated, nil, conditionalRow{17, 3, math.MaxUint64}},
		{"MaxUint64-1 回读被拒", msg(97, 3, math.MaxUint64-1), strict, SaveRejected, []uint64{3, math.MaxUint64}, conditionalRow{17, 3, math.MaxUint64}},
	}
	for _, step := range steps {
		result, err := pdb.SaveConditional(step.message, step.opts)
		if err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		if result.Outcome != step.want || fmt.Sprint(result.Stored) != fmt.Sprint(step.wantStored) {
			t.Fatalf("%s: 结果 %+v，期望 %s Stored=%v", step.name, result, step.want, step.wantStored)
		}
		row, ok := readConditionalRow(t, db, table, 1)
		if !ok || row != step.wantRow {
			t.Fatalf("%s: 库内 %+v (存在=%v)，期望 %+v", step.name, row, ok, step.wantRow)
		}
	}

	// 不认识栅栏的旧写入方：保留栅栏列，只写业务列。栅栏不能被清零。
	legacy := msg(50, 0, 0)
	result, err := pdb.SaveConditional(legacy, SaveOptions{PreserveFields: []string{"group_id", "player_id"}})
	if err != nil || result.Outcome != SaveUpdated {
		t.Fatalf("保留栅栏列的旧写入: %+v, %v", result, err)
	}
	if row, _ := readConditionalRow(t, db, table, 1); row != (conditionalRow{50, 3, math.MaxUint64}) {
		t.Fatalf("保留列被改写了: %+v", row)
	}
}

func TestSaveConditionalRealDatabaseAlternateUniqueConflict(t *testing.T) {
	const table = "cond_save_unique_probe"
	pdb, db := openConditionalSaveTable(t, table)
	fence := SaveOptions{FenceFields: []string{"group_id", "player_id"}}
	if _, err := pdb.SaveConditional(&testpb.GolangTest{Id: 1, Ip: "taken", Port: 1, GroupId: 1, PlayerId: 1}, fence); err != nil {
		t.Fatalf("基准行: %v", err)
	}
	result, err := pdb.SaveConditional(&testpb.GolangTest{Id: 2, Ip: "taken", Port: 2, GroupId: 9, PlayerId: 9}, fence)
	if !errors.Is(err, ErrDuplicateKey) || result.Outcome != SaveOutcomeUnknown {
		t.Fatalf("撞备用唯一键应返回 ErrDuplicateKey: %+v, %v", result, err)
	}
	if row, ok := readConditionalRow(t, db, table, 1); !ok || row != (conditionalRow{1, 1, 1}) {
		t.Fatalf("被撞的那一行不得改动: %+v", row)
	}
	if _, ok := readConditionalRow(t, db, table, 2); ok {
		t.Fatalf("冲突的 id=2 不得插入")
	}
}

// 20 个写者乱序并发写同一主键，终态必须是字典序最大的那一份完整行，且只插入一次。
// 调用方对 ErrConditionalSaveRace 的正确处理是重试，这里照做。
func TestSaveConditionalRealDatabaseConcurrentWritersConvergeToNewest(t *testing.T) {
	for _, preexisting := range []bool{false, true} {
		t.Run(fmt.Sprintf("preexisting=%v", preexisting), func(t *testing.T) {
			const table = "cond_save_concurrency_probe"
			pdb, db := openConditionalSaveTable(t, table)
			fence := SaveOptions{FenceFields: []string{"group_id", "player_id"}}
			if preexisting {
				if result, err := pdb.SaveConditional(&testpb.GolangTest{Id: 1, Ip: "c", GroupId: 1, PlayerId: 0, Port: 0}, SaveOptions{
					FenceFields: fence.FenceFields, AllowEqual: true}); err != nil || result.Outcome != SaveInserted {
					t.Fatalf("基准行: %+v, %v", result, err)
				}
			}

			const writers = 20
			order := rand.New(rand.NewSource(20261013)).Perm(writers)
			start := make(chan struct{})
			var wg sync.WaitGroup
			var mu sync.Mutex
			outcomes := make(map[SaveOutcome]int)
			var failures []error
			for _, k := range order {
				seq := uint64(k + 1)
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					message := &testpb.GolangTest{Id: 1, Ip: "c", Port: uint32(seq), GroupId: 1, PlayerId: seq}
					var result ConditionalSaveResult
					var err error
					for attempt := 0; attempt < 5; attempt++ {
						result, err = pdb.SaveConditional(message, fence)
						if !conditionalSaveRetryable(err) {
							break
						}
					}
					mu.Lock()
					defer mu.Unlock()
					if err != nil {
						failures = append(failures, fmt.Errorf("seq=%d: %w", seq, err))
						return
					}
					outcomes[result.Outcome]++
				}()
			}
			close(start)
			wg.Wait()

			if len(failures) > 0 {
				t.Fatalf("并发写出现错误: %v", failures)
			}
			wantInserted := 1
			if preexisting {
				wantInserted = 0
			}
			if outcomes[SaveInserted] != wantInserted {
				t.Fatalf("插入次数 = %d，期望 %d；结局分布 %v", outcomes[SaveInserted], wantInserted, outcomes)
			}
			total := 0
			for _, n := range outcomes {
				total += n
			}
			if total != writers {
				t.Fatalf("结局总数 = %d，期望 %d: %v", total, writers, outcomes)
			}
			if row, ok := readConditionalRow(t, db, table, 1); !ok || row != (conditionalRow{writers, 1, writers}) {
				t.Fatalf("终态 %+v，期望最新版本 {port:%d group:1 player:%d}；结局分布 %v", row, writers, writers, outcomes)
			}
		})
	}
}

func TestSaveConditionalRealDatabaseInTransaction(t *testing.T) {
	const table = "cond_save_tx_probe"
	pdb, db := openConditionalSaveTable(t, table)
	fence := SaveOptions{FenceFields: []string{"group_id", "player_id"}}
	if _, err := pdb.SaveConditional(&testpb.GolangTest{Id: 1, Ip: "tx", Port: 1, GroupId: 1, PlayerId: 5}, fence); err != nil {
		t.Fatalf("基准行: %v", err)
	}
	var rejected, updated ConditionalSaveResult
	err := pdb.RunInTransaction(func(tx *DB) error {
		var err error
		if rejected, err = tx.SaveConditional(&testpb.GolangTest{Id: 1, Ip: "tx", Port: 2, GroupId: 1, PlayerId: 4}, fence); err != nil {
			return err
		}
		updated, err = tx.SaveConditional(&testpb.GolangTest{Id: 1, Ip: "tx", Port: 3, GroupId: 1, PlayerId: 6}, fence)
		return err
	})
	if err != nil {
		t.Fatalf("事务: %v", err)
	}
	if rejected.Outcome != SaveRejected || updated.Outcome != SaveUpdated {
		t.Fatalf("事务内结局 rejected=%+v updated=%+v", rejected, updated)
	}
	if row, _ := readConditionalRow(t, db, table, 1); row != (conditionalRow{3, 1, 6}) {
		t.Fatalf("提交后库内 %+v", row)
	}
}
