package proto2mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

func TestSaveDuplicateRetryUsesCurrentReadAndTargetValues(t *testing.T) {
	backends := []string{"core", "gorm"}
	cases := []struct {
		name           string
		currentMatches bool
	}{
		{name: "same row and values", currentMatches: true},
		{name: "same primary key but different values", currentMatches: false},
	}

	for _, backend := range backends {
		for _, tc := range cases {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				var saver interface {
					Save(proto.Message) error
				}
				var conn *gormAuditConn
				if backend == "core" {
					pdb, auditConn := newCoreAuditDB(t)
					saver, conn = pdb, auditConn
				} else {
					gdb, auditConn := newGormAuditDB(t)
					gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))
					saver, conn = gdb, auditConn
				}

				conn.queueExec(
					gormAuditExec{affected: 0},
					gormAuditExec{err: &mysqldriver.MySQLError{Number: 1062, Message: "duplicate key"}},
					gormAuditExec{affected: 0},
				)
				classification := gormAuditQuery{columns: []string{"one"}}
				if tc.currentMatches {
					classification.rows = [][]driver.Value{{int64(1)}}
				}
				conn.addQueryRule("FOR UPDATE", classification)

				err := saver.Save(&testpb.GolangTest{Id: 2, Ip: "Same@example.test", Port: 7})
				if tc.currentMatches {
					if err != nil {
						t.Fatalf("当前主键行已是目标值时 Save 应成功: %v", err)
					}
				} else if !errors.Is(err, ErrDuplicateKey) {
					t.Fatalf("当前主键行的非主键值不同时必须返回 ErrDuplicateKey，实际: %v", err)
				}

				var matchSQL string
				for _, stmt := range conn.sqls() {
					if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "SELECT 1 ") {
						matchSQL = stmt
					}
				}
				if matchSQL == "" || !strings.Contains(matchSQL, "FOR UPDATE") {
					t.Fatalf("duplicate 分类必须用 current read，SQL=%v", conn.sqls())
				}
				for _, fragment := range []string{
					"`id` = ?",
					"CAST(`ip` AS BINARY) <=> CAST(? AS BINARY)",
					"`port` <=> ?",
					"CAST(`player` AS BINARY) <=> CAST(? AS BINARY)",
				} {
					if !strings.Contains(matchSQL, fragment) {
						t.Errorf("duplicate 分类必须核对目标值，缺少 %q: %s", fragment, matchSQL)
					}
				}
			})
		}
	}
}

func TestSaveCurrentRowMatchReusesExecutedUpdateValues(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	update, err := table.getSaveUpdateSQLWithArgs(&testpb.GolangTest{
		Id: 2, Ip: "Same@example.test", Port: 7, Player: &testpb.Player{Name: "alice"},
	})
	if err != nil {
		t.Fatalf("getSaveUpdateSQLWithArgs: %v", err)
	}
	match, err := table.getSaveCurrentRowMatchSQLWithArgs(update)
	if err != nil {
		t.Fatalf("getSaveCurrentRowMatchSQLWithArgs: %v", err)
	}

	// UPDATE args = [ip, port, group_id, player, player_id, id]；分类查询把它们
	// 重排成 [id, ip, port, group_id, player, player_id]。字符串/BLOB继续复用原值，
	// 数值比较参数则恢复为精确 driver 类型，避免 MySQL 先转 DOUBLE 丢失 64 位精度。
	want := []interface{}{uint64(2), update.Args[0], uint64(7), uint64(0), update.Args[3], uint64(0)}
	if len(match.Args) != len(want) {
		t.Fatalf("classification args len=%d, want %d", len(match.Args), len(want))
	}
	for i := range want {
		if match.Args[i] != want[i] {
			t.Fatalf("classification arg[%d]=%#v (%T), want %#v (%T)",
				i, match.Args[i], match.Args[i], want[i], want[i])
		}
	}
}

func newCoreAuditDB(t *testing.T) (*DB, *gormAuditConn) {
	t.Helper()
	conn := &gormAuditConn{}
	sqlDB := sql.OpenDB(&gormAuditDriver{conn: conn})
	t.Cleanup(func() { _ = sqlDB.Close() })

	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))
	return pdb, conn
}

func TestGormSaveRejectsAlternateUniqueConflict(t *testing.T) {
	gdb, conn := newGormAuditDB(t)
	gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))

	conn.queueExec(
		gormAuditExec{affected: 0},
		gormAuditExec{err: &mysqldriver.MySQLError{Number: 1062, Message: "duplicate uk_golang_test"}},
		gormAuditExec{affected: 0},
	)
	conn.addQueryRule("SELECT 1", gormAuditQuery{columns: []string{"one"}})

	err := gdb.Save(&testpb.GolangTest{Id: 2, Ip: "same@example.test", Port: 7})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("备用 UNIQUE 冲突必须返回 ErrDuplicateKey，实际: %v", err)
	}

	for _, stmt := range conn.sqls() {
		if strings.Contains(strings.ToUpper(stmt), "ON DUPLICATE KEY UPDATE") {
			t.Fatalf("Save 不得再让备用 UNIQUE 冲突进入 ODKU: %s", stmt)
		}
	}
}

func TestGormBatchSaveUsesPrimaryKeyExactSemanticsPerRow(t *testing.T) {
	gdb, conn := newGormAuditDB(t)
	gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))

	conn.queueExec(
		gormAuditExec{affected: 1}, // 第一行按主键更新成功
		gormAuditExec{affected: 0}, // 第二行主键不存在
		gormAuditExec{err: &mysqldriver.MySQLError{Number: 1062, Message: "duplicate uk_golang_test"}},
		gormAuditExec{affected: 0}, // 并发同主键重试仍不存在，确认是备用 UNIQUE
	)
	conn.addQueryRule("SELECT 1", gormAuditQuery{columns: []string{"one"}})

	err := gdb.BatchSave([]proto.Message{
		&testpb.GolangTest{Id: 1, Ip: "first@example.test"},
		&testpb.GolangTest{Id: 2, Ip: "same@example.test"},
	})
	if !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("批量 Save 的备用 UNIQUE 冲突必须返回 ErrDuplicateKey，实际: %v", err)
	}

	if got := conn.countSQLPrefix("UPDATE "); got != 3 {
		t.Fatalf("BatchSave 应逐行走主键 UPDATE，实际 UPDATE 数=%d，SQL=%v", got, conn.sqls())
	}
}

func TestGormInsertIgnoreOnlySuppressesDuplicateKey(t *testing.T) {
	t.Run("duplicate", func(t *testing.T) {
		gdb, conn := newGormAuditDB(t)
		gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
		conn.queueExec(gormAuditExec{err: &mysqldriver.MySQLError{Number: 1062, Message: "duplicate PRIMARY"}})

		inserted, err := gdb.InsertIgnore(&testpb.GolangTest{Id: 1})
		if err != nil || inserted {
			t.Fatalf("1062 应只表示未插入，不应报错: inserted=%v err=%v", inserted, err)
		}
	})

	t.Run("data too long", func(t *testing.T) {
		gdb, conn := newGormAuditDB(t)
		gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
		conn.queueExec(gormAuditExec{err: &mysqldriver.MySQLError{Number: 1406, Message: "data too long"}})

		if _, err := gdb.InsertIgnore(&testpb.GolangTest{Id: 1}); err == nil {
			t.Fatal("非重复键错误必须原样返回")
		}
		for _, stmt := range conn.sqls() {
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stmt)), "INSERT IGNORE") {
				t.Fatalf("INSERT IGNORE 会把 1406/截断降级成 warning，必须发普通 INSERT: %s", stmt)
			}
		}
	})
}

func TestGormFindOneByPKForUpdateRequiresTransaction(t *testing.T) {
	gdb, _ := newGormAuditDB(t)
	gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))

	err := gdb.FindOneByPKForUpdate(&testpb.GolangTest{Id: 1})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "transaction") {
		t.Fatalf("事务外 FOR UPDATE 必须 fail-closed，实际: %v", err)
	}
}

func TestGormCreateOrUpdateTableUsesCoreSchemaSync(t *testing.T) {
	gdb, conn := newGormAuditDB(t)
	gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	conn.addQueryRule("SELECT DATABASE()", gormAuditQuery{columns: []string{"database"}, rows: [][]driver.Value{{"testdb"}}})
	conn.addQueryRule("lower_case_table_names", gormAuditQuery{columns: []string{"mode"}, rows: [][]driver.Value{{int64(0)}}})
	conn.addQueryRule("GET_LOCK", gormAuditQuery{columns: []string{"lock"}, rows: [][]driver.Value{{int64(1)}}})
	conn.addQueryRule("INFORMATION_SCHEMA.TABLES", gormAuditQuery{columns: []string{"count"}, rows: [][]driver.Value{{int64(1)}}})
	conn.addQueryRule("INFORMATION_SCHEMA.COLUMNS", gormAuditQuery{columns: []string{"name"}})
	conn.addQueryRule("KEY_COLUMN_USAGE", gormAuditQuery{columns: []string{"name"}, rows: [][]driver.Value{{"id"}}})
	conn.addQueryRule("RELEASE_LOCK", gormAuditQuery{columns: []string{"released"}, rows: [][]driver.Value{{int64(1)}}})

	if err := gdb.CreateOrUpdateTable(&testpb.GolangTest{}); err != nil {
		t.Fatalf("CreateOrUpdateTable: %v", err)
	}
	if conn.countSQLContaining("GET_LOCK") != 1 || conn.countSQLContaining("RELEASE_LOCK") != 1 {
		t.Fatalf("GORM schema sync 必须复用 core 的同 session 咨询锁，SQL=%v", conn.sqls())
	}
	if conn.countSQLContaining("IS_NULLABLE") == 0 || conn.countSQLContaining("EXTRA") == 0 {
		t.Fatalf("GORM schema sync 不得保留旧的残缺元数据查询，SQL=%v", conn.sqls())
	}
}

func TestGormCreateOrUpdateTableValidatesCurrentDatabase(t *testing.T) {
	gdb, conn := newGormAuditDB(t)
	gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	conn.addQueryRule("SELECT DATABASE()", gormAuditQuery{columns: []string{"database"}, rows: [][]driver.Value{{"otherdb"}}})
	conn.addQueryRule("lower_case_table_names", gormAuditQuery{columns: []string{"mode"}, rows: [][]driver.Value{{int64(0)}}})

	err := gdb.CreateOrUpdateTable(&testpb.GolangTest{})
	if err == nil || !strings.Contains(err.Error(), "不一致") {
		t.Fatalf("GORM schema bridge 必须拒绝从 otherdb 读写却按 testdb 元数据规划，实际: %v", err)
	}
	if conn.countSQLContaining("GET_LOCK") != 0 || conn.countSQLContaining("ALTER TABLE") != 0 {
		t.Fatalf("数据库身份校验失败后不得获取锁或下发 DDL，SQL=%v", conn.sqls())
	}
}

func TestGormCreateOrUpdateTableRejectsTransaction(t *testing.T) {
	gdb, _ := newGormAuditDB(t)
	gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))

	err := gdb.Transaction(func(tx *GormDB) error {
		return tx.CreateOrUpdateTable(&testpb.GolangTest{})
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "transaction") {
		t.Fatalf("MySQL DDL 会隐式提交，GORM 事务内必须拒绝 schema sync，实际: %v", err)
	}
}

func TestGormDecrByPKIfEnoughRequiresPositiveDelta(t *testing.T) {
	for _, delta := range []int64{0, -1} {
		t.Run(fmt.Sprintf("delta=%d", delta), func(t *testing.T) {
			gdb, conn := newGormAuditDB(t)
			gdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))

			updated, err := gdb.DecrByPKIfEnough(&testpb.GolangTest{Id: 1}, "port", delta)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "positive") {
				t.Fatalf("delta=%d 必须返回 positive 参数错误，updated=%v err=%v", delta, updated, err)
			}
			if got := conn.sqls(); len(got) != 0 {
				t.Fatalf("非法 delta 不得下发 SQL，delta=%d SQL=%v", delta, got)
			}
		})
	}
}

// gormAuditDialector 只把 GORM 的公开 API 接到可编程 database/sql 边界；
// 测试关心的是 GormDB 对数据库发出的行为，不模拟项目内部对象。
type gormAuditDialector struct{}

func (gormAuditDialector) Name() string { return "gorm-audit" }

func (gormAuditDialector) Initialize(db *gorm.DB) error {
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})
	return nil
}

func (gormAuditDialector) Migrator(*gorm.DB) gorm.Migrator { return nil }
func (gormAuditDialector) DataTypeOf(*schema.Field) string { return "" }
func (gormAuditDialector) DefaultValueOf(*schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}
func (gormAuditDialector) BindVarTo(w clause.Writer, _ *gorm.Statement, _ interface{}) {
	_ = w.WriteByte('?')
}
func (gormAuditDialector) QuoteTo(w clause.Writer, name string) {
	_, _ = w.WriteString("`" + strings.ReplaceAll(name, "`", "``") + "`")
}
func (gormAuditDialector) Explain(query string, _ ...interface{}) string { return query }

func newGormAuditDB(t *testing.T) (*GormDB, *gormAuditConn) {
	t.Helper()
	conn := &gormAuditConn{}
	sqlDB := sql.OpenDB(&gormAuditDriver{conn: conn})
	t.Cleanup(func() { _ = sqlDB.Close() })

	db, err := gorm.Open(gormAuditDialector{}, &gorm.Config{
		ConnPool:               sqlDB,
		DisableAutomaticPing:   true,
		SkipDefaultTransaction: true,
		Logger:                 logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("gorm.Open: %v", err)
	}
	return NewGormDB(db, "testdb"), conn
}

type gormAuditDriver struct{ conn *gormAuditConn }

func (d *gormAuditDriver) Open(string) (driver.Conn, error)             { return d.conn, nil }
func (d *gormAuditDriver) Connect(context.Context) (driver.Conn, error) { return d.conn, nil }
func (d *gormAuditDriver) Driver() driver.Driver                        { return d }

type gormAuditExec struct {
	affected int64
	lastID   int64
	err      error
}

type gormAuditQuery struct {
	columns []string
	rows    [][]driver.Value
	err     error
}

type gormAuditQueryRule struct {
	contains string
	result   gormAuditQuery
}

type gormAuditConn struct {
	mu         sync.Mutex
	executed   []string
	execQueue  []gormAuditExec
	queryQueue []gormAuditQuery
	queryRules []gormAuditQueryRule
}

func (c *gormAuditConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *gormAuditConn) Close() error                        { return nil }
func (c *gormAuditConn) Begin() (driver.Tx, error)           { return gormAuditTx{}, nil }
func (c *gormAuditConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return gormAuditTx{}, nil
}

func (c *gormAuditConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executed = append(c.executed, query)
	step := gormAuditExec{affected: 1}
	if len(c.execQueue) > 0 {
		step, c.execQueue = c.execQueue[0], c.execQueue[1:]
	}
	if step.err != nil {
		return nil, step.err
	}
	return gormAuditResult{affected: step.affected, lastID: step.lastID}, nil
}

func (c *gormAuditConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executed = append(c.executed, query)
	for _, rule := range c.queryRules {
		if strings.Contains(query, rule.contains) {
			if rule.result.err != nil {
				return nil, rule.result.err
			}
			return &gormAuditRows{columns: rule.result.columns, rows: rule.result.rows}, nil
		}
	}
	step := gormAuditQuery{columns: []string{"value"}}
	if len(c.queryQueue) > 0 {
		step, c.queryQueue = c.queryQueue[0], c.queryQueue[1:]
	}
	if step.err != nil {
		return nil, step.err
	}
	return &gormAuditRows{columns: step.columns, rows: step.rows}, nil
}

func (c *gormAuditConn) queueExec(steps ...gormAuditExec) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execQueue = append(c.execQueue, steps...)
}

func (c *gormAuditConn) addQueryRule(contains string, result gormAuditQuery) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryRules = append(c.queryRules, gormAuditQueryRule{contains: contains, result: result})
}

func (c *gormAuditConn) sqls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.executed...)
}

func (c *gormAuditConn) countSQLContaining(substr string) int {
	count := 0
	for _, query := range c.sqls() {
		if strings.Contains(query, substr) {
			count++
		}
	}
	return count
}

func (c *gormAuditConn) countSQLPrefix(prefix string) int {
	count := 0
	for _, query := range c.sqls() {
		if strings.HasPrefix(strings.TrimSpace(query), prefix) {
			count++
		}
	}
	return count
}

type gormAuditTx struct{}

func (gormAuditTx) Commit() error   { return nil }
func (gormAuditTx) Rollback() error { return nil }

type gormAuditResult struct {
	affected int64
	lastID   int64
}

func (r gormAuditResult) LastInsertId() (int64, error) { return r.lastID, nil }
func (r gormAuditResult) RowsAffected() (int64, error) { return r.affected, nil }

type gormAuditRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (r *gormAuditRows) Columns() []string { return r.columns }
func (r *gormAuditRows) Close() error      { return nil }
func (r *gormAuditRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.index])
	r.index++
	return nil
}

var _ driver.Connector = (*gormAuditDriver)(nil)
var _ driver.ExecerContext = (*gormAuditConn)(nil)
var _ driver.QueryerContext = (*gormAuditConn)(nil)
var _ driver.ConnBeginTx = (*gormAuditConn)(nil)
