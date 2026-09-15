package proto2mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
)

// 最小的 database/sql 假驱动，用来在**不连 MySQL** 的前提下测 DB 层。
//
// 为什么需要它：本仓库原先的 DB 层测试只有两种形态——纯函数测试（不碰 DB），
// 或者 mustOpenTestDB 那种"没设 PROTO2MYSQL_INTEGRATION=1 就直接 t.Skip"。
// 于是所有**连库路径**（建表对齐、咨询锁、索引补齐、schema 就绪探测、GormDB）
// 在日常 `go test ./...` 里是**一行都跑不到**的，全绿只是因为没测。
//
// 这正是本仓库栽过的那种坑：TEXT 索引缺前缀长度的 bug 长期没暴露，
// 就是因为测试只比对 SQL 字符串、从不真的执行。
//
// 对应 Python 侧的 tests/fakedb.py，行为刻意保持一致：
//   - 记录每次执行的 (sql, args)，测试据此断言"发出去的语句长什么样"
//   - 返回值由 queueRows 预先排队，按执行顺序依次弹出
//   - 队列空了返回空结果集（不报错），与 Python 的 _next_rows 同语义

type fakeDriver struct{ conn *fakeConn }

func (d *fakeDriver) Connect(context.Context) (driver.Conn, error) { return d.conn, nil }
func (d *fakeDriver) Driver() driver.Driver                        { return nil }

type fakeConn struct {
	mu       sync.Mutex
	executed []executedStmt
	pending  [][]driver.Value // 排队的结果集，每个元素是一行
	rowSets  [][][]driver.Value
	failNext error
}

type executedStmt struct {
	SQL  string
	Args []driver.Value
}

func newFakeConn() *fakeConn { return &fakeConn{} }

// openFakeDB 返回一个绑定假驱动的 *sql.DB 以及那个假连接（用于断言/排队）。
func openFakeDB() (*sql.DB, *fakeConn) {
	conn := newFakeConn()
	return sql.OpenDB(&fakeDriver{conn: conn}), conn
}

// queueRows 排队若干个结果集，按执行顺序依次返回。
func (c *fakeConn) queueRows(sets ...[][]driver.Value) *fakeConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rowSets = append(c.rowSets, sets...)
	return c
}

// sqls 返回至今执行过的全部语句文本。
func (c *fakeConn) sqls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.executed))
	for _, e := range c.executed {
		out = append(out, e.SQL)
	}
	return out
}

// findSQL 返回第一条以 prefix 开头的语句；没有则返回空串。
func (c *fakeConn) findSQL(prefix string) string {
	for _, s := range c.sqls() {
		if strings.HasPrefix(s, prefix) {
			return s
		}
	}
	return ""
}

// countSQL 统计包含 substr 的语句条数。
func (c *fakeConn) countSQL(substr string) int {
	n := 0
	for _, s := range c.sqls() {
		if strings.Contains(s, substr) {
			n++
		}
	}
	return n
}

func (c *fakeConn) record(query string, args []driver.NamedValue) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	vals := make([]driver.Value, len(args))
	for i, a := range args {
		vals[i] = a.Value
	}
	c.executed = append(c.executed, executedStmt{SQL: query, Args: vals})
	if c.failNext != nil {
		err := c.failNext
		c.failNext = nil
		return err
	}
	return nil
}

func (c *fakeConn) nextRows() [][]driver.Value {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.rowSets) == 0 {
		return nil
	}
	set := c.rowSets[0]
	c.rowSets = c.rowSets[1:]
	return set
}

// ── driver 接口 ──────────────────────────────────────────────────────────

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return fakeTx{}, nil }

func (c *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.record(query, args); err != nil {
		return nil, err
	}
	c.nextRows() // 与 Python 版一致：每次执行都消费一个结果集槽位
	return driver.RowsAffected(1), nil
}

func (c *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.record(query, args); err != nil {
		return nil, err
	}
	return &fakeRows{rows: c.nextRows()}, nil
}

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

type fakeRows struct {
	rows [][]driver.Value
	pos  int
}

func (r *fakeRows) Columns() []string {
	// 列名对本库无意义（都是按位置取值），但 database/sql 要求长度与每行一致。
	width := 0
	if len(r.rows) > 0 {
		width = len(r.rows[0])
	}
	cols := make([]string, width)
	for i := range cols {
		cols[i] = fmt.Sprintf("c%d", i)
	}
	return cols
}

func (r *fakeRows) Close() error { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

// ── 测试里常用的行构造helper ────────────────────────────────────────────

func rows(vals ...[]driver.Value) [][]driver.Value { return vals }
func row(vals ...driver.Value) []driver.Value      { return vals }

// colRow 造一行 information_schema.COLUMNS 的结果：(列名, 列类型, 列注释)
// colRow 一行 information_schema.COLUMNS 快照。
// 列序必须与 getTableColumnMeta 的 SELECT 一致：
// COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLLATION_NAME。
// colRow 用于本仓库 GolangTest 的对齐快照：数值列是 NOT NULL DEFAULT 0，TEXT/BLOB
// 可空，pb:1 的 id 是无默认值的 AUTO_INCREMENT；varchar/varbinary 按键列形态给
// NOT NULL、默认值空串与 KeyStringCollation。要造漂移用 colRowAttrsDefault / colRowFull。
func colRow(name, colType string, fieldNum int) []driver.Value {
	nullable := fakeColumnIsNullable(colType)
	extra := ""
	if name == "id" && fieldNum == 1 {
		nullable = false
		extra = "auto_increment"
	}
	return colRowAttrs(name, colType, fieldNum, nullable, extra)
}

// colRowAttrs 同 colRow，但能指定 IS_NULLABLE 与 EXTRA（"auto_increment" 等）。
func colRowAttrs(name, colType string, fieldNum int, nullable bool, extra string) []driver.Value {
	return colRowAttrsDefault(name, colType, fieldNum, nullable, extra, fakeColumnDefault(colType, extra))
}

// colRowAttrsDefault 还能显式指定 COLUMN_DEFAULT；nil 表示 information_schema 返回 SQL NULL。
func colRowAttrsDefault(name, colType string, fieldNum int, nullable bool, extra string, defaultValue driver.Value) []driver.Value {
	return colRowFull(name, colType, fieldNum, nullable, extra, defaultValue, fakeColumnCollation(colType))
}

// colRowFull 在 colRowAttrsDefault 之上再显式指定 COLLATION_NAME；nil 表示非字符列的 SQL NULL。
// 旧形态键列（varchar + utf8mb4_unicode_ci 等）的快照只能用它造。
func colRowFull(name, colType string, fieldNum int, nullable bool, extra string, defaultValue, collation driver.Value) []driver.Value {
	comment := ""
	if fieldNum > 0 {
		comment = fmt.Sprintf("pb:%d", fieldNum)
	}
	isNullable := "NO"
	if nullable {
		isNullable = "YES"
	}
	return row(name, colType, comment, isNullable, defaultValue, extra, collation)
}

func fakeColumnIsNullable(colType string) bool {
	upper := strings.ToUpper(colType)
	return strings.Contains(upper, "TEXT") || strings.Contains(upper, "BLOB") || strings.Contains(upper, "DATETIME")
}

func fakeColumnDefault(colType, extra string) driver.Value {
	if strings.Contains(strings.ToLower(extra), "auto_increment") {
		return nil
	}
	base := strings.ToLower(strings.Fields(colType)[0])
	if strings.HasPrefix(base, "tinyint") || strings.HasPrefix(base, "smallint") ||
		strings.HasPrefix(base, "mediumint") || strings.HasPrefix(base, "int") ||
		strings.HasPrefix(base, "bigint") || strings.HasPrefix(base, "float") ||
		strings.HasPrefix(base, "double") {
		return "0"
	}
	// 本库只为主键/唯一键 string/bytes 列生成 varchar/varbinary，形态固定 NOT NULL DEFAULT ''；
	// MySQL 与 TiDB 回读的 COLUMN_DEFAULT 都是空串而不是 NULL。
	if strings.HasPrefix(base, "varchar") || strings.HasPrefix(base, "varbinary") {
		return ""
	}
	return nil
}

// fakeColumnCollation 按真实 information_schema 的口径给 COLLATION_NAME：
// 本库的 varchar 键列显式带 KeyStringCollation，TEXT 列继承表默认的 utf8mb4_unicode_ci，
// 数值/二进制/时间列为 NULL。
func fakeColumnCollation(colType string) driver.Value {
	base := strings.ToLower(strings.Fields(colType)[0])
	switch {
	case strings.HasPrefix(base, "varchar"), strings.HasPrefix(base, "char"):
		return KeyStringCollation
	case strings.Contains(base, "text"):
		return "utf8mb4_unicode_ci"
	}
	return nil
}

// indexRow 造一行 information_schema.STATISTICS 的结果，列序与 core 查询一致。
// subPart 传 nil 表示整列索引；TEXT/BLOB 前缀索引传 int64(TextIndexPrefixLength)。
func indexRow(name string, unique bool, sequence int, column string, subPart driver.Value) []driver.Value {
	nonUnique := int64(1)
	if unique {
		nonUnique = 0
	}
	return row(name, nonUnique, int64(sequence), column, subPart)
}

// newFakeDB 建一个绑定假连接的 *DB，并注册 GolangTest。
func newFakeDB(t *testing.T) (*DB, *fakeConn) {
	t.Helper()
	sqlDB, conn := openFakeDB()
	t.Cleanup(func() { _ = sqlDB.Close() })

	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	return pdb, conn
}
