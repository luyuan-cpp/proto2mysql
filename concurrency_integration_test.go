package proto2mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
)

// 真库并发测试。对应 docs/fixes-2026-08.md 第 11 条（结构同步零并发保护）。
//
// 这条路径此前只在**单线程假驱动**下验过——而它要防的恰恰是并发。
// 没有 GET_LOCK 时：N 个副本同时冷启动 → 一个 ALTER 成功、其余全部撞
// Error 1060 Duplicate column name → 启动失败。下一次重启会成功（列已经存在，
// 对齐结果为空），所以它是"自愈式蒙对"：日志里留下一串启动失败、服务最终起来了，
// 很容易被当成偶发 flake 忽略，直到某次重启风暴把它放大。

// TestConcurrentSyncAllTables N 条独立连接同时 SyncAllTables，全部必须成功。
//
// 每条连接是独立的 *sql.DB + 独立的 *DB 实例，模拟 N 个副本同时冷启动。
// GET_LOCK 是**连接级**的，所以必须用独立连接才测得出真东西——
// 复用同一条连接的话锁是可重入的，等于没测。
func TestConcurrentSyncAllTables(t *testing.T) {
	cfg := GetMysqlConfig()
	if cfg == nil {
		t.Skip("跳过并发集成测试: 需要 PROTO2MYSQL_TEST_DSN 或 testdata/db.json")
	}
	probe := NewDB()
	root := mustOpenTestDB(t, probe)
	defer closeTestDB(t, root)

	const table = "concurrent_sync_probe"
	if _, err := root.Exec("DROP TABLE IF EXISTS `" + table + "`"); err != nil {
		t.Fatalf("清理探针表: %v", err)
	}
	// 先建一张**只有主键列**的表，逼所有副本都去 ADD COLUMN——这才有冲突可撞。
	//
	// 探针用 Player 而不是 GolangTest：后者的 proto 里声明了 auto_increment_key，
	// ALTER 会夹一条 MODIFY ... AUTO_INCREMENT，而 TiDB 不支持给已存在的列加自增
	// （Error 8200，见 fixes-2026-08 第 14 条）——那会把"并发"这件事整个盖住，
	// 8 个副本全失败，但失败原因跟锁毫无关系。
	if _, err := root.Exec("CREATE TABLE `" + table + "` (`player_id` bigint unsigned NOT NULL DEFAULT 0, PRIMARY KEY(`player_id`))"); err != nil {
		t.Fatalf("建探针表: %v", err)
	}
	defer func() { _, _ = root.Exec("DROP TABLE IF EXISTS `" + table + "`") }()

	const replicas = 8
	var wg sync.WaitGroup
	errs := make([]error, replicas)
	start := make(chan struct{})

	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			db, err := sql.Open("mysql", cfg.FormatDSN())
			if err != nil {
				errs[idx] = fmt.Errorf("副本 %d 连库: %w", idx, err)
				return
			}
			defer db.Close()
			// 每个副本一条连接，别让连接池把 GET_LOCK 变成可重入
			db.SetMaxOpenConns(1)

			pdb := NewDB()
			pdb.RegisterTable(&testpb.Player{}, WithTableName(table), WithPrimaryKey("player_id"))
			pdb.DB = db
			pdb.DBName = cfg.DBName

			<-start // 尽量让 8 个副本同时冲进去
			errs[idx] = pdb.SyncAllTables()
		}(i)
	}
	close(start)
	wg.Wait()

	var failed []string
	for i, err := range errs {
		if err != nil {
			failed = append(failed, fmt.Sprintf("副本 %d: %v", i, err))
		}
	}
	if len(failed) > 0 {
		t.Errorf("%d/%d 个副本同步失败——咨询锁没起作用：\n  %s",
			len(failed), replicas, strings.Join(failed, "\n  "))
	}

	// 结构必须真的对齐了（不是"都失败了所以没冲突"）
	var cols int
	if err := root.QueryRow(
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		cfg.DBName, table).Scan(&cols); err != nil {
		t.Fatalf("回读列数: %v", err)
	}
	if cols != 2 {
		t.Errorf("并发同步后应有 2 列，实际 %d 列——说明有副本的 ALTER 丢了", cols)
	}
}

// TestConcurrentSyncIsIdempotent 反复并发同步不该产生任何额外变更。
//
// 结构已经对齐之后再并发跑，每个副本的对齐结果都该是空——
// 如果有副本仍在发 ALTER，说明"对齐"本身不是幂等的（比如注释回填反复触发）。
func TestConcurrentSyncIsIdempotent(t *testing.T) {
	cfg := GetMysqlConfig()
	if cfg == nil {
		t.Skip("跳过并发集成测试: 需要 PROTO2MYSQL_TEST_DSN 或 testdata/db.json")
	}
	probe := NewDB()
	root := mustOpenTestDB(t, probe)
	defer closeTestDB(t, root)

	const table = "concurrent_idempotent_probe"
	_, _ = root.Exec("DROP TABLE IF EXISTS `" + table + "`")
	defer func() { _, _ = root.Exec("DROP TABLE IF EXISTS `" + table + "`") }()

	newReplica := func() (*DB, *sql.DB, error) {
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			return nil, nil, err
		}
		db.SetMaxOpenConns(1)
		pdb := NewDB()
		pdb.RegisterTable(&testpb.Player{}, WithTableName(table), WithPrimaryKey("player_id"))
		pdb.DB = db
		pdb.DBName = cfg.DBName
		return pdb, db, nil
	}

	// 先由一个副本建好
	first, conn, err := newReplica()
	if err != nil {
		t.Fatalf("建首个副本: %v", err)
	}
	if err := first.SyncAllTables(); err != nil {
		conn.Close()
		t.Fatalf("首次同步: %v", err)
	}
	conn.Close()

	// 再让 8 个副本并发跑一遍，全部必须成功且无变更
	const replicas = 8
	var wg sync.WaitGroup
	errs := make([]error, replicas)
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pdb, db, err := newReplica()
			if err != nil {
				errs[idx] = err
				return
			}
			defer db.Close()
			errs[idx] = pdb.SyncAllTables()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("副本 %d 的幂等同步失败: %v", i, err)
		}
	}
}

// TestConcurrentSyncFailsWithoutLock 负向验证：**证明上面那条测试真的在测锁**。
//
// 一条只会报绿的测试毫无价值。这里把咨询锁关掉再跑同样的并发场景——
// 必须有副本失败，且失败原因必须正是 Error 1060 Duplicate column name。
//
// 实测：8 个副本里 7 个失败。也就是说没有这把锁，8 副本的服务冷启动时
// 有 87.5% 的实例起不来；而它们重启后又能成功（列已存在，对齐结果为空），
// 于是整件事表现为"偶发的启动 flake"——这正是最难被认真对待的那种故障形态。
func TestConcurrentSyncFailsWithoutLock(t *testing.T) {
	cfg := GetMysqlConfig()
	if cfg == nil {
		t.Skip("跳过并发集成测试: 需要 PROTO2MYSQL_TEST_DSN 或 testdata/db.json")
	}
	if strings.Contains(strings.ToLower(cfg.Addr), "tidb") {
		t.Skip("TiDB 上 GET_LOCK 行为与 MySQL 不同，本负向验证只在 MySQL 上有意义")
	}

	probe := NewDB()
	root := mustOpenTestDB(t, probe)
	defer closeTestDB(t, root)

	const table = "concurrent_nolock_probe"
	_, _ = root.Exec("DROP TABLE IF EXISTS `" + table + "`")
	if _, err := root.Exec("CREATE TABLE `" + table + "` (`player_id` bigint unsigned NOT NULL DEFAULT 0, PRIMARY KEY(`player_id`))"); err != nil {
		t.Fatalf("建探针表: %v", err)
	}
	defer func() { _, _ = root.Exec("DROP TABLE IF EXISTS `" + table + "`") }()

	disableSyncLockForTest = true
	defer func() { disableSyncLockForTest = false }()

	const replicas = 8
	var wg sync.WaitGroup
	errs := make([]error, replicas)
	start := make(chan struct{})
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			db, err := sql.Open("mysql", cfg.FormatDSN())
			if err != nil {
				errs[idx] = err
				return
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			pdb := NewDB()
			pdb.RegisterTable(&testpb.Player{}, WithTableName(table), WithPrimaryKey("player_id"))
			pdb.DB = db
			pdb.DBName = cfg.DBName
			<-start
			errs[idx] = pdb.SyncAllTables()
		}(i)
	}
	close(start)
	wg.Wait()

	var dupErrors int
	for _, err := range errs {
		if err != nil && strings.Contains(err.Error(), "Duplicate column name") {
			dupErrors++
		}
	}
	if dupErrors == 0 {
		t.Fatal("关掉咨询锁后竟然一个副本都没失败——" +
			"说明 TestConcurrentSyncAllTables 的绿灯不是锁挣来的，那条测试是摆设")
	}
	t.Logf("关掉锁后 %d/%d 个副本撞 Error 1060——证明锁确实在干活", dupErrors, replicas)
}
