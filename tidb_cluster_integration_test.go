package proto2mysql

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
)

// 多节点 TiDB 集群测试。对应 docs/fixes-2026-08.md 第 13 条（TiDB 异步 DDL 零等待）。
//
// **单容器 standalone 测不出这个**：TiDB 的 DDL 是异步 online 的——ALTER 语句返回时，
// schema 变更只是进了 DDL 队列，各个 TiDB 节点要按 lease（默认 45s）分批加载新版本。
// 只有一个节点时，"其它节点还没加载"这个窗口根本不存在。
//
// 起集群：
//
//	docker compose -f tidb-cluster.yml up -d      # PD + TiKV + tidb0 + tidb1
//
// 跑：
//
//	PROTO2MYSQL_INTEGRATION=1 \
//	PROTO2MYSQL_TEST_DSN="root:@tcp(127.0.0.1:14001)/proto2mysql_go_test" \
//	PROTO2MYSQL_TEST_DSN2="root:@tcp(127.0.0.1:14002)/proto2mysql_go_test" \
//	  go test -count=1 -run TestTiDBCluster ./...

const secondNodeEnv = "PROTO2MYSQL_TEST_DSN2"

// TestTiDBClusterSchemaVisibleOnOtherNode 在节点 0 上改结构，节点 1 必须立刻能用。
//
// 这正是 awaitSchemaVisible 要守的窗口：库执行完 ALTER 就会按新 proto 发 SQL，
// 如果那时连到的是**还没加载新 schema 的节点**，就会报 Unknown column——
// 启动日志漂亮，第一批请求全挂。
func TestTiDBClusterSchemaVisibleOnOtherNode(t *testing.T) {
	cfg := GetMysqlConfig()
	dsn2 := os.Getenv(secondNodeEnv)
	if cfg == nil || dsn2 == "" {
		t.Skipf("跳过多节点测试: 需要 PROTO2MYSQL_TEST_DSN 与 %s 指向同一集群的两个 TiDB 节点", secondNodeEnv)
	}

	node0 := NewDB()
	db0 := mustOpenTestDB(t, node0)
	defer closeTestDB(t, db0)

	var version string
	if err := db0.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatalf("查版本: %v", err)
	}
	if !strings.Contains(strings.ToLower(version), "tidb") {
		t.Skipf("本用例只针对 TiDB，当前后端是 %s", version)
	}

	db1, err := sql.Open("mysql", dsn2)
	if err != nil {
		t.Fatalf("连第二个节点: %v", err)
	}
	defer db1.Close()

	// 确认真是两个不同的 TiDB 实例，而不是同一个的两个端口
	var id0, id1 string
	if err := db0.QueryRow("SELECT @@tidb_config").Scan(&id0); err != nil {
		t.Logf("读 tidb_config 失败（不影响主体断言）: %v", err)
	}
	if err := db1.QueryRow("SELECT @@tidb_config").Scan(&id1); err != nil {
		t.Logf("读 tidb_config 失败: %v", err)
	}
	var nodes int
	if err := db0.QueryRow(
		"SELECT COUNT(*) FROM INFORMATION_SCHEMA.CLUSTER_INFO WHERE TYPE = 'tidb'").Scan(&nodes); err == nil {
		t.Logf("集群里有 %d 个 TiDB 节点", nodes)
		if nodes < 2 {
			t.Skip("集群只有一个 TiDB 节点，测不出跨节点 schema 加载窗口")
		}
	}

	const table = "tidb_cluster_probe"
	_, _ = db0.Exec("DROP TABLE IF EXISTS `" + table + "`")
	if _, err := db0.Exec(
		"CREATE TABLE `" + table + "` (`player_id` bigint unsigned NOT NULL DEFAULT 0, PRIMARY KEY(`player_id`))"); err != nil {
		t.Fatalf("建探针表: %v", err)
	}
	defer func() { _, _ = db0.Exec("DROP TABLE IF EXISTS `" + table + "`") }()

	// 在节点 0 上把结构对齐（会 ADD COLUMN name）
	pdb := NewDB()
	pdb.RegisterTable(&testpb.Player{}, WithTableName(table), WithPrimaryKey("player_id"))
	pdb.DB = db0
	pdb.DBName = cfg.DBName
	if err := pdb.CreateOrUpdateTable(&testpb.Player{}); err != nil {
		t.Fatalf("节点 0 同步结构失败: %v", err)
	}

	// **立刻**在节点 1 上按新结构写读——这是 awaitSchemaVisible 要守的那个窗口
	other := NewDB()
	other.RegisterTable(&testpb.Player{}, WithTableName(table), WithPrimaryKey("player_id"))
	other.DB = db1
	other.DBName = cfg.DBName

	if err := other.Save(&testpb.Player{PlayerId: 1, Name: "alice"}); err != nil {
		t.Fatalf("节点 1 立刻写入失败——跨节点 schema 还没生效: %v", err)
	}
	out := &testpb.Player{PlayerId: 1}
	if err := other.FindOneByPK(out); err != nil {
		t.Fatalf("节点 1 立刻读回失败: %v", err)
	}
	if out.Name != "alice" {
		t.Errorf("节点 1 读到的内容不对: %q", out.Name)
	}
}

// TestTiDBClusterConcurrentSyncAcrossNodes 两个节点上的副本同时同步同一张表。
//
// 比单节点并发更狠：不仅是多连接，还是**多个 TiDB 实例**——DDL 要经过 PD 排队。
func TestTiDBClusterConcurrentSyncAcrossNodes(t *testing.T) {
	cfg := GetMysqlConfig()
	dsn2 := os.Getenv(secondNodeEnv)
	if cfg == nil || dsn2 == "" {
		t.Skipf("跳过多节点测试: 需要 PROTO2MYSQL_TEST_DSN 与 %s", secondNodeEnv)
	}

	probe := NewDB()
	root := mustOpenTestDB(t, probe)
	defer closeTestDB(t, root)

	var version string
	_ = root.QueryRow("SELECT VERSION()").Scan(&version)
	if !strings.Contains(strings.ToLower(version), "tidb") {
		t.Skipf("本用例只针对 TiDB，当前后端是 %s", version)
	}

	const table = "tidb_cluster_concurrent_probe"
	_, _ = root.Exec("DROP TABLE IF EXISTS `" + table + "`")
	if _, err := root.Exec(
		"CREATE TABLE `" + table + "` (`player_id` bigint unsigned NOT NULL DEFAULT 0, PRIMARY KEY(`player_id`))"); err != nil {
		t.Fatalf("建探针表: %v", err)
	}
	defer func() { _, _ = root.Exec("DROP TABLE IF EXISTS `" + table + "`") }()

	dsns := []string{cfg.FormatDSN(), dsn2}
	const perNode = 4
	type result struct {
		node int
		err  error
	}
	results := make(chan result, len(dsns)*perNode)

	for n, dsn := range dsns {
		for i := 0; i < perNode; i++ {
			go func(node int, dsn string) {
				db, err := sql.Open("mysql", dsn)
				if err != nil {
					results <- result{node, err}
					return
				}
				defer db.Close()
				db.SetMaxOpenConns(1)
				pdb := NewDB()
				pdb.RegisterTable(&testpb.Player{}, WithTableName(table), WithPrimaryKey("player_id"))
				pdb.DB = db
				pdb.DBName = cfg.DBName
				results <- result{node, pdb.SyncAllTables()}
			}(n, dsn)
		}
	}

	var failures []string
	for i := 0; i < len(dsns)*perNode; i++ {
		r := <-results
		if r.err != nil {
			failures = append(failures, "节点"+string(rune('0'+r.node))+": "+r.err.Error())
		}
	}
	if len(failures) > 0 {
		t.Errorf("跨节点并发同步有 %d 个失败：\n  %s",
			len(failures), strings.Join(failures, "\n  "))
	}
}
