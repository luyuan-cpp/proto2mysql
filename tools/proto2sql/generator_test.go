package proto2sql

import (
	"context"
	"strings"
	"testing"
)

// optionProtoDir 返回本仓库自带选项定义（proto2mysql_option.proto）所在目录，
// 供 protocompile 解析 account.proto 的 import。descriptor.proto 由标准 import 自动提供。
func optionProtoDir() string {
	return "../../proto"
}

func TestGenerate(t *testing.T) {
	tables, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{"testdata/account.proto"},
		ImportPaths: []string{optionProtoDir()},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}

	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d: %+v", len(tables), tables)
	}
	tbl := tables[0]
	if tbl.Name != "account" {
		t.Fatalf("table name = %q, want account", tbl.Name)
	}

	sql := tbl.SQL
	checks := []string{
		"CREATE TABLE IF NOT EXISTS `account`",
		"`id` bigint unsigned NOT NULL AUTO_INCREMENT",
		// email 在唯一键里 → VARCHAR 键列，唯一键建在整列上（MEDIUMTEXT 前缀索引只保证前 191 个字符唯一）
		"`email` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''",
		"UNIQUE KEY `uk_account` (`email`)",
		"PRIMARY KEY (`id`)",
		// name 只在普通索引里，仍是 MEDIUMTEXT。MySQL 不允许对 TEXT/BLOB 列建
		// 不带前缀长度的索引（Error 1170），所以这里必须带 (191)。
		//
		// 早先这条断言写的是裸列名——也就是说这个测试一直在**断言一条 MySQL
		// 根本不会执行的 DDL**。之所以长期没暴露，正是因为它只比对字符串、
		// 从不真的把语句打到库上。
		"`name` MEDIUMTEXT",
		"INDEX `idx_account_0` (`name`(191))",
	}
	for _, c := range checks {
		if !strings.Contains(sql, c) {
			t.Errorf("SQL missing %q\n--- got ---\n%s", c, sql)
		}
	}
}

func TestGenerateDrop(t *testing.T) {
	tables, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{"testdata/account.proto"},
		ImportPaths: []string{optionProtoDir()},
		Drop:        true,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(tables))
	}
	if !strings.HasPrefix(tables[0].SQL, "DROP TABLE IF EXISTS `account`;") {
		t.Errorf("expected DROP prefix, got:\n%s", tables[0].SQL)
	}
}

func TestGenerateRequireDBOption(t *testing.T) {
	tables, err := Generate(context.Background(), Config{
		ProtoFiles:      []string{"testdata/account.proto", "testdata/unmarked.proto"},
		ImportPaths:     []string{optionProtoDir()},
		RequireDBOption: true,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(tables) != 1 || tables[0].Name != "account" {
		t.Fatalf("expected only db-marked account table, got %+v", tables)
	}
}

// TestGenerateTiDBDialect 端到端：proto 里声明的 TiDB 方言选项应进入生成的 DDL
// （/*T!*/ 扩展注释，MySQL 视为注释忽略，TiDB 解析生效）。
func TestGenerateTiDBDialect(t *testing.T) {
	tables, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{"testdata/tidb_hotspot.proto"},
		ImportPaths: []string{optionProtoDir()},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(tables) != 1 || tables[0].Name != "tidb_hotspot" {
		t.Fatalf("expected 1 tidb_hotspot table, got %+v", tables)
	}

	sql := tables[0].SQL
	checks := []string{
		"PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */",
		"/*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */",
	}
	for _, c := range checks {
		if !strings.Contains(sql, c) {
			t.Errorf("SQL missing %q\n--- got ---\n%s", c, sql)
		}
	}
}
