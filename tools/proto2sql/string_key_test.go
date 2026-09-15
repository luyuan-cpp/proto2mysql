package proto2sql

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/luyuancpp/proto2mysql"
)

// TestGenerateStringKeyMaxLength max_length 经 protocompile 编译出来的 field option 必须生效：
// 唯一键里的 string 按声明的长度建 VARCHAR 键列，不在键里的 bytes 仍是 MEDIUMBLOB。
func TestGenerateStringKeyMaxLength(t *testing.T) {
	tables, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{"testdata/string_key.proto"},
		ImportPaths: []string{optionProtoDir()},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(tables) != 1 || tables[0].Name != "third_party_account" {
		t.Fatalf("expected 1 third_party_account table, got %+v", tables)
	}

	sql := tables[0].SQL
	for _, want := range []string{
		"`provider` VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''",
		"`provider_id` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''",
		"`token_hash` MEDIUMBLOB",
		"UNIQUE KEY `uk_third_party_account` (`provider`,`provider_id`)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q\n--- got ---\n%s", want, sql)
		}
	}
}

// TestGenerateRejectsOutOfRangeMaxLength 越界的 max_length 必须在产出 SQL 之前报错，并点名字段。
// VARCHAR(769) utf8mb4 做主键在 MySQL 上是 Error 1071，离线生成的脚本不能带着它出去。
func TestGenerateRejectsOutOfRangeMaxLength(t *testing.T) {
	dir := t.TempDir()
	src := `syntax = "proto3";
package badlen;
import "proto2mysql_option.proto";

message BadLen {
  option (proto2mysql.table_name)  = "bad_len";
  option (proto2mysql.primary_key) = "external_id";
  string external_id = 1 [(proto2mysql.max_length) = 769];
}
`
	path := filepath.Join(dir, "bad_len.proto")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("写 proto: %v", err)
	}

	_, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{path},
		ImportPaths: []string{optionProtoDir()},
	})
	if !errors.Is(err, proto2mysql.ErrInvalidTableOption) {
		t.Fatalf("越界的 max_length 必须返回 ErrInvalidTableOption，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "external_id") || !strings.Contains(err.Error(), "769") {
		t.Errorf("错误信息要点名字段与越界值: %v", err)
	}
}
