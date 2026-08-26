package proto2sql

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 2026-08-26 审计里 CLI/生成器一侧确认发现的回归网。

// writeProto 在 dir 下写一个最小可编译的 .proto，声明 table_name = tableName。
func writeProto(t *testing.T, dir, file, pkg, message, tableName string) string {
	t.Helper()
	src := `syntax = "proto3";
package ` + pkg + `;
import "proto2mysql_option.proto";

message ` + message + ` {
  option (proto2mysql.table_name)  = "` + tableName + `";
  option (proto2mysql.primary_key) = "id";
  uint64 id = 1;
  string name = 2;
}
`
	path := filepath.Join(dir, file)
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("写 %s: %v", path, err)
	}
	return path
}

// TestDuplicateTableNameIsRejected 重复的 table_name 原先被静默 first-wins：
// 后一个 message 直接被条件跳过，全程无 log 无 error。而这几乎总是配错了
// （复制粘贴、merge 冲突），产出的 schema 只按先遍历到的那份建，
// 另一份定义凭空消失，排查时根本看不出发生过什么。
func TestDuplicateTableNameIsRejected(t *testing.T) {
	dir := t.TempDir()
	writeProto(t, dir, "a.proto", "dup", "Alpha", "same_table")
	writeProto(t, dir, "b.proto", "dup", "Beta", "same_table")

	_, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{filepath.Join(dir, "a.proto"), filepath.Join(dir, "b.proto")},
		ImportPaths: []string{optionProtoDir()},
	})
	if err == nil {
		t.Fatal("重复的 table_name 必须报错，不能静默丢掉一份定义")
	}
	if !strings.Contains(err.Error(), "same_table") {
		t.Errorf("错误信息要点名是哪个表名: %v", err)
	}
	if !strings.Contains(err.Error(), "Alpha") || !strings.Contains(err.Error(), "Beta") {
		t.Errorf("错误信息要把冲突双方都列出来，否则还得自己去翻: %v", err)
	}
}

// TestCaseInsensitiveTableNameCollisionIsRejected 即使目标机器当前区分大小写，
// 生成物也经常会被提交后转到 Windows/macOS 或 lower_case_table_names=1 的 MySQL。
// Users.sql 与 users.sql 在这些环境里是同一个文件，必须在落盘前 fail-closed。
func TestCaseInsensitiveTableNameCollisionIsRejected(t *testing.T) {
	dir := t.TempDir()
	writeProto(t, dir, "upper.proto", "caseclash", "Upper", "Users")
	writeProto(t, dir, "lower.proto", "caseclash", "Lower", "users")

	_, err := Generate(context.Background(), Config{
		ProtoFiles: []string{
			filepath.Join(dir, "upper.proto"),
			filepath.Join(dir, "lower.proto"),
		},
		ImportPaths: []string{optionProtoDir()},
	})
	if err == nil {
		t.Fatal("仅大小写不同的 table_name 必须拒绝，否则会在大小写不敏感文件系统静默覆盖")
	}
	for _, want := range []string{"Users", "users", "Upper", "Lower"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误应列出冲突名称和双方 message，缺少 %q: %v", want, err)
		}
	}
}

// TestGenerateAllowsNamesThatAreUnsafeOnlyAsPerTableFilenames Generate 也服务于
// -single 和纯内存调用；Windows 文件名规则不能污染 MySQL 反引号可安全引用的表名。
// 逐表落盘时由 cmd.checkOutputName 在真正构造路径的边界上拒绝。
func TestGenerateAllowsNamesThatAreUnsafeOnlyAsPerTableFilenames(t *testing.T) {
	for _, tableName := range []string{"CON", "bad:name"} {
		t.Run(tableName, func(t *testing.T) {
			dir := t.TempDir()
			path := writeProto(t, dir, "portable.proto", "portable", "Portable", tableName)
			tables, err := Generate(context.Background(), Config{
				ProtoFiles:  []string{path},
				ImportPaths: []string{optionProtoDir()},
			})
			if err != nil {
				t.Fatalf("纯内存生成不应套用 Windows 文件名规则: %v", err)
			}
			if len(tables) != 1 || tables[0].Name != tableName {
				t.Fatalf("生成结果 = %+v, want table_name %q", tables, tableName)
			}
		})
	}
}

// TestInputShadowingIsRejected resolveInputs 把 `-proto a/b/game.proto` 降成裸文件名，
// 再把 a/b 追加到搜索路径**末尾**。于是任何一个 -I 目录里的同名文件都会先命中——
// 用户明明写了路径，编译的却是另一个文件，产出的是另一份表结构，而且完全没有提示。
func TestInputShadowingIsRejected(t *testing.T) {
	shadowDir := t.TempDir() // 会被当成 -I 传进去，里头放一个同名文件
	realDir := t.TempDir()   // 用户真正点名的那个

	writeProto(t, shadowDir, "game.proto", "shadow", "Shadow", "shadow_table")
	real := writeProto(t, realDir, "game.proto", "real", "Real", "real_table")

	_, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{real},
		ImportPaths: []string{optionProtoDir(), shadowDir},
	})
	if err == nil {
		t.Fatal("输入文件被同名文件顶掉时必须报错")
	}
	if !strings.Contains(err.Error(), "game.proto") {
		t.Errorf("错误信息要点名是哪个文件: %v", err)
	}
}

// TestNoShadowingFalsePositive 正常情况（没有同名文件）不该被误伤——
// 这条挡的是"检查太严把好用例打死"。
func TestNoShadowingFalsePositive(t *testing.T) {
	tables, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{"testdata/account.proto"},
		ImportPaths: []string{optionProtoDir()},
	})
	if err != nil {
		t.Fatalf("正常输入不该报 shadow: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("应当生成出表")
	}
}

// TestUnsupportedKindRejectedOffline 离线这条路原先没有前置校验：
// 一个没有 MySQL 映射的字段（sint64 等）会被静默按 TEXT 写进 .sql，
// 那份脚本建出来的表类型是错的，事后改回正确类型是跨族 MODIFY，会把数据吃成 0。
func TestUnsupportedKindRejectedOffline(t *testing.T) {
	dir := t.TempDir()
	src := `syntax = "proto3";
package unsup;
import "proto2mysql_option.proto";

message Unsup {
  option (proto2mysql.table_name)  = "unsup_table";
  option (proto2mysql.primary_key) = "id";
  uint64 id  = 1;
  sint64 zig = 2;   // 没有 MySQL 类型映射
}
`
	path := filepath.Join(dir, "unsup.proto")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatalf("写 proto: %v", err)
	}

	_, err := Generate(context.Background(), Config{
		ProtoFiles:  []string{path},
		ImportPaths: []string{optionProtoDir()},
	})
	if err == nil {
		t.Fatal("不支持的字段类型必须在产出 SQL 之前被拒")
	}
	if !strings.Contains(err.Error(), "zig") {
		t.Errorf("错误信息要点名是哪个字段: %v", err)
	}
}
