// Command proto2sql 从 .proto 文件生成 MySQL 建表 SQL 文件。
//
// 用法示例：
//
//	proto2sql -proto game.proto -I . -I /path/to/proto2mysql/proto -out ./sql
//	proto2sql -proto a.proto,b.proto -out ./sql -single schema.sql -drop
//
// 说明：
//   - -proto  要编译的 .proto 文件（逗号分隔或重复传入）；其所在目录会自动加入搜索路径。
//   - -I      额外的 import 搜索目录（用于定位被 import 的 proto 文件和 descriptor.proto 等）。
//   - -out    输出目录（默认当前目录，不存在会创建）。
//   - -single 若指定，则所有表合并写入该文件；否则每张表一个 <表名>.sql。
//   - -drop   在每条 CREATE TABLE 前加 DROP TABLE IF EXISTS。
//     ⚠️ 破坏性：DROP 会删除整张表及其全部数据，仅用于空库/测试库初始化，切勿用于生产或服务器启动。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/luyuancpp/proto2sql"
)

// repeatedFlag 支持重复传入的字符串 flag（如多个 -I）。
type repeatedFlag []string

func (r *repeatedFlag) String() string { return strings.Join(*r, ",") }
func (r *repeatedFlag) Set(v string) error {
	*r = append(*r, v)
	return nil
}

func main() {
	var (
		protoArg   string
		importDirs repeatedFlag
		outDir     string
		single     string
		drop       bool
	)
	flag.StringVar(&protoArg, "proto", "", "要编译的 .proto 文件，逗号分隔（也可重复用 -proto）")
	flag.Var(&importDirs, "I", "import 搜索目录（可重复）")
	flag.StringVar(&outDir, "out", ".", "输出目录")
	flag.StringVar(&single, "single", "", "合并输出到单个文件名（相对 -out）；不设则每表一个文件")
	flag.BoolVar(&drop, "drop", false, "⚠️ 破坏性：每条 CREATE TABLE 前加 DROP TABLE IF EXISTS（会删除表及全部数据，仅用于空库/测试初始化）")
	// 允许重复 -proto
	var protoRepeated repeatedFlag
	flag.Var(&protoRepeated, "proto-file", "要编译的 .proto 文件（可重复；等价于 -proto 的逐个形式）")
	flag.Parse()

	protoFiles := append([]string(protoRepeated), splitCSV(protoArg)...)
	if len(protoFiles) == 0 {
		fmt.Fprintln(os.Stderr, "error: 至少需要一个 -proto 文件")
		flag.Usage()
		os.Exit(2)
	}

	tables, err := proto2sql.Generate(context.Background(), proto2sql.Config{
		ProtoFiles:  protoFiles,
		ImportPaths: importDirs,
		Drop:        drop,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if len(tables) == 0 {
		fmt.Fprintln(os.Stderr, "warning: 未发现带 OptionTableName 的表消息，未生成任何文件")
		return
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "error: 创建输出目录失败: %v\n", err)
		os.Exit(1)
	}

	// -drop 的警告必须**跟着生成物走**：光在 CLI 帮助里写没用，
	// 真正的风险是这份 .sql 被别人拿去 `mysql < schema.sql`——它看起来就是一份
	// 普通的建表脚本，一敲整库蒸发。
	banner := ""
	if drop {
		banner = dropModeBanner
	}

	if single != "" {
		var b strings.Builder
		b.WriteString(banner)
		for _, t := range tables {
			b.WriteString(t.SQL)
			b.WriteString("\n\n")
		}
		path := filepath.Join(outDir, single)
		if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "error: 写文件 %s 失败: %v\n", path, err)
			os.Exit(1)
		}
		fmt.Printf("生成 %d 张表 -> %s\n", len(tables), path)
		return
	}

	for _, t := range tables {
		path := filepath.Join(outDir, t.Name+".sql")
		if err := os.WriteFile(path, []byte(banner+t.SQL+"\n"), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "error: 写文件 %s 失败: %v\n", path, err)
			os.Exit(1)
		}
		fmt.Printf("生成表 %s -> %s\n", t.Name, path)
	}
}

// dropModeBanner -drop 生成物的文件头警告。
//
// 光在 CLI 帮助和文档里写警告是不够的——真正的风险是**生成出来的 .sql 文件被别人
// 拿去执行**：它看起来就是一份普通的建表脚本，`mysql < schema.sql` 一敲，整库蒸发。
// 所以警告必须**跟着文件走**，谁打开都能第一眼看见。
const dropModeBanner = `-- ############################################################################
-- ##  危险：本文件由 proto2sql -drop 生成，每张表前都有 DROP TABLE IF EXISTS
-- ##
-- ##  执行它会删掉这些表及其全部数据，且不可恢复。
-- ##  仅用于空库初始化 / 测试库重建，切勿用于生产库或服务启动流程。
-- ##
-- ##  要在保留数据的前提下演进结构，请改用运行时库：
-- ##      DB.GenerateMigrationSQL()   只产出 ALTER，不删数据（推荐：交人工/CI 审核）
-- ##      DB.SyncAllTables()          直接执行 ALTER
-- ############################################################################
`

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
