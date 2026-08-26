// Package proto2sql 解析 .proto 源文件（读取本仓库 proto2mysql_option.proto 的建表相关 message option），
// 复用 github.com/luyuancpp/proto2mysql 的 SQL 生成内核，产出 CREATE TABLE 语句。
//
// 与 proto2mysql 的分工：proto2mysql 是运行时库（吃编译好的 Go proto 类型、连库执行）；
// 本模块是构建期/离线工具（吃 .proto 源文件、产出 .sql 文件），依赖单向（proto2sql -> proto2mysql），
// 类型映射规则只有一份、不会漂移。
package proto2sql

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/luyuancpp/proto2mysql"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Config 生成配置。
type Config struct {
	// ProtoFiles 要编译的 .proto 文件（可为相对/绝对路径；其所在目录会自动加入 import 搜索路径）。
	ProtoFiles []string
	// ImportPaths 额外的 import 搜索目录（用于定位被 import 的 proto 文件和 descriptor.proto 等）。
	ImportPaths []string
	// Drop 为 true 时，在每条 CREATE TABLE 前加 DROP TABLE IF EXISTS。
	// ⚠️ 破坏性：DROP 会删除整张表及其全部数据。仅用于空库初始化 / 测试库重建，
	// 切勿用于生产库或服务器启动流程。要在保留数据的前提下演进结构，请用运行时库的
	// DB.SyncAllTables / DB.GenerateMigrationSQL（走 ALTER，不删数据）。
	Drop bool
	// RequireDBOption 为 true 时，只处理声明了文件级选项 option (proto2mysql.db) = true; 的
	// .proto 文件（与运行时 DB.RegisterAllTables 的筛选规则一致）；未声明的文件整体跳过。
	// 默认 false：处理全部输入文件里带 table_name 的消息。
	RequireDBOption bool
}

// Table 单张表的生成结果。
type Table struct {
	Name string // 表名（来自 OptionTableName）
	SQL  string // 该表的建表 SQL（含末尾分号；Drop 开启时含前置 DROP 语句）
}

type tableOrigin struct {
	name    string
	message protoreflect.FullName
}

// Generate 编译 cfg.ProtoFiles，为每个带 table_name 选项的 message 生成建表 SQL，按表名排序返回。
// 若 cfg.RequireDBOption 为 true，则只处理声明了文件级 option (proto2mysql.db) = true; 的文件。
func Generate(ctx context.Context, cfg Config) ([]Table, error) {
	filenames, importPaths := resolveInputs(cfg.ProtoFiles, cfg.ImportPaths)
	if err := checkInputShadowing(cfg.ProtoFiles, filenames, importPaths); err != nil {
		return nil, err
	}

	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			ImportPaths: importPaths,
		}),
		SourceInfoMode: protocompile.SourceInfoStandard,
	}

	files, err := compiler.Compile(ctx, filenames...)
	if err != nil {
		return nil, fmt.Errorf("compile proto: %w", err)
	}

	var tables []Table
	// seen 记的是「这个 table_name 是哪个 message 先声明的」，不只是"见过没有"——
	// 冲突时要把两边的 message 全名都报出来，否则用户只知道少了一张表。
	// 用有序切片而不是 map：除了要按 EqualFold 检查跨大小写碰撞，冲突错误也必须
	// 与编译输入顺序一致，不能因为 map 迭代随机而每次点名不同的一对 message。
	var seen []tableOrigin
	for _, f := range files {
		// 与运行时一致：开启 RequireDBOption 时，只扫描声明了 db 文件选项的文件。
		if cfg.RequireDBOption && !proto2mysql.FileHasDBOption(f) {
			continue
		}
		if err := collectTables(f.Messages(), cfg.Drop, &seen, &tables); err != nil {
			return nil, err
		}
	}

	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })
	return tables, nil
}

// collectTables 遍历消息（含嵌套消息），把带表选项的消息生成建表 SQL 追加到 out。
// 表配置的读取与应用全部由 proto2mysql 内核完成（TableOptionsFromDescriptor），此处只做筛选。
//
// 重复的 table_name **报错，不再静默 first-wins**。原先是 `ok && !seen[name]`：
// 两个 message 声明同一个表名时，后一个被条件直接跳过，全程无 log 无 error——
// 而这几乎总是配错了（复制粘贴、merge 冲突），产出的 schema 只按先遍历到的那份建，
// 另一份定义凭空消失，排查时根本看不出发生过什么。
func collectTables(msgs protoreflect.MessageDescriptors, drop bool, seen *[]tableOrigin, out *[]Table) error {
	for i := 0; i < msgs.Len(); i++ {
		md := msgs.Get(i)

		if name, ok := proto2mysql.TableNameFromDescriptor(md); ok {
			for _, prev := range *seen {
				if !strings.EqualFold(prev.name, name) {
					continue
				}
				if prev.name == name {
					return fmt.Errorf("table_name %q 被多个 message 声明：%s 与 %s。"+
						"两份定义只会有一份落进 schema，另一份被静默丢弃；"+
						"请改掉其中一个的 table_name，或删掉重复的 message",
						name, prev.message, md.FullName())
				}
				return fmt.Errorf("table_name %q 与 %q 在大小写不敏感的文件系统/数据库中冲突："+
					"message %s 与 %s 不能生成两份可移植的 schema；请为其中一张表换一个不只靠大小写区分的名字",
					prev.name, name, prev.message, md.FullName())
			}
			*seen = append(*seen, tableOrigin{name: name, message: md.FullName()})
			sql, err := buildTableSQL(md, name, drop)
			if err != nil {
				return err
			}
			*out = append(*out, Table{Name: name, SQL: sql})
		}

		// 递归嵌套消息（跳过 map entry 合成类型）
		if err := collectTables(md.Messages(), drop, seen, out); err != nil {
			return err
		}
	}
	return nil
}

// buildTableSQL 用 proto2mysql 的 SQL 生成内核产出建表语句（表选项自动从描述符读取）。
//
// 先 ValidateTableMessage 再生成：不然一个没有 MySQL 映射的字段（sint32/fixed64…）
// 会被静默按 TEXT 写进 .sql，那份脚本执行出来的表**类型是错的**，
// 事后改回正确类型是跨族 MODIFY，会把数据吃成 0。运行时路径早就在建表前挡了，
// 离线这条路一直没挡——同一份 proto，两条路的结论不一样。
func buildTableSQL(md protoreflect.MessageDescriptor, tableName string, drop bool) (string, error) {
	msg := dynamicpb.NewMessage(md)
	if err := proto2mysql.ValidateTableMessage(msg); err != nil {
		return "", fmt.Errorf("message %s: %w", md.FullName(), err)
	}
	sql := proto2mysql.GenerateCreateTableSQL(msg)
	if drop {
		sql = fmt.Sprintf("DROP TABLE IF EXISTS `%s`;\n%s", strings.ReplaceAll(tableName, "`", "``"), sql)
	}
	return sql, nil
}

// resolveInputs 把 proto 文件路径归一化为“相对 import 路径的文件名 + 搜索目录”。
func resolveInputs(protoFiles, importPaths []string) (filenames, paths []string) {
	paths = append(paths, importPaths...)
	seenDir := make(map[string]bool)
	for _, p := range paths {
		seenDir[p] = true
	}
	for _, pf := range protoFiles {
		dir := filepath.Dir(pf)
		if !seenDir[dir] {
			paths = append(paths, dir)
			seenDir[dir] = true
		}
		filenames = append(filenames, filepath.Base(pf))
	}
	return filenames, paths
}

// checkInputShadowing 用户点名的 .proto 会不会被搜索路径里的**同名文件**顶掉。
//
// resolveInputs 把 `-proto a/b/game.proto` 降成裸文件名 "game.proto"，再把 a/b 追加到
// 搜索路径**末尾**。protocompile 的 SourceResolver 按 ImportPaths 顺序取第一个命中的，
// 所以只要任何一个 `-I` 目录里也有 game.proto，编译的就是**那一个**——
// 用户明明写了路径，产出的却是另一个文件的表结构，而且完全没有提示。
// 两个 `-proto` 传了不同目录下的同名文件时同理：后一个被前一个静默吞掉。
//
// 这里不去改解析顺序（那会连带影响 import 的解析语义），而是**先算一遍谁会赢**，
// 赢的不是用户点名的那个就报错，把路径两边都打出来。
func checkInputShadowing(protoFiles, filenames, paths []string) error {
	for i, pf := range protoFiles {
		want, err := filepath.Abs(pf)
		if err != nil {
			continue // 算不出绝对路径就不判，别把正常流程搞挂
		}
		for _, dir := range paths {
			candidate := filepath.Join(dir, filenames[i])
			abs, err := filepath.Abs(candidate)
			if err != nil {
				continue
			}
			if _, statErr := os.Stat(abs); statErr != nil {
				continue // 这个搜索目录里没有同名文件
			}
			if !sameFile(abs, want) {
				return fmt.Errorf("输入文件被同名文件顶掉：你写的是 %s，"+
					"但按 import 搜索路径顺序实际会编译 %s。\n"+
					"  编译器只认文件名（%s），先命中的搜索目录赢。\n"+
					"  改法：去掉冲突的 -I，或把两个文件改成不同的文件名/放进不同的 package 目录并用相对路径引用",
					pf, abs, filenames[i])
			}
			break // 第一个命中的就是赢家，且它正是用户点名的那个
		}
	}
	return nil
}

// sameFile 判断两个路径是不是同一个文件（跟随符号链接、忽略大小写差异等平台细节）。
func sameFile(a, b string) bool {
	fa, err := os.Stat(a)
	if err != nil {
		return false
	}
	fb, err := os.Stat(b)
	if err != nil {
		return false
	}
	return os.SameFile(fa, fb)
}
