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
	"errors"
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

type outFile struct {
	path, name string
	data       []byte
}

func main() {
	var (
		protoArgs  repeatedFlag
		importDirs repeatedFlag
		outDir     string
		single     string
		drop       bool
	)
	// -proto 用 Var 而不是 StringVar：帮助里一直写着"也可重复用 -proto"，
	// 而 flag.StringVar 是**最后一次覆盖前面所有次**——照文档写 `-proto a.proto -proto b.proto`
	// 的人，拿到的只有 b.proto 的表，a.proto 那些表凭空少了且没有任何提示。
	flag.Var(&protoArgs, "proto", "要编译的 .proto 文件，逗号分隔（也可重复用 -proto）")
	flag.Var(&importDirs, "I", "import 搜索目录（可重复）")
	flag.StringVar(&outDir, "out", ".", "输出目录")
	flag.StringVar(&single, "single", "", "合并输出到单个文件名（相对 -out，不得含路径分隔符）；不设则每表一个文件")
	flag.BoolVar(&drop, "drop", false, "⚠️ 破坏性：每条 CREATE TABLE 前加 DROP TABLE IF EXISTS（会删除表及全部数据，仅用于空库/测试初始化）")
	// -proto-file 保留为等价别名（历史用法）
	var protoRepeated repeatedFlag
	flag.Var(&protoRepeated, "proto-file", "要编译的 .proto 文件（可重复；等价于 -proto 的逐个形式）")
	flag.Parse()

	protoFiles := append([]string(protoRepeated), splitCSVAll(protoArgs)...)
	if len(protoFiles) == 0 {
		fmt.Fprintln(os.Stderr, "error: 至少需要一个 -proto 文件")
		flag.Usage()
		os.Exit(2)
	}
	if single != "" {
		if err := checkOutputName(single); err != nil {
			fmt.Fprintf(os.Stderr, "error: -single %q 不合法: %v\n", single, err)
			os.Exit(2)
		}
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
		if err := writeFileAtomic(path, []byte(b.String())); err != nil {
			var cleanupErr *committedCleanupError
			if !errors.As(err, &cleanupErr) {
				fmt.Fprintf(os.Stderr, "error: 写文件 %s 失败: %v\n", path, err)
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "warning: 文件 %s 已成功写入，但旧备份清理不完整: %v\n",
				path, cleanupErr.Unwrap())
		}
		fmt.Printf("生成 %d 张表 -> %s\n", len(tables), path)
		return
	}

	// 先把全部 (路径, 内容) 算好并校验完，再开始落盘。
	//
	// 原先是边生成边 os.WriteFile：第 k 张写失败（磁盘满 / 权限 / 表名非法）时
	// 前 k-1 张**已经覆盖掉旧文件**了，进程退出，输出目录留下一套新旧混杂的 .sql。
	// 这种目录被 `mysql < *.sql` 批量执行出来的是一份不一致的 schema。
	files := make([]outFile, 0, len(tables))
	for _, t := range tables {
		// 表名来自 .proto 的 table_name 选项，会被直接当文件名用——
		// 里头带 ../ 或路径分隔符就能写到 -out 之外去。
		if err := checkOutputName(t.Name + ".sql"); err != nil {
			fmt.Fprintf(os.Stderr, "error: 表名 %q 不能用作文件名: %v\n", t.Name, err)
			os.Exit(1)
		}
		files = append(files, outFile{
			path: filepath.Join(outDir, t.Name+".sql"),
			name: t.Name,
			data: []byte(banner + t.SQL + "\n"),
		})
	}

	if err := writeFilesAtomic(files); err != nil {
		var cleanupErr *committedCleanupError
		if !errors.As(err, &cleanupErr) {
			fmt.Fprintf(os.Stderr, "error: 写文件失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "warning: 全部 SQL 文件已成功写入，但旧备份清理不完整: %v\n",
			cleanupErr.Unwrap())
	}
	for _, f := range files {
		fmt.Printf("生成表 %s -> %s\n", f.name, f.path)
	}
}

// checkOutputName 校验一个"文件名"确实只是文件名，不会跑到 -out 目录外面去。
//
// 两个来源都不可全信：-single 是命令行传的，表名来自 .proto 的 table_name 选项
// （对生成器来说同样是外部输入，一份从别处拿来的 proto 就能带上 `../../etc/x`）。
// 原先两处都直接 filepath.Join(outDir, name)，而 Join 会把 ".." 规规矩矩地解析掉——
// 于是 -out 这道边界形同虚设，写出去的文件落在谁也没想到的地方。
func checkOutputName(name string) error {
	return proto2sql.ValidateOutputFileName(name)
}

// writeFileAtomic 保留单文件 API；实际准备与替换逻辑和批量路径共用一份实现。
func writeFileAtomic(path string, data []byte) error {
	return writeFilesAtomic([]outFile{{path: path, data: data}})
}

type stagedFile struct {
	target string
	tmp    string
	backup string
	new    bool
}

// committedCleanupError 表示所有新目标均已跨过 commit point，只有旧备份清理不完整。
// 调用方必须把它报告为 warning/已生成成功，不能提示用户“整批写入失败”后诱导重试。
type committedCleanupError struct {
	err error
}

func (e *committedCleanupError) Error() string {
	return fmt.Sprintf("files committed; backup cleanup incomplete: %v", e.err)
}

func (e *committedCleanupError) Unwrap() error { return e.err }

// writeFilesAtomic 先把所有文件完整写入各自目标目录里的临时文件；只有全部准备成功后，
// 才开始逐个 rename。这样磁盘满、目录不存在、写入或关闭失败都不会让输出目录出现
// 一部分新文件、一部分旧文件的混合状态。
//
// 同目录 rename 保证每个文件不会以半截内容出现。文件系统没有跨多个路径的原子 rename，
// 所以进程崩溃时无法保证整批原子。commit point 前观察到的错误会 best-effort 撤掉
// 本轮已安装的新文件并恢复旧备份；commit point 后只清理备份，清理失败返回
// committedCleanupError，让 CLI 报 warning 而不是误报整批写入失败。
func writeFilesAtomic(files []outFile) error {
	return writeFilesAtomicWithRename(files, os.Rename)
}

func writeFilesAtomicWithRename(files []outFile, renameFile func(string, string) error) error {
	return writeFilesAtomicWithOps(files, renameFile, os.Remove)
}

func writeFilesAtomicWithOps(
	files []outFile,
	renameFile func(string, string) error,
	removeFile func(string) error,
) error {
	staged := make([]stagedFile, 0, len(files))
	defer func() {
		for _, file := range staged {
			_ = removeFile(file.tmp)
		}
	}()

	for _, file := range files {
		tmpName, err := stageFile(file.path, file.data)
		if err != nil {
			return fmt.Errorf("准备文件 %s: %w", file.path, err)
		}
		staged = append(staged, stagedFile{target: file.path, tmp: tmpName})
	}

	// 全部临时文件准备好以后，先把既有目标移到同目录唯一备份。
	// 安装阶段因而可以在失败时恢复原文件；原本不存在的目标则在回滚时直接删除。
	for i := range staged {
		if _, err := os.Lstat(staged[i].target); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			cause := fmt.Errorf("检查旧文件 %s: %w", staged[i].target, err)
			return withRollbackError(cause, rollbackStagedFiles(staged, renameFile, removeFile))
		}

		backup, err := backupTarget(staged[i].target, renameFile)
		if err != nil {
			cause := fmt.Errorf("备份旧文件 %s: %w", staged[i].target, err)
			return withRollbackError(cause, rollbackStagedFiles(staged, renameFile, removeFile))
		}
		staged[i].backup = backup
	}

	for i := range staged {
		if err := renameFile(staged[i].tmp, staged[i].target); err != nil {
			cause := fmt.Errorf("替换文件 %s: %w", staged[i].target, err)
			return withRollbackError(cause, rollbackStagedFiles(staged, renameFile, removeFile))
		}
		staged[i].new = true
	}

	// commit point：到这里所有 tmp 都已安装为目标。后续只清理旧备份；无论 cleanup
	// 成败，都不能再把已提交结果描述成“整批写入失败”，也不能回滚部分目标。
	var cleanupErrs []error
	for i := range staged {
		if staged[i].backup == "" {
			continue
		}
		if err := removeFile(staged[i].backup); err != nil {
			cleanupErrs = append(cleanupErrs,
				fmt.Errorf("清理旧文件备份 %s: %w", staged[i].backup, err))
			continue
		}
		staged[i].backup = ""
	}
	if err := errors.Join(cleanupErrs...); err != nil {
		return &committedCleanupError{err: err}
	}
	return nil
}

func backupTarget(target string, renameFile func(string, string) error) (string, error) {
	dir := filepath.Dir(target)
	placeholder, err := os.CreateTemp(dir, filepath.Base(target)+".bak*")
	if err != nil {
		return "", err
	}
	backup := placeholder.Name()
	if err := placeholder.Close(); err != nil {
		_ = os.Remove(backup)
		return "", err
	}
	if err := renameFile(target, backup); err != nil {
		_ = os.Remove(backup)
		return "", err
	}
	return backup, nil
}

func rollbackStagedFiles(
	staged []stagedFile,
	renameFile func(string, string) error,
	removeFile func(string) error,
) error {
	var rollbackErrs []error
	for i := range staged {
		if staged[i].new {
			if err := removeFile(staged[i].target); err != nil && !os.IsNotExist(err) {
				rollbackErrs = append(rollbackErrs,
					fmt.Errorf("删除本轮新文件 %s: %w", staged[i].target, err))
			}
		}
	}
	for i := len(staged) - 1; i >= 0; i-- {
		if staged[i].backup != "" {
			if err := renameFile(staged[i].backup, staged[i].target); err != nil {
				rollbackErrs = append(rollbackErrs,
					fmt.Errorf("恢复旧文件 %s（备份 %s）: %w", staged[i].target, staged[i].backup, err))
				continue
			}
			staged[i].backup = ""
		}
	}
	return errors.Join(rollbackErrs...)
}

func withRollbackError(cause, rollbackErr error) error {
	if rollbackErr == nil {
		return cause
	}
	return errors.Join(cause, fmt.Errorf("回滚失败: %w", rollbackErr))
}

func stageFile(path string, data []byte) (string, error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return "", err
	}
	ok = true
	return tmpName, nil
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

// splitCSVAll 把重复传入的 -proto 逐个按逗号展开后拼起来（顺序保持命令行顺序）。
func splitCSVAll(values []string) []string {
	var out []string
	for _, v := range values {
		out = append(out, splitCSV(v)...)
	}
	return out
}
