package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDropModeBannerContent -drop 的警告必须跟着生成物走。
//
// 光在 CLI 帮助和文档里写警告是不够的——真正的风险是**生成出来的 .sql 文件被别人
// 拿去执行**：它看起来就是一份普通的建表脚本，`mysql < schema.sql` 一敲，整库蒸发。
func TestDropModeBannerContent(t *testing.T) {
	for _, want := range []string{
		"DROP TABLE IF EXISTS",
		"切勿",
		"GenerateMigrationSQL", // 指出安全的替代路径
	} {
		if !strings.Contains(dropModeBanner, want) {
			t.Errorf("横幅缺少 %q:\n%s", want, dropModeBanner)
		}
	}
	if !strings.HasPrefix(dropModeBanner, "-- ") {
		t.Error("横幅必须整体是 SQL 注释，否则灌库时会语法错误")
	}
	for _, line := range strings.Split(strings.TrimRight(dropModeBanner, "\n"), "\n") {
		if !strings.HasPrefix(line, "--") {
			t.Errorf("横幅每一行都必须是 SQL 注释，这行不是: %q", line)
		}
	}
}

// TestCheckOutputNameBlocksTraversal -single 是命令行传的，表名来自 .proto 的
// table_name 选项——对生成器来说同样是外部输入。两处原先都直接
// filepath.Join(outDir, name)，而 Join 会把 ".." 规规矩矩地解析掉，
// 于是 -out 这道边界形同虚设，文件写到谁也没想到的地方去。
func TestCheckOutputNameBlocksTraversal(t *testing.T) {
	bad := []string{
		"",
		"..",
		".",
		"../evil.sql",
		"../../etc/passwd",
		"sub/dir.sql",
		`sub\dir.sql`,
		"/abs.sql",
		"CON.sql",
		"nul.backup.sql",
		"COM1.sql",
		"lpt9.archive.sql",
		"bad:name.sql",
		"bad?name.sql",
		"bad|name.sql",
	}
	for _, name := range bad {
		if err := checkOutputName(name); err == nil {
			t.Errorf("checkOutputName(%q) 应当拒绝", name)
		}
	}

	good := []string{"schema.sql", "account.sql", "a-b_c.1.sql", "中文表.sql", "console.sql", "com10.sql"}
	for _, name := range good {
		if err := checkOutputName(name); err != nil {
			t.Errorf("checkOutputName(%q) 不该拒绝: %v", name, err)
		}
	}
}

// TestSplitCSVAllKeepsEveryOccurrence 帮助里一直写着"也可重复用 -proto"，
// 而 flag.StringVar 是最后一次覆盖前面所有次——照文档写
// `-proto a.proto -proto b.proto` 的人只会拿到 b.proto 的表，
// a.proto 那些表凭空少了且没有任何提示。
func TestSplitCSVAllKeepsEveryOccurrence(t *testing.T) {
	got := splitCSVAll([]string{"a.proto", "b.proto,c.proto", " d.proto "})
	want := []string{"a.proto", "b.proto", "c.proto", "d.proto"}
	if len(got) != len(want) {
		t.Fatalf("splitCSVAll = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("splitCSVAll = %v, want %v（顺序也要保持命令行顺序）", got, want)
		}
	}
}

// TestWriteFileAtomicLeavesNoTemp 写成功后目录里不该留下 .tmp 残渣，
// 且内容是完整覆盖而不是追加。
func TestWriteFileAtomicLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.sql")

	if err := os.WriteFile(path, []byte("OLD-CONTENT-MUCH-LONGER"), 0o644); err != nil {
		t.Fatalf("预置旧文件: %v", err)
	}
	if err := writeFileAtomic(path, []byte("new")); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("回读: %v", err)
	}
	if string(data) != "new" {
		t.Errorf("内容 = %q, want \"new\"", data)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("列目录: %v", err)
	}
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp") {
			t.Errorf("留下了临时文件残渣: %s", e.Name())
		}
	}
}

// TestWriteFilesAtomicPreparationFailureKeepsEveryOldFile 准备第 2 个文件失败时，
// 第 1 个文件虽然已经成功写进临时文件，也绝不能提前替换线上旧文件。
func TestWriteFilesAtomicPreparationFailureKeepsEveryOldFile(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.sql")
	second := filepath.Join(dir, "missing", "second.sql") // 父目录不存在，CreateTemp 必然失败

	const oldFirst = "OLD-FIRST-CONTENT"
	if err := os.WriteFile(first, []byte(oldFirst), 0o644); err != nil {
		t.Fatalf("预置第一个旧文件: %v", err)
	}

	err := writeFilesAtomic([]outFile{
		{path: first, data: []byte("new first")},
		{path: second, data: []byte("new second")},
	})
	if err == nil {
		t.Fatal("后一个文件无法准备时，批量写入必须失败")
	}

	got, err := os.ReadFile(first)
	if err != nil {
		t.Fatalf("回读第一个旧文件: %v", err)
	}
	if string(got) != oldFirst {
		t.Fatalf("准备阶段失败却改写了第一个文件：got %q, want %q", got, oldFirst)
	}
	if _, err := os.Stat(second); !os.IsNotExist(err) {
		t.Fatalf("准备失败的第二个目标不该存在，stat err = %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取输出目录: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp") {
			t.Errorf("准备失败后留下临时文件: %s", entry.Name())
		}
	}
}

// TestWriteFilesAtomicInstallFailureRestoresEveryOldFile 模拟所有临时文件都准备好后，
// 第 2 个安装 rename 失败。第 1 个已经安装的新文件必须撤掉，两份旧文件都要逐字节恢复。
func TestWriteFilesAtomicInstallFailureRestoresEveryOldFile(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.sql")
	second := filepath.Join(dir, "second.sql")

	const oldFirst = "OLD-FIRST-CONTENT"
	const oldSecond = "OLD-SECOND-CONTENT"
	if err := os.WriteFile(first, []byte(oldFirst), 0o644); err != nil {
		t.Fatalf("预置第一个旧文件: %v", err)
	}
	if err := os.WriteFile(second, []byte(oldSecond), 0o644); err != nil {
		t.Fatalf("预置第二个旧文件: %v", err)
	}

	installErr := errors.New("模拟第二个目标安装失败")
	rename := func(oldPath, newPath string) error {
		if strings.Contains(filepath.Base(oldPath), ".tmp") && newPath == second {
			return installErr
		}
		return os.Rename(oldPath, newPath)
	}

	err := writeFilesAtomicWithRename([]outFile{
		{path: first, data: []byte("new first")},
		{path: second, data: []byte("new second")},
	}, rename)
	if !errors.Is(err, installErr) {
		t.Fatalf("应返回第二个目标的安装错误，实际: %v", err)
	}

	for path, want := range map[string]string{first: oldFirst, second: oldSecond} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("回读旧文件 %s: %v", path, err)
		}
		if string(got) != want {
			t.Errorf("安装失败后 %s = %q, want %q", path, got, want)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取输出目录: %v", err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp") || strings.Contains(entry.Name(), ".bak") {
			t.Errorf("回滚成功后留下中间文件: %s", entry.Name())
		}
	}
}

// TestWriteFilesAtomicReportsRollbackFailure 回滚本身失败时不能吞掉错误，更不能删除
// 尚未恢复的旧文件备份；返回值必须同时保留最初的安装错误和回滚错误。
func TestWriteFilesAtomicReportsRollbackFailure(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.sql")
	second := filepath.Join(dir, "second.sql")

	const oldFirst = "OLD-FIRST-CONTENT"
	const oldSecond = "OLD-SECOND-CONTENT"
	if err := os.WriteFile(first, []byte(oldFirst), 0o644); err != nil {
		t.Fatalf("预置第一个旧文件: %v", err)
	}
	if err := os.WriteFile(second, []byte(oldSecond), 0o644); err != nil {
		t.Fatalf("预置第二个旧文件: %v", err)
	}

	installErr := errors.New("模拟第二个目标安装失败")
	rollbackErr := errors.New("模拟第一个目标恢复失败")
	rename := func(oldPath, newPath string) error {
		base := filepath.Base(oldPath)
		switch {
		case strings.Contains(base, ".tmp") && newPath == second:
			return installErr
		case strings.Contains(base, ".bak") && newPath == first:
			return rollbackErr
		default:
			return os.Rename(oldPath, newPath)
		}
	}

	err := writeFilesAtomicWithRename([]outFile{
		{path: first, data: []byte("new first")},
		{path: second, data: []byte("new second")},
	}, rename)
	if !errors.Is(err, installErr) || !errors.Is(err, rollbackErr) {
		t.Fatalf("返回错误必须同时包含安装失败和回滚失败，实际: %v", err)
	}

	gotSecond, err := os.ReadFile(second)
	if err != nil {
		t.Fatalf("第二个旧文件应成功恢复: %v", err)
	}
	if string(gotSecond) != oldSecond {
		t.Fatalf("第二个旧文件 = %q, want %q", gotSecond, oldSecond)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取输出目录: %v", err)
	}
	var backups []string
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp") {
			t.Errorf("错误返回后不该留下临时新文件: %s", entry.Name())
		}
		if strings.Contains(entry.Name(), ".bak") {
			backups = append(backups, filepath.Join(dir, entry.Name()))
		}
	}
	if len(backups) != 1 {
		t.Fatalf("恢复失败的旧文件必须保留唯一备份，实际备份: %v", backups)
	}
	gotBackup, err := os.ReadFile(backups[0])
	if err != nil {
		t.Fatalf("读取残留备份: %v", err)
	}
	if string(gotBackup) != oldFirst {
		t.Fatalf("残留备份 = %q, want %q", gotBackup, oldFirst)
	}
}

// TestWriteFilesAtomicCleanupFailureIsCommitted commit point 是所有 tmp 都成功安装之后。
// 此后删除旧备份失败不能伪装成“整批写入失败”，也不能清理到第一处错误就停；
// 调用方需要能识别“新文件已提交，只是备份清理不完整”并继续报告生成成功。
func TestWriteFilesAtomicCleanupFailureIsCommitted(t *testing.T) {
	dir := t.TempDir()
	first := filepath.Join(dir, "first.sql")
	second := filepath.Join(dir, "second.sql")
	if err := os.WriteFile(first, []byte("old first"), 0o644); err != nil {
		t.Fatalf("预置第一个旧文件: %v", err)
	}
	if err := os.WriteFile(second, []byte("old second"), 0o644); err != nil {
		t.Fatalf("预置第二个旧文件: %v", err)
	}

	firstCleanupErr := errors.New("模拟 first 备份清理失败")
	secondCleanupErr := errors.New("模拟 second 备份清理失败")
	removeFile := func(path string) error {
		base := filepath.Base(path)
		switch {
		case strings.HasPrefix(base, "first.sql.bak"):
			return firstCleanupErr
		case strings.HasPrefix(base, "second.sql.bak"):
			return secondCleanupErr
		default:
			return os.Remove(path)
		}
	}

	err := writeFilesAtomicWithOps([]outFile{
		{path: first, data: []byte("new first")},
		{path: second, data: []byte("new second")},
	}, os.Rename, removeFile)
	var committedErr *committedCleanupError
	if !errors.As(err, &committedErr) {
		t.Fatalf("backup cleanup 失败必须返回可区分的 committed 错误，实际: %v", err)
	}
	if !errors.Is(err, firstCleanupErr) || !errors.Is(err, secondCleanupErr) {
		t.Fatalf("必须尝试清理全部备份并汇总每个错误，实际: %v", err)
	}

	for path, want := range map[string]string{first: "new first", second: "new second"} {
		got, readErr := os.ReadFile(path)
		if readErr != nil {
			t.Fatalf("commit point 之后目标必须存在 %s: %v", path, readErr)
		}
		if string(got) != want {
			t.Fatalf("cleanup 失败不能回滚已提交目标 %s = %q, want %q", path, got, want)
		}
	}

	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatalf("读取目录: %v", readErr)
	}
	var backups int
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".bak") {
			backups++
		}
	}
	if backups != 2 {
		t.Fatalf("两个失败清理的备份都应保留供人工恢复，实际 %d 个", backups)
	}
}
