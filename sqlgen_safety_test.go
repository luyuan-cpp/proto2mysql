package proto2mysql

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TestGenerateCreateTableSQLFailsClosedForUnsupportedKinds 老的 string-only API 没有 error 位，
// 也不能继续静默把未知 kind 回退成 TEXT；失败时返回空串，checked API 给出可识别错误。
func TestGenerateCreateTableSQLFailsClosedForUnsupportedKinds(t *testing.T) {
	msg := dynamicpb.NewMessage(unsupportedKindDescriptor(t))
	if got := GenerateCreateTableSQL(msg, WithTableName("unsupported_probe")); got != "" {
		t.Fatalf("string-only API 遇到不支持类型必须 fail-closed，实际生成:\n%s", got)
	}
	if _, err := GenerateCreateTableSQLChecked(msg, WithTableName("unsupported_probe")); !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("checked API 应返回 ErrUnsupportedFieldKind，实际: %v", err)
	}
}

func TestWriteCreateTableSQLRejectsUnsupportedKinds(t *testing.T) {
	msg := dynamicpb.NewMessage(unsupportedKindDescriptor(t))
	pdb := NewDB()
	pdb.RegisterTable(msg, WithTableName("unsupported_probe"))
	if err := pdb.WriteCreateTableSQL(&discardWriter{}); !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("批量离线输出必须在写出坏 DDL 前失败，实际: %v", err)
	}
}

func TestValidateTableOptionsRejectsUnknownFieldReferences(t *testing.T) {
	tests := []struct {
		name string
		opts []TableOption
	}{
		{"primary key", []TableOption{WithPrimaryKey("missing"), WithAutoIncrementKey("")}},
		{"ordinary index", []TableOption{WithIndexes("missing")}},
		{"unique key", []TableOption{WithUniqueKey("missing")}},
		{"nullable", []TableOption{WithNullableFields("missing")}},
		{"auto increment", []TableOption{WithPrimaryKey("id"), WithAutoIncrementKey("missing")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateTableMessage(&testpb.GolangTest{}, tc.opts...); err == nil {
				t.Fatalf("TableOption 引用不存在字段时必须在产出 DDL 前失败: %+v", tc.opts)
			}
		})
	}
}

func TestValidateTableOptionsRejectsInvalidAutoIncrement(t *testing.T) {
	tests := []struct {
		name string
		opts []TableOption
	}{
		{"non integer", []TableOption{WithPrimaryKey("ip"), WithAutoIncrementKey("ip")}},
		{"not indexed", []TableOption{WithPrimaryKey("id"), WithAutoIncrementKey("port")}},
		{"not first in key", []TableOption{WithPrimaryKey("id", "port"), WithAutoIncrementKey("port")}},
		{"nullable", []TableOption{WithPrimaryKey("id"), WithAutoIncrementKey("id"), WithNullableFields("id")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := GenerateCreateTableSQLChecked(&testpb.GolangTest{}, tc.opts...); err == nil {
				t.Fatal("非法 AUTO_INCREMENT 配置不得生成 DDL")
			}
		})
	}

	if _, err := GenerateCreateTableSQLChecked(&testpb.GolangTest{},
		WithPrimaryKey("id"), WithAutoIncrementKey("id")); err != nil {
		t.Fatalf("整数列作为首个主键列是合法 AUTO_INCREMENT 配置: %v", err)
	}
}

func TestValidateTableOptionsRejectsNullablePrimaryKeyWithoutAutoIncrement(t *testing.T) {
	_, err := GenerateCreateTableSQLChecked(&testpb.GolangTest{},
		WithPrimaryKey("port"), WithAutoIncrementKey(""), WithNullableFields("port"))
	if err == nil {
		t.Fatal("非自增主键也不能声明 nullable；MySQL 会强制改回 NOT NULL 并造成永久 schema drift")
	}
}

func TestValidateTableOptionsRejectsImplicitlyNullableOrPrefixPrimaryKey(t *testing.T) {
	tests := []struct {
		name string
		msg  gproto.Message
		opts []TableOption
	}{
		{
			name: "text primary key is only prefix unique",
			msg:  &testpb.GolangTest{},
			opts: []TableOption{WithPrimaryKey("ip"), WithAutoIncrementKey("")},
		},
		{
			name: "timestamp primary key is implicitly nullable",
			msg:  dynamicpb.NewMessage(timestampProbeDescriptor(t)),
			opts: []TableOption{WithPrimaryKey("ts")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GenerateCreateTableSQLChecked(tc.msg, tc.opts...)
			if !errors.Is(err, ErrInvalidTableOption) {
				t.Fatalf("不能完整、稳定映射的主键必须在 DDL 前拒绝，实际: %v", err)
			}
		})
	}
}

func TestValidateTableOptionsRejectsEmptyTableName(t *testing.T) {
	for _, name := range []string{"", "   "} {
		_, err := GenerateCreateTableSQLChecked(&testpb.GolangTest{}, WithTableName(name))
		if !errors.Is(err, ErrInvalidTableOption) {
			t.Fatalf("空 table_name %q 必须在 DDL 前拒绝，实际: %v", name, err)
		}
	}
}

func TestValidateTableOptionsRejectsMalformedKeyDefinitions(t *testing.T) {
	tests := []struct {
		name string
		opts []TableOption
	}{
		{"primary key empty component", []TableOption{WithPrimaryKey("id", "", "port"), WithAutoIncrementKey("")}},
		{"index leading empty component", []TableOption{WithIndexes(",id")}},
		{"index middle empty component", []TableOption{WithIndexes("id,,port")}},
		{"unique key leading empty component", []TableOption{WithUniqueKey(",id")}},
		{"unique key middle empty component", []TableOption{WithUniqueKey("id,,port")}},
		{"primary key duplicate field", []TableOption{WithPrimaryKey("id", "id"), WithAutoIncrementKey("")}},
		{"index duplicate field", []TableOption{WithIndexes("id,id")}},
		{"unique key duplicate field", []TableOption{WithUniqueKey("id, id")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := GenerateCreateTableSQLChecked(&testpb.GolangTest{}, tc.opts...)
			if !errors.Is(err, ErrInvalidTableOption) {
				t.Fatalf("必失败的键定义必须在生成 DDL 前返回 ErrInvalidTableOption，实际: %v", err)
			}
		})
	}
}

// TestWriteCreateTableSQLRejectsDuplicatePhysicalTableBeforeWrite 不同 proto message
// 可以分别注册成功，但不能在离线脚本里竞争同一张物理表；CREATE IF NOT EXISTS 会让
// 第一份随机胜出、后一份静默 no-op。
func TestWriteCreateTableSQLRejectsDuplicatePhysicalTableBeforeWrite(t *testing.T) {
	pdb := NewDB()
	pdb.RegisterTable(&testpb.GolangTest{}, WithTableName("shared_output"))
	pdb.RegisterTable(&testpb.GolangTest1{}, WithTableName("shared_output"))

	var out bytes.Buffer
	err := pdb.WriteCreateTableSQL(&out)
	if !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("不同 message 映射同一物理表必须返回 ErrDuplicateTableMapping，实际: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("重复映射必须在写入前拒绝，实际写了 %d 字节", out.Len())
	}
	for _, want := range []string{
		string((&testpb.GolangTest{}).ProtoReflect().Descriptor().FullName()),
		string((&testpb.GolangTest1{}).ProtoReflect().Descriptor().FullName()),
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误应列出冲突 message %s: %v", want, err)
		}
	}
}

func TestWriteCreateTableSQLRejectsCaseInsensitivePhysicalTableCollision(t *testing.T) {
	pdb := NewDB()
	pdb.RegisterTable(&testpb.GolangTest{}, WithTableName("Users"))
	pdb.RegisterTable(&testpb.GolangTest1{}, WithTableName("users"))

	var out bytes.Buffer
	err := pdb.WriteCreateTableSQL(&out)
	if !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("大小写不敏感环境会碰撞的物理表必须拒绝，实际: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("大小写碰撞必须在 writer 前拒绝，实际写了 %d 字节", out.Len())
	}
	for _, want := range []string{
		"Users",
		"users",
		string((&testpb.GolangTest{}).ProtoReflect().Descriptor().FullName()),
		string((&testpb.GolangTest1{}).ProtoReflect().Descriptor().FullName()),
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误应列出两边物理表名和 registry key，缺少 %q: %v", want, err)
		}
	}
}

// TestWriteCreateTableSQLFindsNonAdjacentUnicodeFoldCollision
// Kelvin sign K 与 ASCII K 属于同一个 Unicode simple-fold 等价类；中间插入 Z 可证明
// 判重不能只扫描相邻项，否则保持历史原始表名排序时会漏掉这对冲突。
func TestWriteCreateTableSQLFindsNonAdjacentUnicodeFoldCollision(t *testing.T) {
	pdb := NewDB()
	pdb.RegisterTable(&testpb.GolangTest{}, WithTableName("Keys"))
	pdb.RegisterTable(&testpb.GolangTest1{}, WithTableName("Zulu"))
	pdb.RegisterTable(&testpb.GolangTest2{}, WithTableName("Keys"))

	var out bytes.Buffer
	err := pdb.WriteCreateTableSQL(&out)
	if !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("Unicode fold 碰撞必须在确定性排序后拒绝，实际: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("Unicode fold 碰撞必须在 writer 前拒绝，实际写了 %d 字节", out.Len())
	}
}

// TestWriteCreateTableSQLDuplicateErrorIsDeterministic 注册表来自 map；即使有三方冲突，
// 错误也必须固定列出排序后的同一对，不能随 map 迭代顺序漂移。
func TestWriteCreateTableSQLDuplicateErrorIsDeterministic(t *testing.T) {
	pdb := NewDB()
	pdb.RegisterTable(&testpb.GolangTest{}, WithTableName("shared_output"))
	pdb.RegisterTable(&testpb.GolangTest1{}, WithTableName("shared_output"))
	pdb.RegisterTable(&testpb.GolangTest2{}, WithTableName("shared_output"))

	var first string
	for i := 0; i < 50; i++ {
		err := pdb.WriteCreateTableSQL(&bytes.Buffer{})
		if !errors.Is(err, ErrDuplicateTableMapping) {
			t.Fatalf("第 %d 次未返回重复映射错误: %v", i, err)
		}
		if i == 0 {
			first = err.Error()
			continue
		}
		if err.Error() != first {
			t.Fatalf("重复映射错误不确定：第一次 %q，第 %d 次 %q", first, i, err)
		}
	}
}

// TestWriteMigrationSQLRejectsDuplicatePhysicalTableBeforeDBIO 迁移生成会先读
// information_schema；重复映射必须在第一条查询和第一个 writer byte 之前统一拒绝。
func TestWriteMigrationSQLRejectsDuplicatePhysicalTableBeforeDBIO(t *testing.T) {
	pdb, conn := newFakeDB(t)
	pdb.RegisterTable(&testpb.GolangTest{}, WithTableName("shared_migration"))
	pdb.RegisterTable(&testpb.GolangTest1{}, WithTableName("shared_migration"))

	var out bytes.Buffer
	err := pdb.WriteMigrationSQL(&out, &testpb.GolangTest{}, &testpb.GolangTest1{})
	if !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("不同 message 映射同一迁移表必须返回 ErrDuplicateTableMapping，实际: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("重复映射必须在 writer 写入前拒绝，实际写了 %d 字节", out.Len())
	}
	if got := conn.sqls(); len(got) != 0 {
		t.Fatalf("重复映射必须在任何 DB I/O 前拒绝，实际执行了: %v", got)
	}
}

func TestWriteMigrationSQLRejectsCaseInsensitiveCollisionBeforeDBIO(t *testing.T) {
	pdb, conn := newFakeDB(t)
	pdb.RegisterTable(&testpb.GolangTest{}, WithTableName("Users"))
	pdb.RegisterTable(&testpb.GolangTest1{}, WithTableName("users"))

	var out bytes.Buffer
	err := pdb.WriteMigrationSQL(&out, &testpb.GolangTest{}, &testpb.GolangTest1{})
	if !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("迁移生成必须拒绝大小写不敏感物理表碰撞，实际: %v", err)
	}
	if out.Len() != 0 || len(conn.sqls()) != 0 {
		t.Fatalf("碰撞必须在 writer/DB I/O 前拒绝：bytes=%d sql=%v", out.Len(), conn.sqls())
	}
}

func TestWriteMigrationSQLRejectsRepeatedMessageBeforeDBIO(t *testing.T) {
	pdb, conn := newFakeDB(t)
	msg := &testpb.GolangTest{}

	var out bytes.Buffer
	err := pdb.WriteMigrationSQL(&out, msg, msg)
	if !errors.Is(err, ErrDuplicateTableMapping) {
		t.Fatalf("同一 message 重复传入会生成重复 ALTER，必须在生成前拒绝，实际: %v", err)
	}
	if out.Len() != 0 || len(conn.sqls()) != 0 {
		t.Fatalf("重复 message 必须在 writer/DB I/O 前拒绝：bytes=%d sql=%v", out.Len(), conn.sqls())
	}
}

type discardWriter struct{}

func (*discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func sqlgenUnsupportedMessage(t *testing.T) *dynamicpb.Message {
	t.Helper()
	fd := &descriptorpb.FileDescriptorProto{
		Name:    gproto.String("sqlgen_unsupported.proto"),
		Package: gproto.String("sqlgenprobe"),
		Syntax:  gproto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: gproto.String("Unsupported"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   gproto.String("zig"),
				Number: gproto.Int32(1),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_SINT64.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			}},
		}},
	}
	desc, err := protodesc.NewFile(fd, nil)
	if err != nil {
		t.Fatalf("构造不支持类型描述符: %v", err)
	}
	return dynamicpb.NewMessage(desc.Messages().Get(0))
}

// TestWriteMigrationSQLGenerationFailureWritesNothing 第一表能完整生成 CREATE，第二表
// 因不支持的字段类型失败；调用方看到 error 时，writer 必须仍是零字节而不是半套迁移。
func TestWriteMigrationSQLGenerationFailureWritesNothing(t *testing.T) {
	pdb, conn := newFakeDB(t)
	invalid := sqlgenUnsupportedMessage(t)
	pdb.RegisterTable(invalid, WithTableName("z_invalid"))
	conn.queueRows(rows(row(int64(0)))) // 第一张合法表不存在，因此会生成 CREATE TABLE

	var out bytes.Buffer
	err := pdb.WriteMigrationSQL(&out, &testpb.GolangTest{}, invalid)
	if !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("第二表应返回 ErrUnsupportedFieldKind，实际: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("完整生成失败时 writer 必须零字节，实际写入 %d 字节:\n%s", out.Len(), out.String())
	}
}

// TestDumpMigrationSQLFileGenerationFailureKeepsOldTarget 与 writer seam 相同的失败，
// 落到文件 API 时不能因为提前 os.Create 而先把已有 migrate.sql 截成空文件。
func TestDumpMigrationSQLFileGenerationFailureKeepsOldTarget(t *testing.T) {
	pdb, conn := newFakeDB(t)
	invalid := sqlgenUnsupportedMessage(t)
	pdb.RegisterTable(invalid, WithTableName("z_invalid"))
	conn.queueRows(rows(row(int64(0))))

	dir := t.TempDir()
	path := filepath.Join(dir, "migrate.sql")
	const oldContent = "-- OLD MIGRATION MUST SURVIVE\n"
	if err := os.WriteFile(path, []byte(oldContent), 0o644); err != nil {
		t.Fatalf("预置旧迁移文件: %v", err)
	}

	err := pdb.DumpMigrationSQLFile(path, &testpb.GolangTest{}, invalid)
	if !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("第二表应返回 ErrUnsupportedFieldKind，实际: %v", err)
	}
	assertFileContent(t, path, oldContent)
	assertNoSQLGenTempFiles(t, dir)
}

// TestDumpCreateTableSQLFileValidationFailureKeepsOldTarget 合法表按表名排在非法表前；
// 批量校验失败时，文件 API 不能在调用 WriteCreateTableSQL 前就截断旧 schema。
func TestDumpCreateTableSQLFileValidationFailureKeepsOldTarget(t *testing.T) {
	pdb := NewDB()
	pdb.RegisterTable(&testpb.GolangTest{}, WithTableName("a_valid"))
	pdb.RegisterTable(sqlgenUnsupportedMessage(t), WithTableName("z_invalid"))

	dir := t.TempDir()
	path := filepath.Join(dir, "schema.sql")
	const oldContent = "-- OLD SCHEMA MUST SURVIVE\n"
	if err := os.WriteFile(path, []byte(oldContent), 0o644); err != nil {
		t.Fatalf("预置旧建表文件: %v", err)
	}

	err := pdb.DumpCreateTableSQLFile(path)
	if !errors.Is(err, ErrUnsupportedFieldKind) {
		t.Fatalf("第二表应返回 ErrUnsupportedFieldKind，实际: %v", err)
	}
	assertFileContent(t, path, oldContent)
	assertNoSQLGenTempFiles(t, dir)
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("%s 内容 = %q, want %q", path, got, want)
	}
}

func assertNoSQLGenTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取目录 %s: %v", dir, err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), ".tmp") {
			t.Errorf("生成失败后留下临时文件: %s", entry.Name())
		}
	}
}
