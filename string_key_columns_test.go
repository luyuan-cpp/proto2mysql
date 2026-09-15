package proto2mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"github.com/luyuancpp/proto2mysql/pbopt"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// 主键/唯一键里 string/bytes 列（键列）的单元测试：列类型映射、max_length、表选项校验、
// 写入前的值校验与结构同步。真库上的行为见 string_key_integration_test.go。

const (
	probeString = descriptorpb.FieldDescriptorProto_TYPE_STRING
	probeBytes  = descriptorpb.FieldDescriptorProto_TYPE_BYTES
	probeUint64 = descriptorpb.FieldDescriptorProto_TYPE_UINT64
)

type keyProbeField struct {
	name     string
	typ      descriptorpb.FieldDescriptorProto_Type
	repeated bool
	opts     *descriptorpb.FieldOptions
}

// keyProbeDescriptor 现造一个 proto3 消息描述符，字段号按声明顺序从 1 开始。
func keyProbeDescriptor(t *testing.T, pkg string, fields ...keyProbeField) protoreflect.MessageDescriptor {
	t.Helper()
	msg := &descriptorpb.DescriptorProto{Name: proto.String("key_probe")}
	for i, f := range fields {
		label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
		if f.repeated {
			label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED
		}
		msg.Field = append(msg.Field, &descriptorpb.FieldDescriptorProto{
			Name:    proto.String(f.name),
			Number:  proto.Int32(int32(i + 1)),
			Type:    f.typ.Enum(),
			Label:   label.Enum(),
			Options: f.opts,
		})
	}
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String(pkg + ".proto"),
		Package:     proto.String(pkg),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{msg},
	}, nil)
	if err != nil {
		t.Fatalf("构造探针描述符: %v", err)
	}
	return fd.Messages().Get(0)
}

// keyTableProbe 第三方登录表的典型形状：sub/provider 是 string、token 是 bytes，
// id/note/tags 是普通列（tags 是 repeated string）。
func keyTableProbe(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	return keyProbeDescriptor(t, "key_table_probe",
		keyProbeField{name: "sub", typ: probeString},
		keyProbeField{name: "provider", typ: probeString},
		keyProbeField{name: "token", typ: probeBytes},
		keyProbeField{name: "id", typ: probeUint64},
		keyProbeField{name: "note", typ: probeString},
		keyProbeField{name: "tags", typ: probeString, repeated: true},
	)
}

func keyTableRow(md protoreflect.MessageDescriptor, sub string, token []byte) *dynamicpb.Message {
	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("sub"), protoreflect.ValueOfString(sub))
	msg.Set(md.Fields().ByName("token"), protoreflect.ValueOfBytes(token))
	msg.Set(md.Fields().ByName("note"), protoreflect.ValueOfString("n"))
	return msg
}

// parityKeyProbeMessage 对拍语料 ddl/create/bytes_unique 的样本消息。testpb 里没有 bytes 字段；
// Python 侧必须按同一个定义构造：message key_probe { uint64 id = 1; string provider = 2; bytes token = 3; }
func parityKeyProbeMessage(t *testing.T) *dynamicpb.Message {
	t.Helper()
	return dynamicpb.NewMessage(keyProbeDescriptor(t, "parity_key_probe",
		keyProbeField{name: "id", typ: probeUint64},
		keyProbeField{name: "provider", typ: probeString},
		keyProbeField{name: "token", typ: probeBytes},
	))
}

func newKeyProbeDB(t *testing.T, msg proto.Message, opts ...TableOption) (*DB, *fakeConn) {
	t.Helper()
	sqlDB, conn := openFakeDB()
	t.Cleanup(func() { _ = sqlDB.Close() })
	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(msg, opts...)
	return pdb, conn
}

func assertContainsAll(t *testing.T, got string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(got, want) {
			t.Errorf("缺少 %q\n--- got ---\n%s", want, got)
		}
	}
}

// ── 列类型映射 ──────────────────────────────────────────────────────────

func TestKeyColumnDDLForCompositeStringBytesPrimaryKey(t *testing.T) {
	msg := dynamicpb.NewMessage(keyTableProbe(t))
	ddl, err := GenerateCreateTableSQLChecked(msg, WithTableName("key_probe"),
		WithPrimaryKey("sub", "token"), WithIndexes("note", "provider"))
	if err != nil {
		t.Fatalf("string+bytes 联合主键应被接受: %v", err)
	}
	assertContainsAll(t, ddl,
		"`sub` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:1'",
		"`token` VARBINARY(191) NOT NULL DEFAULT '' COMMENT 'pb:3'",
		"PRIMARY KEY (`sub`,`token`)",
		// 不在主键/唯一键里的 string/bytes 保持原映射，普通索引仍走 191 前缀
		"`provider` MEDIUMTEXT COMMENT 'pb:2'",
		"`note` MEDIUMTEXT COMMENT 'pb:5'",
		"`tags` MEDIUMBLOB COMMENT 'pb:6'",
		"INDEX `idx_key_probe_0` (`note`(191))",
		"INDEX `idx_key_probe_1` (`provider`(191))",
	)
}

func TestKeyColumnIsWholeColumnInOrdinaryIndexToo(t *testing.T) {
	msg := dynamicpb.NewMessage(keyTableProbe(t))
	ddl, err := GenerateCreateTableSQLChecked(msg, WithTableName("key_probe"),
		WithPrimaryKey("id"), WithUniqueKey("provider, token"), WithIndexes("token,note", "provider"))
	if err != nil {
		t.Fatalf("GenerateCreateTableSQLChecked: %v", err)
	}
	assertContainsAll(t, ddl,
		// 唯一键按建表处同样的拆分 + TrimSpace 识别键列，"provider, token" 里的空格不影响
		"`provider` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''",
		"`token` VARBINARY(191) NOT NULL DEFAULT ''",
		"UNIQUE KEY `uk_key_probe` (`provider`,`token`)",
		"INDEX `idx_key_probe_0` (`token`,`note`(191))",
		"INDEX `idx_key_probe_1` (`provider`)",
	)
}

// TestKeyColumnTypeIgnoresMutableFieldTypeMap MySQLFieldTypes 是可被改写的全局表（TestUpdateFieldType
// 就会改它），键列类型不能跟着漂移；非键列照旧查表。
func TestKeyColumnTypeIgnoresMutableFieldTypeMap(t *testing.T) {
	md := keyTableProbe(t)
	old := MySQLFieldTypes[protoreflect.StringKind]
	MySQLFieldTypes[protoreflect.StringKind] = "LONGTEXT"
	defer func() { MySQLFieldTypes[protoreflect.StringKind] = old }()

	table := newMessageTable(dynamicpb.NewMessage(md), WithTableName("key_probe"), WithPrimaryKey("sub"))
	if got := table.getMySQLFieldType(md.Fields().ByName("sub")); !strings.HasPrefix(got, "VARCHAR(191) ") {
		t.Errorf("键列类型不应受 MySQLFieldTypes 影响，实际 %q", got)
	}
	if got := table.getMySQLFieldType(md.Fields().ByName("note")); got != "LONGTEXT" {
		t.Errorf("非键列仍按 MySQLFieldTypes 映射，实际 %q", got)
	}
}

// ── max_length 选项 ─────────────────────────────────────────────────────

func TestMaxLengthFromProtoOptionsBothReadPaths(t *testing.T) {
	knownOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(knownOpts, pbopt.E_MaxLength, uint32(255))

	var raw []byte
	raw = protowire.AppendTag(raw, optNumFieldMaxLength, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 300)
	unknownOpts := &descriptorpb.FieldOptions{}
	unknownOpts.ProtoReflect().SetUnknown(protoreflect.RawFields(raw))

	md := keyProbeDescriptor(t, "max_length_paths_probe",
		keyProbeField{name: "sub", typ: probeString, opts: knownOpts},
		keyProbeField{name: "token", typ: probeBytes, opts: unknownOpts},
	)

	// 先确认两条读取路径确实各自覆盖到了：已知扩展 vs 生成代码滞后时落进 unknown fields
	subOpts := md.Fields().ByName("sub").Options()
	if !proto.HasExtension(subOpts, pbopt.E_MaxLength) || len(subOpts.ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("sub 的 max_length 应以已知扩展的形式出现")
	}
	tokenOpts := md.Fields().ByName("token").Options()
	if proto.HasExtension(tokenOpts, pbopt.E_MaxLength) || len(tokenOpts.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("token 的 max_length 应以 unknown fields 的形式出现")
	}

	ddl, err := GenerateCreateTableSQLChecked(dynamicpb.NewMessage(md), WithTableName("max_length_probe"),
		WithPrimaryKey("sub"), WithUniqueKey("token"))
	if err != nil {
		t.Fatalf("GenerateCreateTableSQLChecked: %v", err)
	}
	assertContainsAll(t, ddl,
		"`sub` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''",
		"`token` VARBINARY(300) NOT NULL DEFAULT ''",
	)
}

func TestMaxLengthExplicitZeroInProtoIsRejected(t *testing.T) {
	opts := &descriptorpb.FieldOptions{}
	proto.SetExtension(opts, pbopt.E_MaxLength, uint32(0))
	md := keyProbeDescriptor(t, "zero_max_length_probe", keyProbeField{name: "sub", typ: probeString, opts: opts})

	err := ValidateTableMessage(dynamicpb.NewMessage(md), WithTableName("zero_probe"), WithPrimaryKey("sub"))
	if !errors.Is(err, ErrInvalidTableOption) || !strings.Contains(err.Error(), "max_length=0") {
		t.Fatalf("显式写 0 不能当成未声明而退回默认长度，实际: %v", err)
	}
}

// TestForeignExtensionOnMaxLengthNumberDoesNotPanic 别的库在同一字段号上注册了非整数扩展时，
// 读取选项必须忽略它，不能在 v.Uint() 上 panic。
func TestForeignExtensionOnMaxLengthNumberDoesNotPanic(t *testing.T) {
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:       proto.String("foreign_max_length_ext.proto"),
		Package:    proto.String("foreignext"),
		Syntax:     proto.String("proto2"),
		Dependency: []string{"google/protobuf/descriptor.proto"},
		Extension: []*descriptorpb.FieldDescriptorProto{{
			Name:     proto.String("foreign_max_length"),
			Number:   proto.Int32(optNumFieldMaxLength),
			Type:     probeString.Enum(),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Extendee: proto.String(".google.protobuf.FieldOptions"),
		}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("构造外部扩展: %v", err)
	}
	xt := dynamicpb.NewExtensionType(file.Extensions().Get(0))
	opts := &descriptorpb.FieldOptions{}
	opts.ProtoReflect().Set(xt.TypeDescriptor(), protoreflect.ValueOfString("not a number"))
	md := keyProbeDescriptor(t, "foreign_ext_probe", keyProbeField{name: "sub", typ: probeString, opts: opts})

	table := newMessageTable(dynamicpb.NewMessage(md), WithTableName("foreign_ext_probe"), WithPrimaryKey("sub"))
	if got := table.getMySQLFieldType(md.Fields().ByName("sub")); !strings.HasPrefix(got, "VARCHAR(191) ") {
		t.Fatalf("非 uint32 的同号扩展应被忽略、退回默认长度，实际 %q", got)
	}
}

func TestWithMaxLengthMergesPerFieldAndOverridesProto(t *testing.T) {
	subOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(subOpts, pbopt.E_MaxLength, uint32(100))
	providerOpts := &descriptorpb.FieldOptions{}
	proto.SetExtension(providerOpts, pbopt.E_MaxLength, uint32(50))
	md := keyProbeDescriptor(t, "merge_max_length_probe",
		keyProbeField{name: "sub", typ: probeString, opts: subOpts},
		keyProbeField{name: "provider", typ: probeString, opts: providerOpts},
		keyProbeField{name: "token", typ: probeBytes},
	)

	table := newMessageTable(dynamicpb.NewMessage(md), WithTableName("merge_probe"),
		WithPrimaryKey("sub"), WithUniqueKey("provider,token"),
		WithMaxLength("sub", 200), WithMaxLength("token", 64), WithMaxLength("sub", 255))
	if err := table.validateSchemaDefinition(); err != nil {
		t.Fatalf("validateSchemaDefinition: %v", err)
	}
	for field, want := range map[string]string{
		"sub":      "VARCHAR(255) ", // 代码覆盖 proto 的 100，且后应用的 255 覆盖先应用的 200
		"provider": "VARCHAR(50) ",  // 代码没提到的字段保留 proto 声明
		"token":    "VARBINARY(64) ",
	} {
		if got := table.getMySQLFieldType(md.Fields().ByName(protoreflect.Name(field))); !strings.HasPrefix(got, want) {
			t.Errorf("%s 列类型 = %q, want prefix %q", field, got, want)
		}
	}
}

// ── 表选项校验 ──────────────────────────────────────────────────────────

func TestMaxLengthValidation(t *testing.T) {
	msg := dynamicpb.NewMessage(keyTableProbe(t))
	cases := []struct {
		name    string
		opts    []TableOption
		wantErr []string // 为空表示必须通过
	}{
		{"string lower bound", []TableOption{WithPrimaryKey("sub"), WithMaxLength("sub", 1)}, nil},
		{"string upper bound fills the index", []TableOption{WithPrimaryKey("sub"), WithMaxLength("sub", MaxKeyStringLength)}, nil},
		{"bytes lower bound", []TableOption{WithPrimaryKey("token"), WithMaxLength("token", 1)}, nil},
		{"bytes upper bound fills the index", []TableOption{WithPrimaryKey("token"), WithMaxLength("token", MaxKeyBytesLength)}, nil},
		{"string above upper bound", []TableOption{WithPrimaryKey("sub"), WithMaxLength("sub", MaxKeyStringLength+1)},
			[]string{"sub", "max_length=769", "1..768"}},
		{"bytes above upper bound", []TableOption{WithPrimaryKey("token"), WithMaxLength("token", MaxKeyBytesLength+1)},
			[]string{"token", "max_length=3073", "1..3072"}},
		{"explicit zero", []TableOption{WithPrimaryKey("sub"), WithMaxLength("sub", 0)},
			[]string{"sub", "max_length=0"}},
		{"missing field", []TableOption{WithPrimaryKey("sub"), WithMaxLength("missing", 10)},
			[]string{"max_length", "missing"}},
		{"string outside keys", []TableOption{WithPrimaryKey("sub"), WithMaxLength("note", 10)},
			[]string{"note", "目前仅用于主键/唯一键的 string/bytes 字段"}},
		{"integer key field", []TableOption{WithPrimaryKey("id"), WithMaxLength("id", 10)},
			[]string{"id", "目前仅用于主键/唯一键的 string/bytes 字段"}},
		{"repeated string in unique key", []TableOption{WithPrimaryKey("id"), WithUniqueKey("tags"), WithMaxLength("tags", 10)},
			[]string{"tags", "目前仅用于主键/唯一键的 string/bytes 字段"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]TableOption{WithTableName("max_length_probe")}, tc.opts...)
			ddl, err := GenerateCreateTableSQLChecked(msg, opts...)
			if len(tc.wantErr) == 0 {
				if err != nil || ddl == "" {
					t.Fatalf("合法的 max_length 应能建表，err=%v", err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidTableOption) {
				t.Fatalf("应返回 ErrInvalidTableOption，实际: %v", err)
			}
			assertContainsAll(t, err.Error(), tc.wantErr...)
		})
	}

	// 多个字段同时出错时按字段名排序，每次报同一条，不随 map 迭代漂移
	var first string
	for i := 0; i < 30; i++ {
		err := ValidateTableMessage(msg, WithTableName("max_length_probe"), WithPrimaryKey("sub"),
			WithMaxLength("note", 1), WithMaxLength("id", 1), WithMaxLength("tags", 1))
		if err == nil {
			t.Fatal("多个非法 max_length 必须报错")
		}
		if i == 0 {
			first = err.Error()
			if !strings.Contains(first, `"id"`) {
				t.Fatalf("按名字排序后应先报 id，实际: %v", err)
			}
		} else if err.Error() != first {
			t.Fatalf("报错不稳定：%q vs %q", first, err.Error())
		}
	}
}

func TestKeyColumnNullableRejected(t *testing.T) {
	msg := dynamicpb.NewMessage(keyTableProbe(t))
	for _, tc := range []struct {
		name string
		opts []TableOption
	}{
		{"string primary key", []TableOption{WithPrimaryKey("sub"), WithNullableFields("sub")}},
		{"bytes unique key", []TableOption{WithPrimaryKey("id"), WithUniqueKey("token"), WithNullableFields("token")}},
		{"string in composite unique key", []TableOption{WithPrimaryKey("id"), WithUniqueKey("provider,sub"), WithNullableFields("note", "sub")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]TableOption{WithTableName("nullable_key_probe")}, tc.opts...)
			err := ValidateTableMessage(msg, opts...)
			if !errors.Is(err, ErrInvalidTableOption) {
				t.Fatalf("键列声明 nullable 必须拒绝，实际: %v", err)
			}
			assertContainsAll(t, err.Error(), "nullable", "从不为 string/bytes 写 NULL")
		})
	}

	if err := ValidateTableMessage(msg, WithTableName("nullable_key_probe"),
		WithPrimaryKey("id"), WithNullableFields("note")); err != nil {
		t.Fatalf("不在键里的 string 仍可以声明 nullable: %v", err)
	}
}

func TestIndexKeyByteBudget(t *testing.T) {
	msg := dynamicpb.NewMessage(keyTableProbe(t))
	cases := []struct {
		name    string
		opts    []TableOption
		wantErr []string
	}{
		{"composite string primary key over budget",
			[]TableOption{WithPrimaryKey("sub", "provider"), WithMaxLength("sub", 500), WithMaxLength("provider", 500)},
			[]string{"primary_key 键长 4000 字节", "sub VARCHAR(500)×4=2000", "provider VARCHAR(500)×4=2000"}},
		{"bytes unique key plus integer over budget",
			[]TableOption{WithPrimaryKey("id"), WithUniqueKey("token,id"), WithMaxLength("token", MaxKeyBytesLength)},
			[]string{"unique_key 键长 3080 字节", "token VARBINARY(3072)=3072", "id uint64=8"}},
		{"key column counts as whole column in an ordinary index",
			[]TableOption{WithPrimaryKey("sub"), WithMaxLength("sub", 700), WithIndexes("sub,note")},
			[]string{"index[0] 键长 3564 字节", "sub VARCHAR(700)×4=2800", "note MEDIUMTEXT 前缀(191)×4=764"}},
		{"exactly at the budget",
			[]TableOption{WithPrimaryKey("sub", "token"), WithMaxLength("sub", 700), WithMaxLength("token", 272)},
			nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]TableOption{WithTableName("budget_probe")}, tc.opts...)
			err := ValidateTableMessage(msg, opts...)
			if len(tc.wantErr) == 0 {
				if err != nil {
					t.Fatalf("恰好 %d 字节的索引应被接受: %v", MaxIndexKeyBytes, err)
				}
				return
			}
			if !errors.Is(err, ErrInvalidTableOption) {
				t.Fatalf("超出索引字节预算必须返回 ErrInvalidTableOption，实际: %v", err)
			}
			assertContainsAll(t, err.Error(), tc.wantErr...)
		})
	}
}

// ── 写入前的键值校验 ────────────────────────────────────────────────────

type keyValueEntry struct {
	name   string
	pkOnly bool // 只序列化主键的入口：唯一键列里的坏值不应拦住它
	run    func(pdb *DB, msg proto.Message) error
}

func keyValueEntries(md protoreflect.MessageDescriptor) []keyValueEntry {
	valid := keyTableRow(md, "ok", []byte("ok"))
	builder := func(pdb *DB, msg proto.Message) *SQLBuilder {
		b, err := pdb.SQLBuilder(msg)
		if err != nil {
			panic(err)
		}
		return b
	}
	return []keyValueEntry{
		{"Insert", false, func(p *DB, m proto.Message) error { return p.Insert(m) }},
		{"InsertIgnore", false, func(p *DB, m proto.Message) error { _, err := p.InsertIgnore(m); return err }},
		{"Save", false, func(p *DB, m proto.Message) error { return p.Save(m) }},
		{"Update", false, func(p *DB, m proto.Message) error { return p.Update(m) }},
		{"BatchInsert", false, func(p *DB, m proto.Message) error { return p.BatchInsert([]proto.Message{valid, m}) }},
		{"BatchSave", false, func(p *DB, m proto.Message) error { return p.BatchSave([]proto.Message{valid, m}) }},
		{"UpdateFieldsByPK", true, func(p *DB, m proto.Message) error { return p.UpdateFieldsByPK(m, "note") }},
		{"Delete", true, func(p *DB, m proto.Message) error { return p.Delete(m) }},
		{"BatchDelete", true, func(p *DB, m proto.Message) error { return p.BatchDelete([]proto.Message{valid, m}) }},
		{"FindOneByPK", true, func(p *DB, m proto.Message) error { return p.FindOneByPK(m) }},
		{"ExistsByPK", true, func(p *DB, m proto.Message) error { _, err := p.ExistsByPK(m); return err }},
		{"SQLBuilder.Insert", false, func(p *DB, m proto.Message) error { _, err := builder(p, m).Insert(m); return err }},
		{"SQLBuilder.InsertSetFields", false, func(p *DB, m proto.Message) error { _, err := builder(p, m).InsertSetFields(m); return err }},
		{"SQLBuilder.UpdateByPK", false, func(p *DB, m proto.Message) error { _, err := builder(p, m).UpdateByPK(m); return err }},
		{"SQLBuilder.UpdateFieldsByPK", false, func(p *DB, m proto.Message) error {
			_, err := builder(p, m).UpdateFieldsByPK(m, "token")
			return err
		}},
		{"SQLBuilder.SelectByPK", true, func(p *DB, m proto.Message) error { _, err := builder(p, m).SelectByPK(m); return err }},
		{"SQLBuilder.DeleteByPK", true, func(p *DB, m proto.Message) error { _, err := builder(p, m).DeleteByPK(m); return err }},
	}
}

func TestInvalidKeyValueRejectedBeforeSQL(t *testing.T) {
	md := keyTableProbe(t)
	opts := []TableOption{WithTableName("key_value_probe"), WithPrimaryKey("sub"), WithUniqueKey("token"),
		WithMaxLength("sub", 8), WithMaxLength("token", 4)}

	invalid := []struct {
		name   string
		msg    *dynamicpb.Message
		inPK   bool
		want   []string
		secret string // 错误信息不得回显的值
	}{
		{"string longer than max_length", keyTableRow(md, "123456789", nil), true,
			[]string{"key_value_probe", "sub", "9 个字符", "VARCHAR(8)"}, "123456789"},
		{"four-byte emoji counted per character", keyTableRow(md, strings.Repeat("😀", 9), nil), true,
			[]string{"sub", "9 个字符", "VARCHAR(8)"}, "😀"},
		{"invalid utf-8", keyTableRow(md, "ab\xffcd", nil), true,
			[]string{"sub", "不是合法 UTF-8", "5 字节"}, ""},
		{"bytes longer than max_length", keyTableRow(md, "ok", []byte("12345")), false,
			[]string{"key_value_probe", "token", "5 字节", "VARBINARY(4)"}, "12345"},
	}

	for _, bad := range invalid {
		for _, entry := range keyValueEntries(md) {
			t.Run(bad.name+"/"+entry.name, func(t *testing.T) {
				pdb, conn := newKeyProbeDB(t, bad.msg, opts...)
				err := entry.run(pdb, bad.msg)
				if !bad.inPK && entry.pkOnly {
					if errors.Is(err, ErrInvalidKeyValue) {
						t.Fatalf("只用主键的入口不应被唯一键列的值拦住: %v", err)
					}
					return
				}
				if !errors.Is(err, ErrInvalidKeyValue) {
					t.Fatalf("应返回 ErrInvalidKeyValue，实际: %v", err)
				}
				if got := conn.sqls(); len(got) != 0 {
					t.Fatalf("键值校验必须发生在任何 SQL 之前，实际发出: %v", got)
				}
				assertContainsAll(t, err.Error(), bad.want...)
				if bad.secret != "" && strings.Contains(err.Error(), bad.secret) {
					t.Fatalf("错误信息不得回显键值: %v", err)
				}
			})
		}
	}
}

func TestKeyValueBoundariesAreAccepted(t *testing.T) {
	md := keyTableProbe(t)
	opts := []TableOption{WithTableName("key_value_probe"), WithPrimaryKey("sub"), WithUniqueKey("token"),
		WithMaxLength("sub", 8), WithMaxLength("token", 4)}
	for _, tc := range []struct {
		name string
		msg  *dynamicpb.Message
	}{
		{"exactly max_length characters", keyTableRow(md, "12345678", []byte("1234"))},
		{"eight four-byte characters", keyTableRow(md, strings.Repeat("😀", 8), nil)},
		{"empty key values", keyTableRow(md, "", nil)},
		{"bytes that are not utf-8", keyTableRow(md, "ok", []byte{0xff, 0x00, 0x20, 0xfe})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pdb, conn := newKeyProbeDB(t, tc.msg, opts...)
			if err := pdb.Insert(tc.msg); err != nil {
				t.Fatalf("边界内的键值应能写入: %v", err)
			}
			if n := conn.countSQL("INSERT INTO"); n != 1 {
				t.Fatalf("应恰好发出一条 INSERT，实际 %d: %v", n, conn.sqls())
			}
		})
	}
}

// TestBatchKeyValidationCoversLaterChunks 分批入口不是原子的：第二批里的坏值必须在第一批
// 发出之前就被拦下，否则调用方拿到错误时库里已经落了一半。
func TestBatchKeyValidationCoversLaterChunks(t *testing.T) {
	md := keyTableProbe(t)
	opts := []TableOption{WithTableName("key_batch_probe"), WithPrimaryKey("sub"), WithMaxLength("sub", 8)}
	msgs := make([]proto.Message, BatchInsertMaxSize+1)
	for i := range msgs {
		msgs[i] = keyTableRow(md, fmt.Sprintf("k%d", i), nil)
	}
	msgs[len(msgs)-1] = keyTableRow(md, "123456789", nil)

	for _, tc := range []struct {
		name string
		run  func(*DB) error
	}{
		{"BatchInsert", func(p *DB) error { return p.BatchInsert(msgs) }},
		{"BatchDelete", func(p *DB) error { return p.BatchDelete(msgs) }},
		{"BatchSave", func(p *DB) error { return p.BatchSave(msgs) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pdb, conn := newKeyProbeDB(t, msgs[0], opts...)
			if err := tc.run(pdb); !errors.Is(err, ErrInvalidKeyValue) {
				t.Fatalf("应返回 ErrInvalidKeyValue，实际: %v", err)
			}
			if got := conn.sqls(); len(got) != 0 {
				t.Fatalf("后面批次的坏值必须在第一条语句前拦下，实际发出 %d 条", len(got))
			}
		})
	}
}

func TestGormKeyValueRejectedBeforeSQL(t *testing.T) {
	md := keyTableProbe(t)
	bad := keyTableRow(md, "123456789", nil)
	valid := keyTableRow(md, "ok", nil)
	for _, tc := range []struct {
		name string
		run  func(*GormDB) error
	}{
		{"Insert", func(g *GormDB) error { return g.Insert(bad) }},
		{"Save", func(g *GormDB) error { return g.Save(bad) }},
		{"Update", func(g *GormDB) error { return g.Update(bad) }},
		{"Delete", func(g *GormDB) error { return g.Delete(bad) }},
		{"BatchInsert", func(g *GormDB) error { return g.BatchInsert([]proto.Message{valid, bad}) }},
		{"BatchSave", func(g *GormDB) error { return g.BatchSave([]proto.Message{valid, bad}) }},
		{"BatchDelete", func(g *GormDB) error { return g.BatchDelete([]proto.Message{valid, bad}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gdb, conn := newGormAuditDB(t)
			gdb.RegisterTable(bad, WithTableName("gorm_key_probe"), WithPrimaryKey("sub"), WithMaxLength("sub", 8))
			before := len(conn.sqls())
			if err := tc.run(gdb); !errors.Is(err, ErrInvalidKeyValue) {
				t.Fatalf("GORM 路径同样必须返回 ErrInvalidKeyValue，实际: %v", err)
			}
			if got := conn.sqls()[before:]; len(got) != 0 {
				t.Fatalf("GORM 路径的键值校验必须发生在 SQL 之前，实际发出: %v", got)
			}
		})
	}
}

func TestCacheKeyRejectsInvalidPrimaryKeyValue(t *testing.T) {
	md := keyTableProbe(t)
	msg := keyTableRow(md, "123456789", nil)
	table := newMessageTable(msg, WithTableName("cache_key_probe"), WithPrimaryKey("sub"), WithMaxLength("sub", 8))
	if _, err := cacheKeyFor(table, msg); !errors.Is(err, ErrInvalidKeyValue) {
		t.Fatalf("缓存 key 与 WHERE 条件共用同一份主键序列化，超长值必须同样被拒，实际: %v", err)
	}
}

// ── 结构同步 ────────────────────────────────────────────────────────────

func keyTableAlignedSnapshot() (cols, indexes, primary [][]driver.Value) {
	cols = rows(
		colRow("sub", "varchar(64)", 1),
		colRow("provider", "varchar(191)", 2),
		colRow("token", "varbinary(191)", 3),
		colRow("id", "bigint unsigned", 4),
		colRow("note", "mediumtext", 5),
		colRow("tags", "mediumblob", 6),
	)
	indexes = rows(
		indexRow("idx_key_sync_probe_0", false, 1, "note", int64(TextIndexPrefixLength)),
		indexRow("idx_key_sync_probe_1", false, 1, "sub", nil),
		indexRow("idx_key_sync_probe_1", false, 2, "id", nil),
		indexRow("uk_key_sync_probe", true, 1, "provider", nil),
		indexRow("uk_key_sync_probe", true, 2, "token", nil),
	)
	primary = rows(indexRow("PRIMARY", true, 1, "sub", nil))
	return cols, indexes, primary
}

// TestNewKeyTableSyncHasNoDrift 建表后回读的结构（VARCHAR NOT NULL、默认值空串、KeyStringCollation、
// 整列索引）必须被判为已对齐：首次同步与再次同步都零漂移、零 ALTER。
func TestNewKeyTableSyncHasNoDrift(t *testing.T) {
	md := keyTableProbe(t)
	msg := dynamicpb.NewMessage(md)
	pdb, conn := newKeyProbeDB(t, msg, WithTableName("key_sync_probe"), WithPrimaryKey("sub"),
		WithUniqueKey("provider,token"), WithIndexes("note", "sub,id"), WithMaxLength("sub", 64))
	cols, indexes, primary := keyTableAlignedSnapshot()

	queueLockedSchemaSync(conn, rows(row(int64(0))), nil, cols, indexes, primary)
	if err := pdb.CreateOrUpdateTable(msg); err != nil {
		t.Fatalf("首次同步: %v", err)
	}
	create := conn.findSQL("CREATE TABLE IF NOT EXISTS `key_sync_probe`")
	assertContainsAll(t, create,
		"`sub` VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''",
		"`token` VARBINARY(191) NOT NULL DEFAULT ''",
		"PRIMARY KEY (`sub`)",
		"UNIQUE KEY `uk_key_sync_probe` (`provider`,`token`)",
		"INDEX `idx_key_sync_probe_1` (`sub`,`id`)",
	)

	queueLockedSchemaSync(conn, rows(row(int64(1))), cols, indexes, primary)
	if err := pdb.CreateOrUpdateTable(msg); err != nil {
		t.Fatalf("再次同步: %v", err)
	}
	if n := conn.countSQL("ALTER TABLE"); n != 0 {
		t.Fatalf("建表后回读的结构必须零漂移、零 ALTER，实际: %v", conn.sqls())
	}
}

// legacyStatements 从 ErrLegacyKeyColumn 的错误信息里取出迁移 SQL。
func legacyStatements(t *testing.T, err error) []string {
	t.Helper()
	text := err.Error()
	idx := strings.Index(text, legacyKeySQLHeader)
	if idx < 0 {
		t.Fatalf("错误信息里没有迁移 SQL: %v", err)
	}
	var statements []string
	for _, line := range strings.Split(text[idx+len(legacyKeySQLHeader):], "\n") {
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "    ") {
			break
		}
		statements = append(statements, strings.TrimSuffix(strings.TrimSpace(line), ";"))
	}
	if len(statements) == 0 {
		t.Fatalf("迁移 SQL 为空: %v", err)
	}
	return statements
}

func TestLegacyKeyColumnsFailClosedWithoutDDL(t *testing.T) {
	golangCols := func(ipRow []driver.Value, idAutoIncrement bool) [][]driver.Value {
		cols := golangTestAlignedCols()
		if !idAutoIncrement {
			cols[0] = colRowAttrs("id", "int unsigned", 1, false, "")
		}
		cols[1] = ipRow
		return cols
	}
	md := keyTableProbe(t)

	cases := []struct {
		name       string
		msg        proto.Message
		opts       []TableOption
		queue      [][][]driver.Value
		wantReason string
		wantSQL    []string
	}{
		{
			name: "mediumtext unique key with 191 prefix",
			msg:  &testpb.GolangTest{},
			opts: []TableOption{WithPrimaryKey("id"), WithUniqueKey("ip")},
			queue: [][][]driver.Value{
				rows(row(int64(1))),
				golangTestAlignedCols(), // ip: mediumtext NULL, utf8mb4_unicode_ci
				rows(indexRow("uk_golang_test", true, 1, "ip", int64(TextIndexPrefixLength))),
				rows(indexRow("PRIMARY", true, 1, "id", nil)),
			},
			wantReason: "TEXT 列只能建前缀索引",
			wantSQL: []string{
				"ALTER TABLE `golang_test` DROP INDEX `uk_golang_test`",
				"UPDATE `golang_test` SET `ip` = '' WHERE `ip` IS NULL",
				"ALTER TABLE `golang_test` MODIFY COLUMN `ip` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:2'",
				"ALTER TABLE `golang_test` ADD UNIQUE KEY `uk_golang_test` (`ip`)",
			},
		},
		{
			name: "varchar utf8mb4_unicode_ci primary key",
			msg:  &testpb.GolangTest{},
			opts: []TableOption{WithPrimaryKey("ip"), WithAutoIncrementKey("")},
			queue: [][][]driver.Value{
				rows(row(int64(1))),
				golangCols(colRowFull("ip", "varchar(191)", 2, false, "", nil, "utf8mb4_unicode_ci"), false),
				rows(indexRow("PRIMARY", true, 1, "ip", nil)),
			},
			wantReason: "不区分大小写",
			wantSQL: []string{
				"CREATE TABLE `golang_test__p2m_new` (`id` int unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1', " +
					"`ip` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:2', ",
				"PRIMARY KEY (`ip`)) ENGINE=InnoDB",
				"INSERT INTO `golang_test__p2m_new` (`id`, `ip`, `port`, `group_id`, `player`, `player_id`) " +
					"SELECT `id`, `ip`, `port`, `group_id`, `player`, `player_id` FROM `golang_test`",
				"RENAME TABLE `golang_test` TO `golang_test__p2m_old`, `golang_test__p2m_new` TO `golang_test`",
			},
		},
		{
			name: "varchar utf8mb4_bin primary key",
			msg:  &testpb.GolangTest{},
			opts: []TableOption{WithPrimaryKey("ip"), WithAutoIncrementKey("")},
			queue: [][][]driver.Value{
				rows(row(int64(1))),
				golangCols(colRowFull("ip", "varchar(191)", 2, false, "", "", "utf8mb4_bin"), false),
				rows(indexRow("PRIMARY", true, 1, "ip", nil)),
			},
			wantReason: "PAD SPACE",
			wantSQL:    []string{"RENAME TABLE `golang_test` TO `golang_test__p2m_old`"},
		},
		{
			name: "mediumblob bytes unique key",
			msg:  dynamicpb.NewMessage(md),
			opts: []TableOption{WithTableName("legacy_bytes_probe"), WithPrimaryKey("id"), WithUniqueKey("token")},
			queue: [][][]driver.Value{
				rows(row(int64(1))),
				rows(
					colRow("sub", "mediumtext", 1),
					colRow("provider", "mediumtext", 2),
					colRow("token", "mediumblob", 3),
					colRow("id", "bigint unsigned", 4),
					colRow("note", "mediumtext", 5),
					colRow("tags", "mediumblob", 6),
				),
				rows(indexRow("uk_legacy_bytes_probe", true, 1, "token", int64(TextIndexPrefixLength))),
				rows(indexRow("PRIMARY", true, 1, "id", nil)),
			},
			wantReason: "BLOB 列只能建前缀索引",
			wantSQL: []string{
				"ALTER TABLE `legacy_bytes_probe` DROP INDEX `uk_legacy_bytes_probe`",
				"UPDATE `legacy_bytes_probe` SET `token` = '' WHERE `token` IS NULL",
				"ALTER TABLE `legacy_bytes_probe` MODIFY COLUMN `token` VARBINARY(191) NOT NULL DEFAULT '' COMMENT 'pb:3'",
				"ALTER TABLE `legacy_bytes_probe` ADD UNIQUE KEY `uk_legacy_bytes_probe` (`token`)",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pdb, conn := newKeyProbeDB(t, tc.msg, tc.opts...)
			queueLockedSchemaSync(conn, tc.queue...)

			err := pdb.CreateOrUpdateTable(tc.msg)
			if !errors.Is(err, ErrSchemaDrift) || !errors.Is(err, ErrLegacyKeyColumn) {
				t.Fatalf("旧形态键列必须同时满足 ErrSchemaDrift 与 ErrLegacyKeyColumn，实际: %v", err)
			}
			for _, prefix := range []string{"ALTER", "CREATE", "UPDATE", "INSERT", "RENAME", "DROP"} {
				for _, stmt := range conn.sqls() {
					if strings.HasPrefix(strings.TrimSpace(stmt), prefix) {
						t.Fatalf("识别到旧形态后不得执行任何 DDL/DML，实际发出: %s", stmt)
					}
				}
			}
			assertContainsAll(t, err.Error(), tc.wantReason, "迁移步骤")
			for _, generic := range []string{"nullable mismatch", "default mismatch", "definition mismatch"} {
				if strings.Contains(err.Error(), generic) {
					t.Errorf("旧形态列不应再报通用漂移 %q: %v", generic, err)
				}
			}
			assertContainsAll(t, strings.Join(legacyStatements(t, err), "\n"), tc.wantSQL...)
		})
	}
}

// TestLegacyKeyColumnDetectionIgnoresIndexMetadata 旧形态识别只看列元数据：没有读取索引元数据
// （indexesKnown=false）时也必须拦下，并把与键列无关的漂移附在后面。
func TestLegacyKeyColumnDetectionIgnoresIndexMetadata(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithUniqueKey("ip"))
	current := map[string]columnMeta{
		"id":        {colType: "int unsigned", fieldNum: 1, extra: "auto_increment", metadataComplete: true},
		"ip":        {colType: "mediumtext", fieldNum: 2, nullable: true, collation: "utf8mb4_unicode_ci", metadataComplete: true},
		"port":      {colType: "int unsigned", fieldNum: 3, nullable: true, defaultValue: sql.NullString{String: "0", Valid: true}, metadataComplete: true},
		"group_id":  {colType: "int unsigned", fieldNum: 4, defaultValue: sql.NullString{String: "0", Valid: true}, metadataComplete: true},
		"player":    {colType: "mediumblob", fieldNum: 5, nullable: true, metadataComplete: true},
		"player_id": {colType: "bigint unsigned", fieldNum: 6, defaultValue: sql.NullString{String: "0", Valid: true}, metadataComplete: true},
	}

	err := table.validateSchemaDrift(current, nil, false, nil)
	if !errors.Is(err, ErrLegacyKeyColumn) || !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("不读索引元数据也必须识别旧形态键列，实际: %v", err)
	}
	assertContainsAll(t, err.Error(), "列 ip", "mediumtext COLLATE utf8mb4_unicode_ci NULL",
		"另有与这些键列无关的漂移", "column port nullable mismatch")
}

func TestKeyColumnWideningGeneratesModifyWithCollation(t *testing.T) {
	md := keyTableProbe(t)
	msg := dynamicpb.NewMessage(md)

	wide := newMessageTable(msg, WithTableName("widen_probe"), WithPrimaryKey("sub"), WithUniqueKey("token"),
		WithMaxLength("sub", 255), WithMaxLength("token", 255))
	clauses, err := wide.buildAlterClauses(alignedCols(wide, map[string]columnMeta{
		"sub":   {colType: "varchar(191)", fieldNum: 1},
		"token": {colType: "varbinary(191)", fieldNum: 3},
	}), false)
	if err != nil {
		t.Fatalf("buildAlterClauses: %v", err)
	}
	assertContainsAll(t, strings.Join(clauses, "\n"),
		"MODIFY COLUMN `sub` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:1'",
		"MODIFY COLUMN `token` VARBINARY(255) NOT NULL DEFAULT '' COMMENT 'pb:3'",
	)

	narrow := newMessageTable(msg, WithTableName("widen_probe"), WithPrimaryKey("sub"), WithUniqueKey("token"),
		WithMaxLength("sub", 64), WithMaxLength("token", 64))
	clauses, err = narrow.buildAlterClauses(alignedCols(narrow, map[string]columnMeta{
		"sub":   {colType: "varchar(255)", fieldNum: 1},
		"token": {colType: "varbinary(255)", fieldNum: 3},
	}), false)
	if err != nil {
		t.Fatalf("buildAlterClauses: %v", err)
	}
	if len(clauses) != 0 {
		t.Fatalf("线上键列更宽时不得收窄，实际: %v", clauses)
	}
}

// TestKeyColumnWideningSyncWithRealMetadata 真实元数据（NOT NULL、默认值空串、KeyStringCollation）下
// 调大 max_length 只产生拓宽的 MODIFY，不会被当成漂移拦下。
func TestKeyColumnWideningSyncWithRealMetadata(t *testing.T) {
	md := keyTableProbe(t)
	msg := dynamicpb.NewMessage(md)
	pdb, conn := newKeyProbeDB(t, msg, WithTableName("key_sync_probe"), WithPrimaryKey("sub"),
		WithUniqueKey("provider,token"), WithIndexes("note", "sub,id"),
		WithMaxLength("sub", 255), WithMaxLength("token", 255))
	cols, indexes, primary := keyTableAlignedSnapshot()
	queueLockedSchemaSync(conn, rows(row(int64(1))), cols, indexes, primary, nil)

	if err := pdb.CreateOrUpdateTable(msg); err != nil {
		t.Fatalf("拓宽键列不应被判为漂移: %v", err)
	}
	assertContainsAll(t, conn.findSQL("ALTER TABLE `key_sync_probe`"),
		"MODIFY COLUMN `sub` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:1'",
		"MODIFY COLUMN `token` VARBINARY(255) NOT NULL DEFAULT '' COMMENT 'pb:3'",
	)
}

func TestColumnMetaReadsCollation(t *testing.T) {
	pdb, conn := newFakeDB(t)
	conn.queueRows(rows(
		colRowFull("ip", "varchar(191)", 2, false, "", "", "utf8mb4_unicode_ci"),
		colRow("player", "mediumblob", 5),
	))
	metas, err := pdb.getTableColumnMeta(GetTableName(&testpb.GolangTest{}))
	if err != nil {
		t.Fatalf("getTableColumnMeta: %v", err)
	}
	if got := metas["ip"].collation; got != "utf8mb4_unicode_ci" {
		t.Errorf("COLLATION_NAME 未读回: %q", got)
	}
	if got := metas["player"].collation; got != "" {
		t.Errorf("二进制列的 COLLATION_NAME 是 NULL，应读成空串: %q", got)
	}
	if !metas["ip"].defaultValue.Valid || metas["ip"].defaultValue.String != "" {
		t.Errorf("DEFAULT '' 回读是空串而不是 NULL: %+v", metas["ip"].defaultValue)
	}
}

func TestEmptyStringDefaultComparison(t *testing.T) {
	want := expectedColumnDefault("VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''")
	if !want.Valid || want.String != "" {
		t.Fatalf("DEFAULT '' 的期望值应是非 NULL 的空串: %+v", want)
	}
	if got := expectedColumnDefault("VARBINARY(64) NOT NULL DEFAULT ''"); !got.Valid || got.String != "" {
		t.Fatalf("VARBINARY 同样期望空串: %+v", got)
	}
	cases := []struct {
		online sql.NullString
		equal  bool
	}{
		{sql.NullString{String: "", Valid: true}, true},
		{sql.NullString{String: "''", Valid: true}, true}, // 回读成带引号字面量的后端
		{sql.NullString{}, false},
		{sql.NullString{String: "0", Valid: true}, false},
	}
	for _, c := range cases {
		if got := columnDefaultsEqual(c.online, want); got != c.equal {
			t.Errorf("columnDefaultsEqual(%+v, '') = %v, want %v", c.online, got, c.equal)
		}
	}
}

func TestBinaryTypeLengthComparison(t *testing.T) {
	cases := []struct {
		current, target string
		match, narrowed bool
	}{
		{"varbinary(191)", "VARBINARY(255) NOT NULL DEFAULT ''", false, false},
		{"varbinary(255)", "VARBINARY(191) NOT NULL DEFAULT ''", true, true},
		{"varbinary(191)", "VARBINARY(191) NOT NULL DEFAULT ''", true, false},
		{"binary(16)", "binary(32)", false, false},
		{"binary(32)", "binary(16)", true, true},
	}
	for _, c := range cases {
		if got := isTypeMatch(c.current, c.target); got != c.match {
			t.Errorf("isTypeMatch(%q, %q) = %v, want %v", c.current, c.target, got, c.match)
		}
		if got := narrowingSuppressed(c.current, c.target); got != c.narrowed {
			t.Errorf("narrowingSuppressed(%q, %q) = %v, want %v", c.current, c.target, got, c.narrowed)
		}
	}
}

func TestLegacyKeyColumnReasons(t *testing.T) {
	cases := []struct {
		kind      protoreflect.Kind
		colType   string
		collation string
		want      string // 空串表示形态正确
	}{
		{protoreflect.StringKind, "varchar(191)", "utf8mb4_0900_bin", ""},
		{protoreflect.StringKind, "varchar(768)", "UTF8MB4_0900_BIN", ""},
		{protoreflect.StringKind, "varchar(64)", "utf8mb4_unicode_ci", "不区分大小写"},
		{protoreflect.StringKind, "varchar(64)", "utf8mb4_0900_ai_ci", "不区分大小写"},
		{protoreflect.StringKind, "varchar(64)", "utf8mb4_bin", "PAD SPACE"},
		{protoreflect.StringKind, "varchar(64)", "utf8mb4_0900_as_cs", "不是按码点比较"},
		{protoreflect.StringKind, "varchar(64)", "", "读不到排序规则"},
		{protoreflect.StringKind, "mediumtext", "utf8mb4_unicode_ci", "前缀索引"},
		{protoreflect.StringKind, "char(32)", "utf8mb4_0900_bin", "尾部空格"},
		{protoreflect.StringKind, "varbinary(191)", "", "不是 string 键列要求的 VARCHAR"},
		{protoreflect.BytesKind, "varbinary(191)", "", ""},
		{protoreflect.BytesKind, "mediumblob", "", "前缀索引"},
		{protoreflect.BytesKind, "binary(16)", "", "0x00"},
		{protoreflect.BytesKind, "varchar(191)", "utf8mb4_0900_bin", "不是 bytes 键列要求的 VARBINARY"},
	}
	for _, c := range cases {
		got := legacyKeyColumnReason(c.kind, columnMeta{colType: c.colType, collation: c.collation})
		if c.want == "" {
			if got != "" {
				t.Errorf("%s %s %s 形态正确，却报 %q", c.kind, c.colType, c.collation, got)
			}
			continue
		}
		if !strings.Contains(got, c.want) {
			t.Errorf("%s %s %s: reason %q 应包含 %q", c.kind, c.colType, c.collation, got, c.want)
		}
	}
}
