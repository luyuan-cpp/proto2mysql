package proto2mysql

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// string/bytes 主键列映射为 VARCHAR(191)/VARBINARY(191) 的建表语句断言；只比对 DDL 文本，不连接 MySQL。
func localKeyColumnMessage(t *testing.T) *dynamicpb.Message {
	t.Helper()
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("local_key_column.proto"), Package: proto.String("local_key_column"),
		Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Row"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("account"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				{Name: proto.String("binary_key"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
				{Name: proto.String("account_detail"), Number: proto.Int32(3), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				{Name: proto.String("payload"), Number: proto.Int32(4), Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
				{Name: proto.String("number"), Number: proto.Int32(5), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()},
			},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("descriptor: %v", err)
	}
	return dynamicpb.NewMessage(fd.Messages().Get(0))
}

func TestLocalKeyColumnCompositePrimaryKeysUseBoundedFullColumns(t *testing.T) {
	msg := localKeyColumnMessage(t)
	model := NewDB()
	model.RegisterTable(msg, WithPrimaryKey("account", "binary_key"))
	ddl := model.GetCreateTableSQL(msg)
	for _, want := range []string{"`account` VARCHAR(191)", "`binary_key` VARBINARY(191)", "PRIMARY KEY (`account`,`binary_key`)"} {
		if !strings.Contains(ddl, want) {
			t.Errorf("missing %q in DDL: %s", want, ddl)
		}
	}
	if strings.Contains(ddl, "PRIMARY KEY (`account`(191)") {
		t.Fatalf("主键必须比较完整列，不能变为前缀唯一性: %s", ddl)
	}
}

func TestLocalKeyColumnNonKeysAndNumericKeyKeepOriginalStorage(t *testing.T) {
	msg := localKeyColumnMessage(t)
	model := NewDB()
	model.RegisterTable(msg, WithPrimaryKey("account", "number"))
	ddl := model.GetCreateTableSQL(msg)
	for _, want := range []string{
		"`account_detail` MEDIUMTEXT", "`payload` MEDIUMBLOB", "`binary_key` MEDIUMBLOB",
		"`number` bigint unsigned NOT NULL DEFAULT 0", "PRIMARY KEY (`account`,`number`)",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("非主键存储或数值主键改变: missing %q in %s", want, ddl)
		}
	}
}

func TestLocalKeyColumnWithoutPrimaryKeyLeavesTextAndBlobUnchanged(t *testing.T) {
	msg := localKeyColumnMessage(t)
	model := NewDB()
	model.RegisterTable(msg)
	ddl := model.GetCreateTableSQL(msg)
	if strings.Contains(ddl, "VARCHAR(191)") || strings.Contains(ddl, "VARBINARY(191)") {
		t.Fatalf("非主键不应被截为191长度: %s", ddl)
	}
}
