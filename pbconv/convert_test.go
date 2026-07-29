package pbconv

import (
	"bytes"
	"strings"
	"testing"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TestBinaryFieldsAreRawNotBase64 钉住二进制字段的存储编码：
// bytes / 嵌套消息 / map / list 一律是 proto wire 裸字节，不做 Base64。
// 目标列是 MEDIUMBLOB，本身二进制安全，Base64 只会多占 33% 体积；更重要的是这层编码
// 一旦改动就与线上已有数据不兼容（新副本写的行旧副本读不出来），所以用测试锁死。
func TestBinaryFieldsAreRawNotBase64(t *testing.T) {
	sub := &testpb.Player{PlayerId: 1 << 62, Name: "玩家名"}
	src := &testpb.GolangTest{Id: 1, Player: sub}

	desc := src.ProtoReflect().Descriptor()
	got, err := SerializeFieldAsString(src, desc.Fields().ByName("player"))
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	want, err := proto.Marshal(sub)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got != string(want) {
		t.Fatalf("嵌套消息不是裸字节\n实际 %d 字节: %q\n期望 %d 字节", len(got), got, len(want))
	}
	// Base64 字母表不含高位字节；出现 >=0x80 即证明没被编码
	if !strings.ContainsFunc(got, func(r rune) bool { return r >= 0x80 }) {
		t.Error("序列化结果里没有高位字节，可能又被编码成了 Base64")
	}

	// 裸字节必须能直接 Unmarshal，不需要先解码
	var back testpb.Player
	if err := proto.Unmarshal([]byte(got), &back); err != nil {
		t.Fatalf("裸字节应可直接 Unmarshal: %v", err)
	}
	if !proto.Equal(sub, &back) {
		t.Errorf("往返不一致: %s vs %s", sub.String(), back.String())
	}
}

// TestContainerFieldsAreRaw map/list 容器同样是裸字节且可逆
func TestContainerFieldsAreRaw(t *testing.T) {
	src := &testpb.GolangTestList{TestList: []*testpb.GolangTest{
		{Id: 1, Ip: "10.0.0.1", Port: 65535},
		{Id: 2, Player: &testpb.Player{PlayerId: 1 << 62}},
	}}
	fd := src.ProtoReflect().Descriptor().Fields().Get(0)

	encoded, err := SerializeFieldAsString(src, fd)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	holder := src.ProtoReflect().New()
	holder.Set(fd, src.ProtoReflect().Get(fd))
	want, _ := proto.Marshal(holder.Interface())
	if encoded != string(want) {
		t.Errorf("repeated 字段不是裸字节: 实际 %d 字节, 期望 %d 字节", len(encoded), len(want))
	}

	var dst testpb.GolangTestList
	if err := ParseFromString(&dst, []string{encoded}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !proto.Equal(src, &dst) {
		t.Error("往返不一致")
	}
}

// TestBytesFieldPreservesArbitraryBytes bytes 字段承载任意字节（含 0x00 与 0xFF）时逐字节无损。
// testpb 里没有 bytes 字段，用 dynamicpb 现造一个描述符覆盖这条路径。
func TestBytesFieldPreservesArbitraryBytes(t *testing.T) {
	fdp := &descriptorpb.FileDescriptorProto{
		Name:   proto.String("bytesprobe.proto"),
		Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("bytes_probe"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name:   proto.String("blob"),
				Number: proto.Int32(1),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			}},
		}},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatal(err)
	}
	md := fd.Messages().Get(0)
	field := md.Fields().ByName("blob")

	payload := []byte{0x00, 0xFF, 0x00, 0x80, 0x7F, 0x00, 0xC3, 0x28} // 末两字节是非法 UTF-8 序列
	src := dynamicpb.NewMessage(md)
	src.Set(field, protoreflect.ValueOfBytes(payload))

	encoded, err := SerializeFieldAsString(src.Interface(), field)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if encoded != string(payload) {
		t.Fatalf("bytes 字段不是裸字节: %q", encoded)
	}

	dst := dynamicpb.NewMessage(md)
	if err := ParseFromString(dst.Interface(), []string{encoded}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := dst.Get(field).Bytes(); !bytes.Equal(got, payload) {
		t.Errorf("字节不一致\n实际: %x\n期望: %x", got, payload)
	}
}

// TestScalarFieldRoundTrip 验证标量与嵌套消息字段的序列化/反序列化对称性
func TestScalarFieldRoundTrip(t *testing.T) {
	src := &testpb.GolangTest{
		Id:      42,
		GroupId: 7,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &testpb.Player{
			PlayerId: 100,
			Name:     "foo'bar\\baz\n中文😊",
		},
	}

	desc := src.ProtoReflect().Descriptor()
	row := make([]string, desc.Fields().Len())
	for i := 0; i < desc.Fields().Len(); i++ {
		val, err := SerializeFieldAsString(src, desc.Fields().Get(i))
		if err != nil {
			t.Fatalf("serialize field %s: %v", desc.Fields().Get(i).Name(), err)
		}
		row[i] = val
	}

	dst := &testpb.GolangTest{}
	if err := ParseFromString(dst, row); err != nil {
		t.Fatalf("parse row: %v", err)
	}

	if !proto.Equal(src, dst) {
		t.Errorf("round trip mismatch\nwant: %s\ngot:  %s", src.String(), dst.String())
	}
}

// TestRepeatedFieldRoundTrip 验证repeated字段（旧实现会panic）的序列化/反序列化
func TestRepeatedFieldRoundTrip(t *testing.T) {
	src := &testpb.GolangTestList{
		TestList: []*testpb.GolangTest{
			{Id: 1, Ip: "10.0.0.1", Port: 1},
			{Id: 2, Ip: "10.0.0.2", Port: 2, Player: &testpb.Player{PlayerId: 9, Name: "p"}},
		},
	}

	fieldDesc := src.ProtoReflect().Descriptor().Fields().Get(0)
	if !fieldDesc.IsList() {
		t.Fatalf("expected first field of GolangTestList to be repeated")
	}

	encoded, err := SerializeFieldAsString(src, fieldDesc)
	if err != nil {
		t.Fatalf("serialize repeated field: %v", err)
	}
	if encoded == "" {
		t.Fatal("expected non-empty encoded value")
	}

	dst := &testpb.GolangTestList{}
	if err := ParseFromString(dst, []string{encoded}); err != nil {
		t.Fatalf("parse repeated field: %v", err)
	}

	if !proto.Equal(src, dst) {
		t.Errorf("round trip mismatch\nwant: %s\ngot:  %s", src.String(), dst.String())
	}
}

// TestEmptyValuesRoundTrip 验证空值/未设置字段的处理
func TestEmptyValuesRoundTrip(t *testing.T) {
	src := &testpb.GolangTest{}

	desc := src.ProtoReflect().Descriptor()
	row := make([]string, desc.Fields().Len())
	for i := 0; i < desc.Fields().Len(); i++ {
		val, err := SerializeFieldAsString(src, desc.Fields().Get(i))
		if err != nil {
			t.Fatalf("serialize field %s: %v", desc.Fields().Get(i).Name(), err)
		}
		row[i] = val
	}

	dst := &testpb.GolangTest{}
	if err := ParseFromString(dst, row); err != nil {
		t.Fatalf("parse row: %v", err)
	}

	if !proto.Equal(src, dst) {
		t.Errorf("round trip mismatch\nwant: %s\ngot:  %s", src.String(), dst.String())
	}
}

// TestBoolIsNumericLiteral bool 必须序列化成 "1"/"0"：目标列是 tinyint(1)，
// STRICT_TRANS_TABLES 下写 'true' 会被 MySQL 拒（Error 1366）。
// 同时验证读侧对存量的 "true"/"false" 仍然兼容。
func TestBoolIsNumericLiteral(t *testing.T) {
	fdp := &descriptorpb.FileDescriptorProto{
		Name:   proto.String("boolenc.proto"),
		Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("bool_enc"),
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("flag"), Number: proto.Int32(1),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			}},
		}},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatal(err)
	}
	md := fd.Messages().Get(0)
	field := md.Fields().ByName("flag")

	for _, tc := range []struct {
		val  bool
		want string
	}{{true, "1"}, {false, "0"}} {
		m := dynamicpb.NewMessage(md)
		m.Set(field, protoreflect.ValueOfBool(tc.val))
		got, err := SerializeFieldAsString(m.Interface(), field)
		if err != nil {
			t.Fatalf("serialize %v: %v", tc.val, err)
		}
		if got != tc.want {
			t.Errorf("bool %v 应序列化为 %q, 实际 %q", tc.val, tc.want, got)
		}
	}

	// 读侧：新格式与存量旧格式都要能解
	for _, raw := range []string{"1", "0", "true", "false"} {
		dst := dynamicpb.NewMessage(md)
		if err := ParseFromString(dst.Interface(), []string{raw}); err != nil {
			t.Errorf("读回 %q 失败: %v", raw, err)
		}
	}
}
