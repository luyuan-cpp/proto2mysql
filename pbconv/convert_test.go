package pbconv

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// probeMsg 现造一个 message 描述符：id(int32) + 一个被测字段
func probeMsg(t *testing.T, name string, f *descriptorpb.FieldDescriptorProto, deps ...string) protoreflect.MessageDescriptor {
	t.Helper()
	fdp := &descriptorpb.FileDescriptorProto{
		Name:       proto.String(name + ".proto"),
		Package:    proto.String("pbconvprobe"),
		Syntax:     proto.String("proto3"),
		Dependency: deps,
		MessageType: []*descriptorpb.DescriptorProto{{
			Name:  proto.String(name),
			Field: []*descriptorpb.FieldDescriptorProto{f},
		}},
	}
	fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build descriptor %s: %v", name, err)
	}
	return fd.Messages().Get(0)
}

func tsField() *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name: proto.String("ts"), Number: proto.Int32(1),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		TypeName: proto.String(".google.protobuf.Timestamp"),
	}
}

// TestTimestampKeepsSubSecond Timestamp 的亚秒精度不得被静默丢掉。
// 旧实现用 "2006-01-02 15:04:05" 写入，毫秒/纳秒无声消失且不报错，是最难发现的一类数据损坏。
func TestTimestampKeepsSubSecond(t *testing.T) {
	md := probeMsg(t, "ts_precision", tsField(), "google/protobuf/timestamp.proto")
	field := md.Fields().ByName("ts")

	base := time.Date(2026, 7, 29, 12, 34, 56, 123456789, time.UTC)
	m := dynamicpb.NewMessage(md)
	m.Set(field, protoreflect.ValueOfMessage(timestamppb.New(base).ProtoReflect()))

	got, err := SerializeFieldAsString(m.Interface(), field)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if got != "2026-07-29 12:34:56.123456" {
		t.Fatalf("应保留到微秒, 实际 %q", got)
	}

	// 读回：微秒必须还在（纳秒位是 MySQL 时间类型的硬上限，允许丢）
	dst := dynamicpb.NewMessage(md)
	if err := ParseFromString(dst.Interface(), []string{got}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	back := dst.Get(field).Message().Interface().(*timestamppb.Timestamp).AsTime().UTC()
	if want := base.Truncate(time.Microsecond); !back.Equal(want) {
		t.Errorf("往返丢精度\n实际: %s\n期望: %s", back.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}

	// 整秒值也要能正常往返
	whole := time.Date(2026, 7, 29, 12, 34, 56, 0, time.UTC)
	m2 := dynamicpb.NewMessage(md)
	m2.Set(field, protoreflect.ValueOfMessage(timestamppb.New(whole).ProtoReflect()))
	s2, _ := SerializeFieldAsString(m2.Interface(), field)
	dst2 := dynamicpb.NewMessage(md)
	if err := ParseFromString(dst2.Interface(), []string{s2}); err != nil {
		t.Fatalf("parse whole second: %v", err)
	}
	if b2 := dst2.Get(field).Message().Interface().(*timestamppb.Timestamp).AsTime().UTC(); !b2.Equal(whole) {
		t.Errorf("整秒往返不一致: %s", b2)
	}
}

// TestUnsetTimestampIsSQLNull 未设置的 Timestamp 必须下发 SQL NULL。
// 空串在 STRICT_TRANS_TABLES 下会被 MySQL 拒（Error 1292），导致整行插不进去。
func TestUnsetTimestampIsSQLNull(t *testing.T) {
	md := probeMsg(t, "ts_null", tsField(), "google/protobuf/timestamp.proto")
	field := md.Fields().ByName("ts")

	unset := dynamicpb.NewMessage(md)
	val, err := SerializeFieldValue(unset.Interface(), field)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if val != nil {
		t.Errorf("未设置的 Timestamp 应下发 nil(SQL NULL), 实际 %#v", val)
	}

	// 已设置时仍是字符串
	set := dynamicpb.NewMessage(md)
	set.Set(field, protoreflect.ValueOfMessage(timestamppb.New(time.Date(2026, 7, 29, 1, 2, 3, 0, time.UTC)).ProtoReflect()))
	val2, err := SerializeFieldValue(set.Interface(), field)
	if err != nil {
		t.Fatalf("serialize set: %v", err)
	}
	if s, ok := val2.(string); !ok || s == "" {
		t.Errorf("已设置的 Timestamp 应下发非空字符串, 实际 %#v", val2)
	}
}

// TestUnsetNonTimestampStaysEmptyString 只有 Timestamp 走 NULL；
// 其余字段（未设置的嵌套消息、空容器）的列是 NOT NULL 的 BLOB，必须保持空串
func TestUnsetNonTimestampStaysEmptyString(t *testing.T) {
	src := &testpb.GolangTest{Id: 1} // player 子消息未设置
	fd := src.ProtoReflect().Descriptor().Fields().ByName("player")
	val, err := SerializeFieldValue(src, fd)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	if val != "" {
		t.Errorf("未设置的嵌套消息应为空串而非 NULL, 实际 %#v", val)
	}
}

// TestNonFiniteFloatRejected NaN/±Inf 必须在序列化阶段就报清楚的错。
// MySQL 的 FLOAT/DOUBLE 无法表示它们：STRICT 模式下报 Error 1265（看不出根因），
// 非 STRICT 模式下会被悄悄存成 0，那是静默的数据损坏。
func TestNonFiniteFloatRejected(t *testing.T) {
	for _, k := range []struct {
		name string
		typ  descriptorpb.FieldDescriptorProto_Type
	}{
		{"double", descriptorpb.FieldDescriptorProto_TYPE_DOUBLE},
		{"float", descriptorpb.FieldDescriptorProto_TYPE_FLOAT},
	} {
		md := probeMsg(t, "f_"+k.name, &descriptorpb.FieldDescriptorProto{
			Name: proto.String("v"), Number: proto.Int32(1),
			Type: k.typ.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		})
		field := md.Fields().ByName("v")

		for _, bad := range []struct {
			label string
			val   float64
		}{{"NaN", math.NaN()}, {"+Inf", math.Inf(1)}, {"-Inf", math.Inf(-1)}} {
			m := dynamicpb.NewMessage(md)
			if k.typ == descriptorpb.FieldDescriptorProto_TYPE_FLOAT {
				m.Set(field, protoreflect.ValueOfFloat32(float32(bad.val)))
			} else {
				m.Set(field, protoreflect.ValueOfFloat64(bad.val))
			}
			if _, err := SerializeFieldValue(m.Interface(), field); !errors.Is(err, ErrNonFiniteFloat) {
				t.Errorf("%s %s 应返回 ErrNonFiniteFloat, 实际: %v", k.name, bad.label, err)
			}
		}

		// 有限值不受影响
		m := dynamicpb.NewMessage(md)
		if k.typ == descriptorpb.FieldDescriptorProto_TYPE_FLOAT {
			m.Set(field, protoreflect.ValueOfFloat32(3.5))
		} else {
			m.Set(field, protoreflect.ValueOfFloat64(-1.25e300))
		}
		if _, err := SerializeFieldValue(m.Interface(), field); err != nil {
			t.Errorf("%s 有限值不应报错: %v", k.name, err)
		}
	}
}

// TestTimestampParsesDriverParseTimeForm 连接串开 parseTime=true 时（NewMysqlConfig 的默认值），
// 驱动把 DATETIME 解成 time.Time，再扫进 []byte 会得到 RFC3339 文本而非 MySQL 原生文本。
// 少了这条兼容，所有开了 parseTime 的连接读 Timestamp 列都会解析失败。
func TestTimestampParsesDriverParseTimeForm(t *testing.T) {
	md := probeMsg(t, "ts_driverform", tsField(), "google/protobuf/timestamp.proto")
	field := md.Fields().ByName("ts")

	want := time.Date(2026, 7, 29, 12, 34, 56, 123456000, time.UTC)
	for _, raw := range []string{
		"2026-07-29 12:34:56.123456",  // MySQL 原生文本
		"2026-07-29T12:34:56.123456Z", // parseTime=true 时驱动给的形态
	} {
		dst := dynamicpb.NewMessage(md)
		if err := ParseFromString(dst.Interface(), []string{raw}); err != nil {
			t.Fatalf("解析 %q 失败: %v", raw, err)
		}
		got := dst.Get(field).Message().Interface().(*timestamppb.Timestamp).AsTime().UTC()
		if !got.Equal(want) {
			t.Errorf("解析 %q 得到 %s, 期望 %s", raw, got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
		}
	}

	// 整秒的两种形态也要都能解
	for _, raw := range []string{"2026-07-29 12:34:56", "2026-07-29T12:34:56Z"} {
		dst := dynamicpb.NewMessage(md)
		if err := ParseFromString(dst.Interface(), []string{raw}); err != nil {
			t.Errorf("解析 %q 失败: %v", raw, err)
		}
	}
}

// TestTimestampNullClearsExistingValue 查询结果为SQL NULL时，复用的消息不得保留上一次的值。
func TestTimestampNullClearsExistingValue(t *testing.T) {
	md := probeMsg(t, "ts_null_read", tsField(), "google/protobuf/timestamp.proto")
	field := md.Fields().ByName("ts")
	dst := dynamicpb.NewMessage(md)
	dst.Set(field, protoreflect.ValueOfMessage(timestamppb.Now().ProtoReflect()))

	if err := ParseFromString(dst.Interface(), []string{""}); err != nil {
		t.Fatalf("解析NULL Timestamp失败: %v", err)
	}
	if dst.Has(field) {
		t.Fatal("SQL NULL 应清除复用消息中已有的 Timestamp")
	}
}
