package pbconv

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// 复用同一个 message 连读多行，是 FindOneByPK（入参即出参）的天然用法，不是误用。
// setScalarDefault 那段注释里已经把这个失效模式写清楚了，但 map/list 那条路
// 当时没堵上：空容器直接 return、map 只合并新键不删旧键。
//
// 这里用动态 message 造出 map + repeated 两种容器，逐条钉死。

// containerMessage 一个带 map<string,int64> 和 repeated int64 的 message。
func containerMessage(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("container_probe.proto"),
		Package: proto.String("probe"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("ContainerProbe"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("id"), Number: proto.Int32(1),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name: proto.String("bag"), Number: proto.Int32(2),
					Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
					Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
					TypeName: proto.String(".probe.ContainerProbe.BagEntry"),
				},
				{
					Name: proto.String("tags"), Number: proto.Int32(3),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
				},
			},
			NestedType: []*descriptorpb.DescriptorProto{{
				Name:    proto.String("BagEntry"),
				Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name: proto.String("key"), Number: proto.Int32(1),
						Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
					{
						Name: proto.String("value"), Number: proto.Int32(2),
						Type:  descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
						Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
				},
			}},
		}},
	}
	fdesc, err := protodesc.NewFile(fd, nil)
	if err != nil {
		t.Fatalf("构造容器描述符: %v", err)
	}
	return fdesc.Messages().Get(0)
}

// serializedContainers 把一组 map/list 值序列化成 ParseFromString 期望的列值。
func serializedContainers(t *testing.T, md protoreflect.MessageDescriptor, bag map[string]int64, tags []int64) (string, string) {
	t.Helper()
	msg := dynamicpb.NewMessage(md)
	refl := msg.ProtoReflect()

	bagField := md.Fields().ByName("bag")
	if len(bag) > 0 {
		m := refl.Mutable(bagField).Map()
		for k, v := range bag {
			m.Set(protoreflect.ValueOfString(k).MapKey(), protoreflect.ValueOfInt64(v))
		}
	}
	tagsField := md.Fields().ByName("tags")
	if len(tags) > 0 {
		l := refl.Mutable(tagsField).List()
		for _, v := range tags {
			l.Append(protoreflect.ValueOfInt64(v))
		}
	}

	bagRaw, err := SerializeFieldAsString(msg, bagField)
	if err != nil {
		t.Fatalf("序列化 bag: %v", err)
	}
	tagsRaw, err := SerializeFieldAsString(msg, tagsField)
	if err != nil {
		t.Fatalf("序列化 tags: %v", err)
	}
	return bagRaw, tagsRaw
}

// TestParseContainerClearsBetweenRows 第二行是空容器时，第一行的内容必须消失。
//
// 修复前 parseContainer 对 raw == "" 直接 return，上一行的 map/list 原封不动留着：
//
//	out := &Probe{Id: 1}; FindOneByPK(out)   // alice: bag={sword:1}
//	out.Id = 2;           FindOneByPK(out)   // bob 的 bag 是空的
//	// → out.Bag 仍是 {sword:1}，**bob 拿到了 alice 的背包**
func TestParseContainerClearsBetweenRows(t *testing.T) {
	md := containerMessage(t)
	bag1, tags1 := serializedContainers(t, md, map[string]int64{"sword": 1}, []int64{7, 8})
	bag2, tags2 := serializedContainers(t, md, nil, nil)

	out := dynamicpb.NewMessage(md)
	if err := ParseFromString(out, []string{"1", bag1, tags1}); err != nil {
		t.Fatalf("解析第一行: %v", err)
	}
	if got := out.ProtoReflect().Get(md.Fields().ByName("bag")).Map().Len(); got != 1 {
		t.Fatalf("第一行应有 1 个 bag 条目，实际 %d", got)
	}

	if err := ParseFromString(out, []string{"2", bag2, tags2}); err != nil {
		t.Fatalf("解析第二行: %v", err)
	}
	if got := out.ProtoReflect().Get(md.Fields().ByName("bag")).Map().Len(); got != 0 {
		t.Errorf("第二行 bag 是空的，却残留了 %d 个条目——跨行串数据", got)
	}
	if got := out.ProtoReflect().Get(md.Fields().ByName("tags")).List().Len(); got != 0 {
		t.Errorf("第二行 tags 是空的，却残留了 %d 项——跨行串数据", got)
	}
}

// TestParseContainerMapDoesNotMergeStaleKeys map 分支原先只 Set 新键、不清旧键，
// 于是"上一行有、这一行没有"的键**永久残留**——比空容器那条更阴，
// 因为这一行明明有内容，看起来一切正常，只是多了几个别人的键。
func TestParseContainerMapDoesNotMergeStaleKeys(t *testing.T) {
	md := containerMessage(t)
	bag1, tags1 := serializedContainers(t, md, map[string]int64{"sword": 1, "shield": 2}, []int64{1, 2, 3})
	bag2, tags2 := serializedContainers(t, md, map[string]int64{"potion": 5}, []int64{9})

	out := dynamicpb.NewMessage(md)
	if err := ParseFromString(out, []string{"1", bag1, tags1}); err != nil {
		t.Fatalf("解析第一行: %v", err)
	}
	if err := ParseFromString(out, []string{"2", bag2, tags2}); err != nil {
		t.Fatalf("解析第二行: %v", err)
	}

	bag := out.ProtoReflect().Get(md.Fields().ByName("bag")).Map()
	if bag.Len() != 1 {
		var keys []string
		bag.Range(func(k protoreflect.MapKey, _ protoreflect.Value) bool {
			keys = append(keys, k.String())
			return true
		})
		t.Errorf("第二行只有 potion 一个键，实际 %d 个: %v——上一行的键残留了", bag.Len(), keys)
	}
	if !bag.Has(protoreflect.ValueOfString("potion").MapKey()) {
		t.Error("第二行自己的键必须在")
	}

	if got := out.ProtoReflect().Get(md.Fields().ByName("tags")).List().Len(); got != 1 {
		t.Errorf("第二行 tags 只有 1 项，实际 %d 项", got)
	}
}

// realOneofMessage 动态造一个真实 oneof；仓库自己的测试 proto 没有 oneof，
// 因而必须在这里显式覆盖公开转换 API 的 fail-closed 行为。
func realOneofMessage(t *testing.T) *dynamicpb.Message {
	t.Helper()
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("pbconv_oneof_probe.proto"),
		Package: proto.String("probe"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("OneofProbe"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("id"), Number: proto.Int32(1),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name: proto.String("as_text"), Number: proto.Int32(2),
					Type:       descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					OneofIndex: proto.Int32(0),
				},
				{
					Name: proto.String("as_number"), Number: proto.Int32(3),
					Type:       descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label:      descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					OneofIndex: proto.Int32(0),
				},
			},
			OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("payload")}},
		}},
	}
	fdesc, err := protodesc.NewFile(fd, nil)
	if err != nil {
		t.Fatalf("构造 oneof 描述符: %v", err)
	}
	return dynamicpb.NewMessage(fdesc.Messages().Get(0))
}

func requireRealOneofError(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrInvalidFieldKind) {
		t.Fatalf("真实 oneof 必须返回 ErrInvalidFieldKind，实际: %v", err)
	}
	if !strings.Contains(err.Error(), "payload") {
		t.Errorf("错误应点名 oneof payload，实际: %v", err)
	}
}

// TestPublicConversionRejectsRealOneof 本库的一列一个字段模型无法表示“哪个成员被选中”。
// 写入未选中成员会落零值，读取逐个 Set 又会让最后声明成员恒胜；因此所有公开转换入口
// 都必须在数据被序列化或目标 message 被修改之前拒绝真实 oneof。
func TestPublicConversionRejectsRealOneof(t *testing.T) {
	msg := realOneofMessage(t)
	md := msg.Descriptor()
	asText := md.Fields().ByName("as_text")
	payload := md.Oneofs().ByName("payload")
	msg.Set(asText, protoreflect.ValueOfString("keep-me"))

	_, err := SerializeFieldAsString(msg, asText)
	requireRealOneofError(t, err)
	_, err = SerializeFieldValue(msg, asText)
	requireRealOneofError(t, err)

	err = ParseFromString(msg, []string{"9", "replacement", "123"})
	requireRealOneofError(t, err)
	if selected := msg.WhichOneof(payload); selected != asText {
		t.Fatalf("拒绝解析后 oneof 选择不应改变，实际选中 %v", selected)
	}
	if got := msg.Get(asText).String(); got != "keep-me" {
		t.Errorf("拒绝解析后原值不应被改写，实际 %q", got)
	}
}

// optionalMessage 动态造一个 proto3 optional 字段。它在描述符里也是 oneof，
// 但属于 synthetic oneof，只承担 presence 位，必须继续允许正常往返。
func optionalMessage(t *testing.T) *dynamicpb.Message {
	t.Helper()
	fd := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("pbconv_optional_probe.proto"),
		Package: proto.String("probe"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("OptionalProbe"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{
					Name: proto.String("id"), Number: proto.Int32(1),
					Type:  descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
					Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				},
				{
					Name: proto.String("nickname"), Number: proto.Int32(2),
					Type:           descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					Label:          descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					OneofIndex:     proto.Int32(0),
					Proto3Optional: proto.Bool(true),
				},
			},
			OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("_nickname")}},
		}},
	}
	fdesc, err := protodesc.NewFile(fd, nil)
	if err != nil {
		t.Fatalf("构造 optional 描述符: %v", err)
	}
	return dynamicpb.NewMessage(fdesc.Messages().Get(0))
}

func TestPublicConversionAllowsProto3Optional(t *testing.T) {
	msg := optionalMessage(t)
	nickname := msg.Descriptor().Fields().ByName("nickname")
	msg.Set(nickname, protoreflect.ValueOfString("alice"))

	if got, err := SerializeFieldAsString(msg, nickname); err != nil || got != "alice" {
		t.Fatalf("SerializeFieldAsString(optional) = %q, %v", got, err)
	}
	if got, err := SerializeFieldValue(msg, nickname); err != nil || got != "alice" {
		t.Fatalf("SerializeFieldValue(optional) = %v, %v", got, err)
	}

	out := optionalMessage(t)
	if err := ParseFromString(out, []string{"7", "bob"}); err != nil {
		t.Fatalf("ParseFromString(optional): %v", err)
	}
	outNickname := out.Descriptor().Fields().ByName("nickname")
	if !out.Has(outNickname) || out.Get(outNickname).String() != "bob" {
		t.Errorf("optional 往返失败: has=%v value=%q", out.Has(outNickname), out.Get(outNickname).String())
	}
}
