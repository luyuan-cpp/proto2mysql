package pbconv

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SerializeFieldAsString 将消息中的单个字段序列化为可直接下发给MySQL的值：
//   - Timestamp        -> "2006-01-02 15:04:05"
//   - map/list/bytes/嵌套消息 -> proto wire格式**裸字节**（装在string里，非UTF-8文本）
//   - 标量             -> 十进制/布尔字符串
//
// 二进制字段不做Base64：目标列是MEDIUMBLOB，本身二进制安全，编码只会白白多占33%体积
// 并在每次读写上加一次编解码。Go的string可承载任意字节，驱动以参数下发时逐字节无损，
// 因此返回类型仍是string。要在SQL控制台查看，用MySQL自带的TO_BASE64(列)。
//
// ⚠️ 这些字段对应的列必须是二进制类型（BLOB系）。写进utf8mb4的TEXT/VARCHAR列会因
// 非法UTF-8被拒或损坏——本库建表时bytes/message/map/list统一映射为MEDIUMBLOB，
// 只有手工建的表才可能踩到。
func SerializeFieldAsString(message proto.Message, fieldDesc protoreflect.FieldDescriptor) (string, error) {
	reflection := message.ProtoReflect()

	if isTimestampField(fieldDesc) {
		return serializeTimestamp(reflection, fieldDesc)
	}
	if fieldDesc.IsMap() || fieldDesc.IsList() {
		return serializeContainer(reflection, fieldDesc)
	}

	switch fieldDesc.Kind() {
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		return strconv.FormatInt(reflection.Get(fieldDesc).Int(), 10), nil
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		return strconv.FormatUint(reflection.Get(fieldDesc).Uint(), 10), nil
	case protoreflect.FloatKind:
		return strconv.FormatFloat(reflection.Get(fieldDesc).Float(), 'f', -1, 32), nil
	case protoreflect.DoubleKind:
		return strconv.FormatFloat(reflection.Get(fieldDesc).Float(), 'f', -1, 64), nil
	case protoreflect.StringKind:
		return reflection.Get(fieldDesc).String(), nil
	case protoreflect.BoolKind:
		// 必须是 "1"/"0" 而不是 "true"/"false"：目标列是 tinyint(1)，
		// 在 STRICT_TRANS_TABLES 下写 'true' 会被拒（Error 1366 Incorrect integer value）。
		// 读侧 strconv.ParseBool 同时吃 "1"/"0" 与 "true"/"false"，所以对存量行向后兼容。
		if reflection.Get(fieldDesc).Bool() {
			return "1", nil
		}
		return "0", nil
	case protoreflect.EnumKind:
		return strconv.FormatInt(int64(reflection.Get(fieldDesc).Enum()), 10), nil
	case protoreflect.BytesKind:
		return string(reflection.Get(fieldDesc).Bytes()), nil
	case protoreflect.MessageKind:
		if !reflection.Has(fieldDesc) {
			return "", nil
		}
		data, err := proto.Marshal(reflection.Get(fieldDesc).Message().Interface())
		if err != nil {
			return "", fmt.Errorf("marshal sub-message field %s: %w", fieldDesc.Name(), err)
		}
		return string(data), nil
	default:
		return "", fmt.Errorf("%w: %v (field: %s)", ErrInvalidFieldKind, fieldDesc.Kind(), fieldDesc.Name())
	}
}

// serializeTimestamp 将Timestamp字段格式化为MySQL DATETIME字符串（未设置或零值返回空串）
func serializeTimestamp(reflection protoreflect.Message, fieldDesc protoreflect.FieldDescriptor) (string, error) {
	if !reflection.Has(fieldDesc) {
		return "", nil
	}
	ts, ok := reflection.Get(fieldDesc).Message().Interface().(*timestamppb.Timestamp)
	if !ok {
		return "", fmt.Errorf("field %s is not a Timestamp", fieldDesc.Name())
	}
	if ts.AsTime().IsZero() {
		return "", nil
	}
	return ts.AsTime().Format(mysqlDateTimeLayout), nil
}

// serializeContainer 序列化map/list字段：将字段放入一个同类型的空消息中，
// 用标准proto wire格式编码为裸字节，保证与parseContainer对称可逆。
func serializeContainer(reflection protoreflect.Message, fieldDesc protoreflect.FieldDescriptor) (string, error) {
	if !reflection.Has(fieldDesc) {
		return "", nil
	}
	holder := reflection.New()
	holder.Set(fieldDesc, reflection.Get(fieldDesc))
	data, err := proto.Marshal(holder.Interface())
	if err != nil {
		return "", fmt.Errorf("serialize field %s: %w", fieldDesc.Name(), err)
	}
	return string(data), nil
}

var (
	// timestampFullName 是google.protobuf.Timestamp的全名，用于字段类型判断
	timestampFullName = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().FullName()

	ErrInvalidFieldKind = errors.New("invalid field kind")
)

// mysqlDateTimeLayout 是写入MySQL DATETIME列的时间格式
const mysqlDateTimeLayout = "2006-01-02 15:04:05"

// timestampParseLayouts 是从MySQL读取时间时支持的格式（按优先级尝试）
var timestampParseLayouts = []string{
	"2006-01-02 15:04:05.999", // 带毫秒
	"2006-01-02 15:04:05",     // 不带毫秒
	"2006-01-02",              // 仅日期
}

// isTimestampField 判断字段是否为单值的google.protobuf.Timestamp
func isTimestampField(fd protoreflect.FieldDescriptor) bool {
	return !fd.IsMap() && !fd.IsList() &&
		fd.Kind() == protoreflect.MessageKind &&
		fd.Message() != nil &&
		fd.Message().FullName() == timestampFullName
}

// ParseFromString 按字段声明顺序，把一行查询结果（字符串切片）反序列化到消息中。
// row[i]对应消息的第i个字段，与SerializeFieldAsString生成的格式对称。
func ParseFromString(message proto.Message, row []string) error {
	reflection := message.ProtoReflect()
	fields := reflection.Descriptor().Fields()

	count := fields.Len()
	if len(row) < count {
		count = len(row)
	}

	for i := 0; i < count; i++ {
		if err := setFieldFromString(reflection, fields.Get(i), row[i]); err != nil {
			return err
		}
	}
	return nil
}

// setFieldFromString 将单个字符串值反序列化到消息的指定字段
func setFieldFromString(reflection protoreflect.Message, fieldDesc protoreflect.FieldDescriptor, raw string) error {
	fieldName := fieldDesc.Name()

	if isTimestampField(fieldDesc) {
		return parseTimestamp(reflection, fieldDesc, raw)
	}
	if fieldDesc.IsMap() || fieldDesc.IsList() {
		return parseContainer(reflection, fieldDesc, raw)
	}
	if raw == "" {
		setScalarDefault(reflection, fieldDesc)
		return nil
	}

	switch fieldDesc.Kind() {
	case protoreflect.Int32Kind:
		val, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return parseFieldErr("int32", fieldName, raw, err)
		}
		reflection.Set(fieldDesc, protoreflect.ValueOfInt32(int32(val)))
	case protoreflect.Int64Kind:
		val, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return parseFieldErr("int64", fieldName, raw, err)
		}
		reflection.Set(fieldDesc, protoreflect.ValueOfInt64(val))
	case protoreflect.Uint32Kind:
		val, err := strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return parseFieldErr("uint32", fieldName, raw, err)
		}
		reflection.Set(fieldDesc, protoreflect.ValueOfUint32(uint32(val)))
	case protoreflect.Uint64Kind:
		val, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return parseFieldErr("uint64", fieldName, raw, err)
		}
		reflection.Set(fieldDesc, protoreflect.ValueOfUint64(val))
	case protoreflect.FloatKind:
		val, err := strconv.ParseFloat(raw, 32)
		if err != nil {
			return parseFieldErr("float", fieldName, raw, err)
		}
		reflection.Set(fieldDesc, protoreflect.ValueOfFloat32(float32(val)))
	case protoreflect.DoubleKind:
		val, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return parseFieldErr("double", fieldName, raw, err)
		}
		reflection.Set(fieldDesc, protoreflect.ValueOfFloat64(val))
	case protoreflect.StringKind:
		reflection.Set(fieldDesc, protoreflect.ValueOfString(raw))
	case protoreflect.BoolKind:
		val, err := strconv.ParseBool(raw)
		if err != nil {
			return parseFieldErr("bool", fieldName, raw, err)
		}
		reflection.Set(fieldDesc, protoreflect.ValueOfBool(val))
	case protoreflect.EnumKind:
		val, err := strconv.Atoi(raw)
		if err != nil {
			return parseFieldErr("enum", fieldName, raw, err)
		}
		reflection.Set(fieldDesc, protoreflect.ValueOfEnum(protoreflect.EnumNumber(val)))
	case protoreflect.BytesKind:
		reflection.Set(fieldDesc, protoreflect.ValueOfBytes([]byte(raw)))
	case protoreflect.MessageKind:
		subMsg := reflection.Mutable(fieldDesc).Message()
		// 出错时不打raw：里面是proto裸字节，直接进日志会喷控制字符
		if err := proto.Unmarshal([]byte(raw), subMsg.Interface()); err != nil {
			return fmt.Errorf("unmarshal sub-message field %s: %w (%d bytes)", fieldName, err, len(raw))
		}
	default:
		return fmt.Errorf("%w: %v (field: %s)", ErrInvalidFieldKind, fieldDesc.Kind(), fieldName)
	}
	return nil
}

// parseTimestamp 解析MySQL时间字符串到Timestamp字段（空值跳过）
func parseTimestamp(reflection protoreflect.Message, fieldDesc protoreflect.FieldDescriptor, raw string) error {
	if raw == "" {
		return nil
	}
	var parsed time.Time
	var err error
	for _, layout := range timestampParseLayouts {
		parsed, err = time.Parse(layout, raw)
		if err == nil {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("parse timestamp field %s: %w (value: %s)", fieldDesc.Name(), err, raw)
	}
	reflection.Set(fieldDesc, protoreflect.ValueOfMessage(timestamppb.New(parsed).ProtoReflect()))
	return nil
}

// parseContainer 反序列化map/list字段（serializeContainer的逆操作）
func parseContainer(reflection protoreflect.Message, fieldDesc protoreflect.FieldDescriptor, raw string) error {
	if raw == "" {
		return nil
	}
	holder := reflection.New()
	if err := proto.Unmarshal([]byte(raw), holder.Interface()); err != nil {
		return fmt.Errorf("parse field %s: %w (%d bytes)", fieldDesc.Name(), err, len(raw))
	}
	if !holder.Has(fieldDesc) {
		return nil
	}

	if fieldDesc.IsMap() {
		dst := reflection.Mutable(fieldDesc).Map()
		holder.Get(fieldDesc).Map().Range(func(key protoreflect.MapKey, val protoreflect.Value) bool {
			dst.Set(key, val)
			return true
		})
		return nil
	}

	dst := reflection.Mutable(fieldDesc).List()
	dst.Truncate(0)
	src := holder.Get(fieldDesc).List()
	for i := 0; i < src.Len(); i++ {
		dst.Append(src.Get(i))
	}
	return nil
}

// setScalarDefault 空字符串时把标量字段重置为默认值（bytes/message/enum保持不变，与旧行为一致）
func setScalarDefault(reflection protoreflect.Message, fieldDesc protoreflect.FieldDescriptor) {
	switch fieldDesc.Kind() {
	case protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.FloatKind, protoreflect.DoubleKind,
		protoreflect.BoolKind, protoreflect.StringKind:
		reflection.Set(fieldDesc, fieldDesc.Default())
	}
}

// parseFieldErr 统一的字段解析错误格式
func parseFieldErr(kind string, fieldName protoreflect.Name, raw string, err error) error {
	return fmt.Errorf("parse %s field %s: %w (value: %s)", kind, fieldName, err, raw)
}
