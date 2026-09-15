package pbconv

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SerializeFieldValue 把字段序列化为可直接下发给MySQL的参数值。
// 与SerializeFieldAsString的唯一区别：**未设置的Timestamp返回nil（SQL NULL）而不是空串**。
//
// 为什么必须区分：DATETIME列没有"零值"可写——在NO_ZERO_DATE下'0000-00-00'非法，空串
// 在STRICT_TRANS_TABLES下直接被拒（Error 1292 Incorrect datetime value: ”）。
// 也就是说，只要消息里有一个Timestamp字段没赋值，整行就插不进去。唯一正确的表示是NULL，
// 而string表达不了NULL，所以生成SQL参数一律走本函数，不要用SerializeFieldAsString。
//
// 其余字段（含未设置的嵌套消息/空容器）仍返回空串：它们的列是BLOB系且NOT NULL，
// 空串能正确往返，不需要NULL。
func SerializeFieldValue(message proto.Message, fieldDesc protoreflect.FieldDescriptor) (interface{}, error) {
	val, err := SerializeFieldAsString(message, fieldDesc)
	if err != nil {
		return nil, err
	}
	// Timestamp的空串代表"未设置"，必须落成SQL NULL
	if val == "" && isTimestampField(fieldDesc) {
		return nil, nil
	}
	return val, nil
}

// SerializeFieldAsString 将消息中的单个字段序列化为字符串形式：
//   - Timestamp        -> "2006-01-02 15:04:05.000000"（未设置时为空串，见SerializeFieldValue）
//   - map/list/bytes/嵌套消息 -> proto wire格式**裸字节**（装在string里，非UTF-8文本）
//   - 标量             -> 十进制/布尔字符串
//
// ⚠️ 生成SQL参数请用SerializeFieldValue：本函数无法表达SQL NULL，
// 未设置的Timestamp会得到空串，直接下发会被MySQL拒绝。
//
// 二进制字段不做Base64：目标列是MEDIUMBLOB/VARBINARY，本身二进制安全，编码只会白白多占33%体积
// 并在每次读写上加一次编解码。Go的string可承载任意字节，驱动以参数下发时逐字节无损，
// 因此返回类型仍是string。要在SQL控制台查看，用MySQL自带的TO_BASE64(列)。
//
// ⚠️ 这些字段对应的列必须是二进制类型。写进utf8mb4的TEXT/VARCHAR列会因非法UTF-8被拒或损坏——
// 本库建表时message/map/list映射为MEDIUMBLOB，bytes映射为MEDIUMBLOB（在主键/唯一键里时为
// VARBINARY），只有手工建的表才可能踩到。
func SerializeFieldAsString(message proto.Message, fieldDesc protoreflect.FieldDescriptor) (string, error) {
	if err := rejectRealOneof(fieldDesc); err != nil {
		return "", err
	}
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
		return formatFloat(reflection.Get(fieldDesc).Float(), 32, fieldDesc)
	case protoreflect.DoubleKind:
		return formatFloat(reflection.Get(fieldDesc).Float(), 64, fieldDesc)
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

// formatFloat 格式化浮点数，NaN/±Inf 直接报错而不是交给MySQL。
// MySQL的FLOAT/DOUBLE没有NaN/Inf的表示：下发"NaN"/"+Inf"在STRICT模式下报
// Error 1265 Data truncated（错误信息完全看不出根因），非STRICT模式下会被悄悄存成0——
// 那是静默的数据损坏。这里fail-closed，把问题挡在写库之前。
func formatFloat(f float64, bitSize int, fieldDesc protoreflect.FieldDescriptor) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", fmt.Errorf("%w: field %s = %v", ErrNonFiniteFloat, fieldDesc.Name(), f)
	}
	return strconv.FormatFloat(f, 'f', -1, bitSize), nil
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
	// ErrNonFiniteFloat 浮点字段为NaN或±Inf；MySQL的FLOAT/DOUBLE无法表示，拒绝写入
	ErrNonFiniteFloat = errors.New("non-finite float value (NaN/Inf) cannot be stored in MySQL")
)

// mysqlDateTimeLayout 是写入MySQL DATETIME列的时间格式。
// 必须带6位小数秒：不带的话毫秒/纳秒会被**静默**截断到整秒（不报错、无警告），
// 对应列类型为DATETIME(6)（见MySQLFieldTypes）。
// MySQL时间类型最高只支持微秒(fsp=6)，所以proto Timestamp的纳秒位仍会丢——这是MySQL的硬上限。
//
// ⚠️ 写进未迁移的老DATETIME(0)列时，MySQL会按小数秒**四舍五入**（.9 会进位到下一秒），
// 而不是像旧版本那样在Go侧截断。存量表请先 ALTER ... MODIFY col DATETIME(6)。
const mysqlDateTimeLayout = "2006-01-02 15:04:05.000000"

// timestampParseLayouts 是从MySQL读取时间时支持的格式（按优先级尝试）。
// 注意Go的解析规则：即使layout里没写小数秒，输入带小数秒也会被正确吸收，
// 所以首个layout足以覆盖到微秒精度，无需为DATETIME(6)单独加一条。
//
// 为什么要带RFC3339：连接串开了parseTime=true时（NewMysqlConfig的默认值，README示例的DSN
// 也是这么写的），驱动会把DATETIME解码成time.Time；再扫描进[]byte时，database/sql按
// RFC3339Nano渲染，拿到的是"2026-07-29T12:34:56.123456Z"而不是MySQL的原生文本。
// 少了这两条，凡是开了parseTime的连接读Timestamp列一律解析失败。
var timestampParseLayouts = []string{
	"2006-01-02 15:04:05.999999", // MySQL原生文本（带小数秒，微秒及以下任意位数）
	"2006-01-02 15:04:05",        // MySQL原生文本（不带小数秒）
	time.RFC3339Nano,             // parseTime=true 时驱动转换后的形态
	time.RFC3339,
	"2006-01-02", // 仅日期（DATE列）
}

// isTimestampField 判断字段是否为单值的google.protobuf.Timestamp
func isTimestampField(fd protoreflect.FieldDescriptor) bool {
	return !fd.IsMap() && !fd.IsList() &&
		fd.Kind() == protoreflect.MessageKind &&
		fd.Message() != nil &&
		fd.Message().FullName() == timestampFullName
}

// rejectRealOneof 拒绝非 synthetic oneof 成员。本库把每个字段映射成独立列，
// 没有“未选中”状态：写入会把未选中成员也落成零值，读取逐个 Set 又会让最后一个
// 成员恒胜。proto3 optional 使用的 synthetic oneof 只表达 presence，正常放行。
func rejectRealOneof(fieldDesc protoreflect.FieldDescriptor) error {
	if oneof := fieldDesc.ContainingOneof(); oneof != nil && !oneof.IsSynthetic() {
		return fmt.Errorf("%w: field %s belongs to unsupported oneof %q",
			ErrInvalidFieldKind, fieldDesc.Name(), oneof.Name())
	}
	return nil
}

// ParseFromString 按字段声明顺序，把一行查询结果（字符串切片）反序列化到消息中。
// row[i]对应消息的第i个字段，与SerializeFieldAsString生成的格式对称。
func ParseFromString(message proto.Message, row []string) error {
	reflection := message.ProtoReflect()
	fields := reflection.Descriptor().Fields()
	// 必须先完整预检再修改 message。否则遇到 oneof 时前面的普通字段已经被覆盖，
	// 调用方即使收到 error，也拿不回传入时的主键/原值。
	for i := 0; i < fields.Len(); i++ {
		if err := rejectRealOneof(fields.Get(i)); err != nil {
			return err
		}
	}

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

// parseTimestamp 解析MySQL时间字符串到Timestamp字段（空值清除字段）
func parseTimestamp(reflection protoreflect.Message, fieldDesc protoreflect.FieldDescriptor, raw string) error {
	if raw == "" {
		// SQL NULL 扫描到 []byte 后表现为空值。调用方可能复用已有 message，
		// 因此必须显式清除字段，不能让上一次查询的 Timestamp 残留。
		reflection.Clear(fieldDesc)
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

// parseContainer 反序列化map/list字段（serializeContainer的逆操作）。
//
// **必须先清空目标字段**，理由与 setScalarDefault 那段完全一致：调用方复用同一个
// message 连续读多行是本 API 的天然用法（FindOneByPK 的入参即出参）。原先有两条
// 路径会带着上一行的内容返回：
//
//	raw == ""               → 直接 return，上一行的 map/list 原封不动留着
//	holder.Has 为 false     → 同上
//	map 分支只 Set 新键      → 上一行有、这一行没有的键**永久残留**
//
//	out := &Player{Id: 1}; db.FindOneByPK(out)   // alice: bag={sword:1}
//	out.Id = 2;            db.FindOneByPK(out)   // bob 的 bag 是空的
//	// → out.Bag 仍是 {sword:1}，**bob 拿到了 alice 的背包**
//
// list 分支原本靠 Truncate(0) 侥幸躲过第三条，但躲不过前两条。统一在函数入口 Clear，
// 三条一起堵死；顺带保证 Unmarshal 失败时也不会留下上一行的残值。
func parseContainer(reflection protoreflect.Message, fieldDesc protoreflect.FieldDescriptor, raw string) error {
	reflection.Clear(fieldDesc)
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
	src := holder.Get(fieldDesc).List()
	for i := 0; i < src.Len(); i++ {
		dst.Append(src.Get(i))
	}
	return nil
}

// setScalarDefault 列值为NULL/空时，把字段重置为默认值。
//
// **必须覆盖所有类型，否则会跨行串位。** 原先这里只处理 8 种标量，刻意跳过
// bytes / enum / message（注释写的是"与旧行为一致"）。后果是：
//
//	out := &KitchenSink{Id: 1}; db.FindOneByPK(out)   // alice: payload=..., tier=GOLD
//	out.Id = 2;                 db.FindOneByPK(out)   // bob 这三列都是 NULL
//	// → out.Payload / out.Tier / out.Sub 全是 alice 的值，**bob 拿到了 alice 的数据**
//
// 而 FindOneByPK(out) 的 out 既是入参（主键）又是出参，复用同一个 message 正是这个
// API 的天然用法，所以这不是"误用"。同样的洞在 Python 侧 _set_scalar_default 里
// 一模一样，两边一起修。
//
// 用 Set(fd.Default()) 而不是 Clear()：对 proto3 optional 字段，Set 会把字段标记为
// **已设置**，Clear 则是未设置——差一个 has 位，再写回数据库时 InsertSetFields 就会
// 少一列。MessageKind 没有可 Set 的默认值，只能 Clear（子消息本来就没有
// "零值已设置"这一说）。
func setScalarDefault(reflection protoreflect.Message, fieldDesc protoreflect.FieldDescriptor) {
	switch fieldDesc.Kind() {
	case protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.FloatKind, protoreflect.DoubleKind,
		protoreflect.BoolKind, protoreflect.StringKind,
		// ↓ 这两类原先被漏掉，导致跨行串位
		protoreflect.BytesKind, protoreflect.EnumKind:
		reflection.Set(fieldDesc, fieldDesc.Default())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		// 子消息没有可 Set 的默认值，只能清掉
		reflection.Clear(fieldDesc)
	}
}

// parseFieldErr 统一的字段解析错误格式
func parseFieldErr(kind string, fieldName protoreflect.Name, raw string, err error) error {
	return fmt.Errorf("parse %s field %s: %w (value: %s)", kind, fieldName, err, raw)
}
