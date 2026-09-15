package proto2mysql

// 从 proto 描述符读取建表元数据（message option / field option），
// 使调用方在 .proto 里声明表配置后，RegisterTable 无需再传任何代码级 TableOption。
//
// 选项定义见本仓库 proto/proto2mysql_option.proto，运行时按字段号反射读取。

import (
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// message option 字段号
const (
	optNumTableName        = 500001 // 表名
	optNumPrimaryKey       = 500002 // 主键（逗号分隔=联合主键）
	optNumAutoIncrementKey = 500006 // 自增字段
	optNumIndex            = 500011 // 普通索引（分号分隔多个索引，每个索引内逗号分隔=联合索引）
	optNumUniqueKey        = 500012 // 唯一键（逗号分隔=联合唯一键）

	// TiDB 方言（生成 /*T!*/ 扩展注释，MySQL 视为普通注释忽略）
	optNumTiDBNonclusteredPK  = 500021 // 主键追加 NONCLUSTERED
	optNumTiDBShardRowIDBits  = 500022 // SHARD_ROW_ID_BITS=N
	optNumTiDBPreSplitRegions = 500023 // PRE_SPLIT_REGIONS=N（依赖 SHARD_ROW_ID_BITS）
	optNumTiDBAutoIDCacheOne  = 500024 // AUTO_ID_CACHE=1
)

// field option 字段号
const (
	optNumFieldNullable  = 600100 // 该字段允许为 NULL
	optNumFieldMaxLength = 600101 // 主键/唯一键 string/bytes 列的最大长度
)

// file option 字段号
const (
	optNumFileDB = 500000 // 标记该 .proto 文件用于 proto2mysql 建表
)

// FileHasDBOption 判断某个 .proto 文件是否声明了文件级 db 选项
// （option (proto2mysql.db) = true;）。用于自动注册时筛选“用于建表”的文件。
func FileHasDBOption(fd protoreflect.FileDescriptor) bool {
	has := false
	rangeExtensions(fd.Options(), func(num protoreflect.FieldNumber, v protoreflect.Value) {
		if num == optNumFileDB && v.Bool() {
			has = true
		}
	})
	return has
}

// TableNameFromDescriptor 读取 message option 中的表名；第二返回值表示该消息是否声明了表选项。
func TableNameFromDescriptor(md protoreflect.MessageDescriptor) (string, bool) {
	name := ""
	rangeExtensions(md.Options(), func(num protoreflect.FieldNumber, v protoreflect.Value) {
		if num == optNumTableName {
			name = v.String()
		}
	})
	return name, name != ""
}

// TableOptionsFromDescriptor 从消息描述符读取建表配置，转换为 TableOption 列表。
// 支持的 message option：表名/主键/自增/索引/唯一键；field option：nullable、max_length。
// RegisterTable / GenerateCreateTableSQL 会自动应用这些选项，代码传入的 TableOption 优先级更高（后应用覆盖）。
func TableOptionsFromDescriptor(md protoreflect.MessageDescriptor) []TableOption {
	var opts []TableOption

	rangeExtensions(md.Options(), func(num protoreflect.FieldNumber, v protoreflect.Value) {
		switch num {
		case optNumTableName:
			if s := v.String(); s != "" {
				opts = append(opts, WithTableName(s))
			}
		case optNumPrimaryKey:
			// 保留空分量交给 validateTableOptions 拒绝。若这里把 "id,,port"
			// 先压成 ["id", "port"]，descriptor 路径就会绕过代码级选项已有的
			// fail-closed 校验，并悄悄改变调用方声明的联合主键。
			opts = append(opts, WithPrimaryKey(splitOptionCSV(v.String())...))
		case optNumAutoIncrementKey:
			if s := strings.TrimSpace(v.String()); s != "" {
				opts = append(opts, WithAutoIncrementKey(s))
			}
		case optNumIndex:
			// 与 primary_key 一样保留空分量，让 schema validator 拒绝
			// "id;;port"，不能静默改写成两个合法索引。
			opts = append(opts, WithIndexes(splitOptionIndexes(v.String())...))
		case optNumUniqueKey:
			if s := strings.TrimSpace(v.String()); s != "" {
				opts = append(opts, WithUniqueKey(s))
			}
		case optNumTiDBNonclusteredPK:
			if v.Bool() {
				opts = append(opts, WithTiDBNonclusteredPK())
			}
		case optNumTiDBShardRowIDBits:
			if n := v.Uint(); n > 0 {
				opts = append(opts, WithTiDBShardRowIDBits(uint32(n)))
			}
		case optNumTiDBPreSplitRegions:
			if n := v.Uint(); n > 0 {
				opts = append(opts, WithTiDBPreSplitRegions(uint32(n)))
			}
		case optNumTiDBAutoIDCacheOne:
			if v.Bool() {
				opts = append(opts, WithTiDBAutoIDCacheOne())
			}
		}
	})

	var nullable []string
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		rangeExtensions(fd.Options(), func(num protoreflect.FieldNumber, v protoreflect.Value) {
			switch num {
			case optNumFieldNullable:
				if v.Bool() {
					nullable = append(nullable, string(fd.Name()))
				}
			case optNumFieldMaxLength:
				// 用类型断言而不是 v.Uint()：别的库若在同一字段号上注册了非整数扩展，v.Uint() 会 panic。
				// 显式写 0 也要传下去交给 validateTableOptions 拒绝，不能当成"没声明"而退回默认长度。
				if n, ok := v.Interface().(uint32); ok {
					opts = append(opts, WithMaxLength(string(fd.Name()), n))
				}
			}
		})
	}
	if len(nullable) > 0 {
		opts = append(opts, WithNullableFields(nullable...))
	}

	return opts
}

// rangeExtensions 遍历 options 消息上已设置的扩展字段（按字段号回调）。
// 按字段号而非扩展类型匹配：动态描述符（protocompile/dynamicpb）与生成代码的扩展类型标识不同，
// 但字段号一致。
//
// 另外补扫 unknown fields：若二进制里链接的选项生成代码（pbopt）旧于运行时使用的选项号
// （例如本库新增选项后使用方尚未重新生成/升级 pbopt），protobuf 解析 options 时会把
// 未注册的扩展落进 unknown fields，Range 看不到——不兜底的话这些选项会被静默丢弃。
// 这里按 wire 格式解出本库定义的选项号，保证选项不因生成代码滞后而失效。
func rangeExtensions(opts protoreflect.ProtoMessage, fn func(protoreflect.FieldNumber, protoreflect.Value)) {
	if opts == nil {
		return
	}
	m := opts.ProtoReflect()
	if !m.IsValid() {
		return
	}
	seen := map[protoreflect.FieldNumber]bool{}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.IsExtension() {
			seen[fd.Number()] = true
			fn(fd.Number(), v)
		}
		return true
	})
	rangeUnknownOptionFields(m.GetUnknown(), seen, fn)
}

// unknownOptionKinds 本库全部选项号 → 声明类型，unknown fields 兜底解码时用。
var unknownOptionKinds = map[protoreflect.FieldNumber]protoreflect.Kind{
	optNumFileDB:              protoreflect.BoolKind,
	optNumTableName:           protoreflect.StringKind,
	optNumPrimaryKey:          protoreflect.StringKind,
	optNumAutoIncrementKey:    protoreflect.StringKind,
	optNumIndex:               protoreflect.StringKind,
	optNumUniqueKey:           protoreflect.StringKind,
	optNumFieldNullable:       protoreflect.BoolKind,
	optNumFieldMaxLength:      protoreflect.Uint32Kind,
	optNumTiDBNonclusteredPK:  protoreflect.BoolKind,
	optNumTiDBShardRowIDBits:  protoreflect.Uint32Kind,
	optNumTiDBPreSplitRegions: protoreflect.Uint32Kind,
	optNumTiDBAutoIDCacheOne:  protoreflect.BoolKind,
}

// rangeUnknownOptionFields 从 options 的 unknown fields 中解出本库定义的选项并回调。
// 只认识 unknownOptionKinds 里的字段号；已经在已知扩展里出现过的字段号（seen）跳过。
// wire 类型与选项声明类型不匹配的字段跳过不回调（正常消费掉字节，保证解析不中断）。
func rangeUnknownOptionFields(b protoreflect.RawFields, seen map[protoreflect.FieldNumber]bool, fn func(protoreflect.FieldNumber, protoreflect.Value)) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]

		kind, known := unknownOptionKinds[num]
		if !known || seen[num] {
			if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return
			}
			b = b[n:]
			continue
		}

		switch typ {
		case protowire.VarintType:
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return
			}
			b = b[m:]
			switch kind {
			case protoreflect.BoolKind:
				fn(num, protoreflect.ValueOfBool(v != 0))
			case protoreflect.Uint32Kind:
				fn(num, protoreflect.ValueOfUint32(uint32(v)))
			}
		case protowire.BytesType:
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return
			}
			b = b[m:]
			if kind == protoreflect.StringKind {
				fn(num, protoreflect.ValueOfString(string(v)))
			}
		default:
			if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return
			}
			b = b[n:]
		}
	}
}

// splitOptionCSV 拆分逗号分隔的字段列表并去掉每项首尾空白，但故意保留空项。
// 空项属于无效 schema 声明，必须由 validateTableOptions 报错，不能在解析层静默吞掉。
func splitOptionCSV(s string) []string {
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// splitOptionIndexes 拆分索引选项：分号分隔多个索引，每个索引保留内部逗号（联合索引）。
// 空索引分量故意保留，交给 validateTableOptions fail-closed。
// 例："last_login" → 1个索引；"player_id;zone_id,created_at" → 2个索引（第2个为联合索引）。
func splitOptionIndexes(s string) []string {
	parts := strings.Split(s, ";")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
