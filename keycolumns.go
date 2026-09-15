package proto2mysql

// 主键/唯一键里的 string/bytes 列（下称键列）：写入前的值校验、表选项校验，以及线上旧形态的识别与迁移 SQL。
// 列类型映射见 getMySQLFieldType，长度与排序规则常量见 DefaultKeyColumnLength 一组。

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/luyuancpp/proto2mysql/pbconv"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// serializeColumnValue 把消息字段序列化成下发给数据库的参数值。写入、主键 WHERE 条件与缓存 key
// 全部经由这里取值，键列的长度与编码校验因此一定发生在 SQL 发出之前；非键列的结果与
// pbconv.SerializeFieldValue 完全相同。
func (m *MessageTable) serializeColumnValue(message proto.Message, field protoreflect.FieldDescriptor) (interface{}, error) {
	value, err := pbconv.SerializeFieldValue(message, field)
	if err != nil {
		return nil, err
	}
	if err := m.checkKeyColumnValue(field, value); err != nil {
		return nil, err
	}
	return value, nil
}

// checkKeyColumnValue string 键列按字符数对照 VARCHAR(N) 并要求合法 UTF-8，bytes 键列按字节数对照
// VARBINARY(N)。空串合法：未赋值的键列写的就是它。
// 报错只给长度、不回显值：键列里常是第三方账号 ID、令牌这类不该进日志的内容。
func (m *MessageTable) checkKeyColumnValue(field protoreflect.FieldDescriptor, value interface{}) error {
	kind, ok := m.keyColumnKind(field)
	if !ok {
		return nil
	}
	name := string(field.Name())
	text, isText := value.(string)
	if !isText {
		return fmt.Errorf("%w: 表 %s 的键列 %s 序列化结果是 %T，无法校验长度",
			ErrInvalidKeyValue, m.tableName, name, value)
	}
	limit := int(m.keyColumnLength(name))
	if kind == protoreflect.BytesKind {
		if n := len(text); n > limit {
			return fmt.Errorf("%w: 表 %s 的键列 %s 长 %d 字节，超过 VARBINARY(%d) 的上限",
				ErrInvalidKeyValue, m.tableName, name, n, limit)
		}
		return nil
	}
	if !utf8.ValidString(text) {
		return fmt.Errorf("%w: 表 %s 的键列 %s 不是合法 UTF-8（%d 字节），utf8mb4 列无法原样存储",
			ErrInvalidKeyValue, m.tableName, name, len(text))
	}
	if n := utf8.RuneCountInString(text); n > limit {
		return fmt.Errorf("%w: 表 %s 的键列 %s 长 %d 个字符，超过 VARCHAR(%d) 的上限",
			ErrInvalidKeyValue, m.tableName, name, n, limit)
	}
	return nil
}

// validateKeyValues 在分批或逐行执行之前，把整批消息的键列值先校验一遍。
// 这些入口会发出多条语句且不是原子的：只靠生成每条语句时顺带校验，第 N 批里的超长值要等
// 前 N-1 批已经落库之后才被发现。
// primaryKeyOnly 只校验主键列：按主键删除本来只序列化主键，不能因为唯一键列的值拒绝删除。
// descriptor 不属于本表的消息跳过，交给各入口原有的 descriptor 校验报错。
func (m *MessageTable) validateKeyValues(messages []proto.Message, primaryKeyOnly bool) error {
	var keyFields []protoreflect.FieldDescriptor
	fields := m.Descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if _, ok := m.keyColumnKind(field); !ok {
			continue
		}
		if primaryKeyOnly && !slices.Contains(m.primaryKey, string(field.Name())) {
			continue
		}
		keyFields = append(keyFields, field)
	}
	if len(keyFields) == 0 {
		return nil
	}
	if err := m.validateFieldKinds(); err != nil {
		return err
	}
	for i, message := range messages {
		if message == nil || message.ProtoReflect().Descriptor() != m.Descriptor {
			continue
		}
		for _, field := range keyFields {
			if _, err := m.serializeColumnValue(message, field); err != nil {
				return fmt.Errorf("row %d: %w", i, err)
			}
		}
	}
	return nil
}

// validateMaxLengths max_length 只作用于主键/唯一键里的 string/bytes 字段，且取值必须落在列类型能建
// 整列索引的区间内。字段按名字排序后逐个检查，多处错误时每次报的是同一条。
func (m *MessageTable) validateMaxLengths(lookup func(option, name string) (protoreflect.FieldDescriptor, error)) error {
	names := make([]string, 0, len(m.maxLengths))
	for name := range m.maxLengths {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		field, err := lookup("max_length", name)
		if err != nil {
			return err
		}
		kind, ok := m.keyColumnKind(field)
		if !ok {
			return fmt.Errorf("%w: 表 %s 的字段 %q（%s）声明了 max_length，但 max_length 目前仅用于主键/唯一键的 string/bytes 字段",
				ErrInvalidTableOption, m.tableName, name, describeKeyCandidate(field))
		}
		n := m.maxLengths[name]
		limit, unit := uint32(MaxKeyStringLength), "个字符"
		if kind == protoreflect.BytesKind {
			limit, unit = MaxKeyBytesLength, "个字节"
		}
		if n == 0 || n > limit {
			return fmt.Errorf("%w: 表 %s 的 %s 键列 %q 的 max_length=%d 越界，取值须在 1..%d %s之间"+
				"（单个索引键最多 %d 字节，VARCHAR 每字符按 4 字节、VARBINARY 每字节按 1 字节计）",
				ErrInvalidTableOption, m.tableName, kind, name, n, limit, unit, MaxIndexKeyBytes)
		}
	}
	return nil
}

func describeKeyCandidate(field protoreflect.FieldDescriptor) string {
	switch {
	case field.IsMap():
		return "map"
	case field.IsList():
		return "repeated " + field.Kind().String()
	case field.Kind() == protoreflect.StringKind || field.Kind() == protoreflect.BytesKind:
		return field.Kind().String() + "，不在主键/唯一键里"
	}
	return field.Kind().String()
}

// validateIndexKeyBytes 主键、唯一键、每个普通索引各自的键长不得超过 MaxIndexKeyBytes。
// 超出时建表报 Error 1071，那条报错只给上限不点名列；这里逐列列出各自占的字节数。
// 调用前各键定义引用的字段都已确认存在。
func (m *MessageTable) validateIndexKeyBytes() error {
	check := func(option string, columns []string) error {
		total := 0
		parts := make([]string, 0, len(columns))
		for _, name := range columns {
			n, how := m.indexKeyPartBytes(m.Descriptor.Fields().ByName(protoreflect.Name(name)))
			total += n
			parts = append(parts, fmt.Sprintf("%s %s=%d", name, how, n))
		}
		if total <= MaxIndexKeyBytes {
			return nil
		}
		return fmt.Errorf("%w: 表 %s 的 %s 键长 %d 字节，超过单个索引 %d 字节的上限（建表会报 Error 1071）：%s；"+
			"请调小 string/bytes 键列的 max_length 或减少索引列",
			ErrInvalidTableOption, m.tableName, option, total, MaxIndexKeyBytes, strings.Join(parts, "，"))
	}
	if len(m.primaryKey) > 0 {
		if err := check("primary_key", m.primaryKey); err != nil {
			return err
		}
	}
	if m.uniqueKeys != "" {
		if err := check("unique_key", splitTrimmed(m.uniqueKeys)); err != nil {
			return err
		}
	}
	for i, spec := range m.indexes {
		if err := check(fmt.Sprintf("index[%d]", i), splitTrimmed(spec)); err != nil {
			return err
		}
	}
	return nil
}

// indexKeyPartBytes 一列在索引键里占的字节数及算法说明，判定顺序与 getMySQLFieldType 一致：
// 键列 VARCHAR(N) 按 utf8mb4 每字符 4 字节、VARBINARY(N) 按 N 字节；其余 TEXT/BLOB 系只进 191 前缀。
func (m *MessageTable) indexKeyPartBytes(field protoreflect.FieldDescriptor) (int, string) {
	if field.Message() != nil && field.Message().FullName() == timestampFullName {
		return 8, "DATETIME(6)"
	}
	if field.IsMap() || field.IsList() {
		return TextIndexPrefixLength, fmt.Sprintf("MEDIUMBLOB 前缀(%d)", TextIndexPrefixLength)
	}
	if kind, ok := m.keyColumnKind(field); ok {
		n := int(m.keyColumnLength(string(field.Name())))
		if kind == protoreflect.StringKind {
			return 4 * n, fmt.Sprintf("VARCHAR(%d)×4", n)
		}
		return n, fmt.Sprintf("VARBINARY(%d)", n)
	}
	switch field.Kind() {
	case protoreflect.StringKind:
		return 4 * TextIndexPrefixLength, fmt.Sprintf("MEDIUMTEXT 前缀(%d)×4", TextIndexPrefixLength)
	case protoreflect.BytesKind, protoreflect.MessageKind:
		return TextIndexPrefixLength, fmt.Sprintf("MEDIUMBLOB 前缀(%d)", TextIndexPrefixLength)
	case protoreflect.Int32Kind, protoreflect.Uint32Kind, protoreflect.EnumKind, protoreflect.FloatKind:
		return 4, field.Kind().String()
	case protoreflect.Int64Kind, protoreflect.Uint64Kind, protoreflect.DoubleKind:
		return 8, field.Kind().String()
	case protoreflect.BoolKind:
		return 1, field.Kind().String()
	}
	return 0, field.Kind().String()
}

// legacyKeyColumn 一列线上形态不符合键列要求的主键/唯一键 string/bytes 列。
type legacyKeyColumn struct {
	field      protoreflect.FieldDescriptor
	onlineName string
	meta       columnMeta
	reason     string
}

// legacyKeyColumnReason 线上列不是键列应有的形态时返回原因，形态正确返回空串。
// 只认 string→varchar + KeyStringCollation、bytes→varbinary：其余形态的唯一性语义都与写入路径不一致，
// 不能再按普通的 nullable/default/索引漂移去比较，那会给出误导性的修法。长度差异不在此列，
// 由正常的拓宽/不收窄规则处理。
func legacyKeyColumnReason(kind protoreflect.Kind, meta columnMeta) string {
	base := normalizeBaseType(parseMySQLType(meta.colType).baseType)
	if kind == protoreflect.BytesKind {
		switch base {
		case "varbinary":
			return ""
		case "tinyblob", "blob", "mediumblob", "longblob":
			return fmt.Sprintf("BLOB 列只能建前缀索引，唯一性只覆盖前 %d 个字节", TextIndexPrefixLength)
		case "binary":
			return "BINARY 定长、写入时右补 0x00，'a' 与 'a\\0' 会存成同一个值"
		}
		return "线上类型不是 bytes 键列要求的 VARBINARY，无法确认按字节唯一"
	}
	switch base {
	case "varchar":
		return keyCollationReason(meta.collation)
	case "tinytext", "text", "mediumtext", "longtext":
		return fmt.Sprintf("TEXT 列只能建前缀索引，唯一性只覆盖前 %d 个字符", TextIndexPrefixLength)
	case "char":
		return "CHAR 比较时忽略尾部空格，'abc' 与 'abc ' 会被判为同一个键"
	}
	return "线上类型不是 string 键列要求的 VARCHAR，无法确认按字符唯一"
}

func keyCollationReason(collation string) string {
	lower := strings.ToLower(collation)
	switch {
	case lower == KeyStringCollation:
		return ""
	case lower == "":
		return "读不到排序规则，无法确认是否区分大小写、是否比较尾部空格"
	case strings.HasSuffix(lower, "_ci"):
		return fmt.Sprintf("排序规则 %s 不区分大小写，'AbC' 与 'abc' 会被判为同一个键", collation)
	case !strings.Contains(lower, "_0900_"):
		return fmt.Sprintf("排序规则 %s 是 PAD SPACE，'abc' 与 'abc ' 会被判为同一个键", collation)
	}
	return fmt.Sprintf("排序规则 %s 不是按码点比较，不同的字符串可能被判为同一个键", collation)
}

// legacyKeyColumnError 把旧形态键列汇总成一条同时满足 ErrSchemaDrift 与 ErrLegacyKeyColumn 的错误：
// 每列的线上形态与原因、迁移步骤，以及按本表实际名字填好、在 MySQL 与 TiDB 上都能执行的 SQL。
// otherDrifts 是与这些列无关、同一轮发现的其它漂移，附在末尾。
func (m *MessageTable) legacyKeyColumnError(
	legacy []legacyKeyColumn, otherDrifts []string,
	currentCols map[string]columnMeta,
	existingIndexes map[string]indexMeta, indexesKnown bool,
) error {
	var b strings.Builder
	fmt.Fprintf(&b, "表 %s 有 %d 个主键/唯一键 string/bytes 列仍是旧形态，拒绝自动同步（本次未执行任何 DDL）：",
		m.tableName, len(legacy))
	inPrimaryKey := false
	for _, col := range legacy {
		name := string(col.field.Name())
		online := col.meta.colType
		if col.meta.collation != "" {
			online += " COLLATE " + col.meta.collation
		}
		if col.meta.nullable {
			online += " NULL"
		}
		fmt.Fprintf(&b, "\n  - 列 %s（%s）：线上 %s，期望 %s；原因：%s",
			col.onlineName, m.keyColumnRoles(name), online, m.getMySQLFieldType(col.field), col.reason)
		if slices.Contains(m.primaryKey, name) {
			inPrimaryKey = true
		}
	}

	var statements []string
	if inPrimaryKey {
		statements = m.legacyKeyRebuildSQL(&b, legacy, currentCols)
	} else {
		statements = m.legacyKeyAlterSQL(&b, legacy, existingIndexes, indexesKnown)
	}
	b.WriteString("\n" + legacyKeySQLHeader)
	for _, stmt := range statements {
		b.WriteString("\n    ")
		b.WriteString(stmt)
		b.WriteString(";")
	}
	if len(otherDrifts) > 0 {
		fmt.Fprintf(&b, "\n另有与这些键列无关的漂移：%s", strings.Join(otherDrifts, "; "))
	}
	return fmt.Errorf("%w: %w: %s", ErrSchemaDrift, ErrLegacyKeyColumn, b.String())
}

// legacyKeySQLHeader 迁移 SQL 块的标题行；其后每条语句独占一行、以 4 个空格缩进、以分号结尾。
const legacyKeySQLHeader = "可直接执行的 SQL（按顺序逐条执行）："

func (m *MessageTable) keyColumnRoles(name string) string {
	var roles []string
	if slices.Contains(m.primaryKey, name) {
		roles = append(roles, "主键")
	}
	if m.uniqueKeys != "" && slices.Contains(splitTrimmed(m.uniqueKeys), name) {
		roles = append(roles, "唯一键")
	}
	for i, spec := range m.indexes {
		if slices.Contains(splitTrimmed(spec), name) {
			roles = append(roles, fmt.Sprintf("index[%d]", i))
		}
	}
	return strings.Join(roles, "、")
}

// legacyKeyAlterSQL 旧形态列都不在主键里时，按"删索引 → NULL 改成空串 → MODIFY → 整列重建索引"原地迁移。
// 必须拆成独立语句：TiDB 不允许在同一条 ALTER 里 DROP 后复用同一个索引名（Error 1061），
// 也不允许给仍带索引的列改排序规则（Error 8200）；前缀索引也不会随 MODIFY 自动变成整列索引。
func (m *MessageTable) legacyKeyAlterSQL(
	b *strings.Builder, legacy []legacyKeyColumn,
	existingIndexes map[string]indexMeta, indexesKnown bool,
) []string {
	legacyNames := make(map[string]struct{}, len(legacy))
	onlineNames := make(map[string]struct{}, len(legacy))
	for _, col := range legacy {
		legacyNames[string(col.field.Name())] = struct{}{}
		onlineNames[strings.ToLower(col.onlineName)] = struct{}{}
	}

	type rebuild struct {
		name string
		add  string
	}
	var rebuilds []rebuild
	declared := make(map[string]struct{})
	if m.uniqueKeys != "" {
		name := m.uniqueKeyName()
		declared[strings.ToLower(name)] = struct{}{}
		if columnsReferenceAny(splitTrimmed(m.uniqueKeys), legacyNames) {
			rebuilds = append(rebuilds, rebuild{name, fmt.Sprintf("ADD UNIQUE KEY %s (%s)",
				escapeMySQLName(name), m.indexColumnsSQL(m.uniqueKeys))})
		}
	}
	for i, spec := range m.indexes {
		name := m.indexNameFor(i)
		declared[strings.ToLower(name)] = struct{}{}
		if columnsReferenceAny(splitTrimmed(spec), legacyNames) {
			rebuilds = append(rebuilds, rebuild{name, fmt.Sprintf("ADD INDEX %s (%s)",
				escapeMySQLName(name), m.indexColumnsSQL(spec))})
		}
	}

	b.WriteString("\n迁移步骤（停写或维护窗口内执行；删索引到重建索引之间唯一性不受约束）：")
	b.WriteString("\n  1) 预检：NULL 改成 '' 后会互相重复、也会与已有的 '' 撞键，须先人工改成唯一值或删除；" +
		"string 列的 CHAR_LENGTH、bytes 列的 LENGTH 不得超过目标长度，否则 MODIFY 会失败")
	b.WriteString("\n  2) 删除包含这些列的索引：前缀索引不会随 MODIFY 变成整列索引，TiDB 也不允许给带索引的列改排序规则")
	b.WriteString("\n  3) 把 NULL 改写成 ''，再把列 MODIFY 成期望类型")
	b.WriteString("\n  4) 按整列重建索引；完成后重新同步应零漂移")
	if indexesKnown {
		var extra []string
		for name, meta := range existingIndexes {
			if _, ok := declared[strings.ToLower(name)]; ok {
				continue
			}
			for _, column := range meta.columns {
				if _, ok := onlineNames[strings.ToLower(column.name)]; ok {
					extra = append(extra, name)
					break
				}
			}
		}
		sort.Strings(extra)
		if len(extra) > 0 {
			fmt.Fprintf(b, "\n  注意：线上另有包含这些列、但不是本库声明的索引 %v；TiDB 上须先删掉它们才能 MODIFY，迁移后按需手工重建", extra)
		}
	}

	table := escapeMySQLName(m.tableName)
	var statements []string
	for _, r := range rebuilds {
		if indexesKnown {
			if _, ok := lookupIndexMeta(existingIndexes, r.name); !ok {
				continue
			}
		}
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", table, escapeMySQLName(r.name)))
	}
	for _, col := range legacy {
		if col.meta.nullable {
			online := escapeMySQLName(col.onlineName)
			statements = append(statements, fmt.Sprintf("UPDATE %s SET %s = '' WHERE %s IS NULL", table, online, online))
		}
	}
	for _, col := range legacy {
		name := string(col.field.Name())
		definition := m.getMySQLFieldType(col.field) + columnComment(col.field.Number())
		if col.onlineName == name {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s",
				table, escapeMySQLName(name), definition))
			continue
		}
		// 重建的索引按 proto 字段名引用列，线上名字不同时必须在同一步改名。
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s CHANGE COLUMN %s %s %s",
			table, escapeMySQLName(col.onlineName), escapeMySQLName(name), definition))
	}
	for _, r := range rebuilds {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s %s", table, r.add))
	}
	return statements
}

// legacyKeyRebuildSQL 旧形态列在主键里时按影子表重建：TiDB 聚簇主键既不允许 DROP PRIMARY KEY，
// 也不允许给带索引的列改排序规则（均为 Error 8200，先建空表再 MODIFY 也一样），原地 ALTER 走不通；
// 建新表 → INSERT ... SELECT → RENAME 在 MySQL 与 TiDB 上都可执行。
// 影子表只换表名，索引名保留原表的 idx_<表名>_N / uk_<表名>，RENAME 回来之后同步才认得出这些索引。
func (m *MessageTable) legacyKeyRebuildSQL(b *strings.Builder, legacy []legacyKeyColumn, currentCols map[string]columnMeta) []string {
	table := escapeMySQLName(m.tableName)
	shadow := escapeMySQLName(truncateIdentifier(m.tableName + "__p2m_new"))
	backup := escapeMySQLName(truncateIdentifier(m.tableName + "__p2m_old"))

	nullableLegacy := make(map[string]struct{}, len(legacy))
	for _, col := range legacy {
		if col.meta.nullable {
			nullableLegacy[string(col.field.Name())] = struct{}{}
		}
	}

	var targets, sources []string
	matched := make(map[string]struct{}, len(currentCols))
	fields := m.Descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		name := string(field.Name())
		onlineName, _, ok, _ := m.lookupColumnMeta(currentCols, name)
		if !ok {
			for candidate, meta := range currentCols {
				if meta.fieldNum == field.Number() && (!ok || candidate < onlineName) {
					onlineName, ok = candidate, true
				}
			}
		}
		if !ok {
			continue
		}
		matched[onlineName] = struct{}{}
		source := escapeMySQLName(onlineName)
		if _, coalesce := nullableLegacy[name]; coalesce {
			source = "COALESCE(" + source + ", '')"
		}
		targets = append(targets, escapeMySQLName(name))
		sources = append(sources, source)
	}
	var orphans []string
	for name := range currentCols {
		if _, ok := matched[name]; !ok {
			orphans = append(orphans, name)
		}
	}
	sort.Strings(orphans)

	b.WriteString("\n迁移步骤（停写或维护窗口内执行）：主键列上的旧形态无法原地修改——TiDB 聚簇主键既不允许 " +
		"DROP PRIMARY KEY，也不允许给带索引的列改排序规则（Error 8200），因此按影子表重建，MySQL 与 TiDB 通用：")
	b.WriteString("\n  1) 预检：NULL 改成 '' 后会互相重复、也会与已有的 '' 撞键，须先人工改成唯一值或删除；" +
		"string 列的 CHAR_LENGTH、bytes 列的 LENGTH 不得超过目标长度，否则拷贝会失败")
	fmt.Fprintf(b, "\n  2) 按本库的建表语句建影子表 %s，把数据拷进去（NULL 改写成 ''）", shadow)
	fmt.Fprintf(b, "\n  3) RENAME 原子换表；核对数据无误后再手工 DROP TABLE %s（本库不会替你删）", backup)
	if len(orphans) > 0 {
		fmt.Fprintf(b, "\n  注意：线上还有 proto 未声明的列 %v，影子表不含它们；执行前把它们补进建表语句和 INSERT 列表，否则这些数据只留在旧表里", orphans)
	}

	createSQL := m.GetCreateTableSQL()
	prefix := "CREATE TABLE IF NOT EXISTS " + table + " ("
	if !strings.HasPrefix(createSQL, prefix) {
		return nil
	}
	body := strings.TrimSuffix(strings.TrimPrefix(createSQL, prefix), ";")
	body = strings.ReplaceAll(body, ",\n  ", ", ")
	body = strings.ReplaceAll(body, "\n  ", "")
	body = strings.ReplaceAll(body, "\n)", ")")
	return []string{
		"CREATE TABLE " + shadow + " (" + body,
		fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
			shadow, strings.Join(targets, ", "), strings.Join(sources, ", "), table),
		fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s", table, backup, shadow, table),
	}
}

// indexColumnsSQL 与建表、补索引同一种写法拼索引列（必要时带前缀长度）。
func (m *MessageTable) indexColumnsSQL(spec string) string {
	cols := strings.Split(spec, ",")
	quoted := make([]string, len(cols))
	for i, col := range cols {
		quoted[i] = m.indexColumn(strings.TrimSpace(col))
	}
	return strings.Join(quoted, ",")
}

func columnsReferenceAny(columns []string, names map[string]struct{}) bool {
	for _, column := range columns {
		if _, ok := names[column]; ok {
			return true
		}
	}
	return false
}
