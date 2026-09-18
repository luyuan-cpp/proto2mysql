package proto2mysql

// 主键/唯一键里的 string/bytes 列（下称键列）：写入前的值校验、表选项校验，以及线上旧形态的识别与迁移 SQL。
// 列类型映射见 getMySQLFieldType，长度与排序规则常量见 DefaultKeyColumnLength 一组。

import (
	"database/sql"
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
			// 声明多半来自 proto，而键是代码用 WithPrimaryKey/WithUniqueKey 改掉的：
			// WithMaxLength 按字段合并，清不掉 proto 的声明（写 0 也越界被拒），只能整体替换。
			return fmt.Errorf("%w: 表 %s 的字段 %q（%s）声明了 max_length，但 max_length 目前仅用于主键/唯一键的 string/bytes 字段；"+
				"若这条声明来自 proto 而代码改掉了键，用 WithMaxLengths 整体替换（传 nil 即清空）",
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
// 每列的线上形态与原因、迁移步骤、执行前要人工看过的核对查询，以及按本表实际名字填好、
// 在 MySQL 与 TiDB 上都能执行的 SQL。
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

	var checks, statements []string
	if inPrimaryKey {
		checks, statements = m.legacyKeyRebuildSQL(&b, legacy, currentCols, existingIndexes, indexesKnown)
	} else {
		checks, statements = m.legacyKeyAlterSQL(&b, legacy, existingIndexes, indexesKnown)
	}
	writeSQLBlock(&b, legacyKeyCheckSQLHeader, checks)
	writeSQLBlock(&b, legacyKeySQLHeader, statements)
	if len(otherDrifts) > 0 {
		fmt.Fprintf(&b, "\n另有与这些键列无关的漂移：%s", strings.Join(otherDrifts, "; "))
	}
	return fmt.Errorf("%w: %w: %s", ErrSchemaDrift, ErrLegacyKeyColumn, b.String())
}

// legacyKeySQLHeader 迁移 SQL 块的标题行；其后每条语句独占一行、以 4 个空格缩进、以分号结尾。
// 必须在**同一个会话**里按顺序执行：块里有 SET SESSION、用户变量与 PREPARE，换一条连接就丢了。
const legacyKeySQLHeader = "可直接执行的 SQL（在同一个会话里按顺序逐条执行）："

// legacyKeyCheckSQLHeader 执行迁移前要人工看过结果的只读核对查询，格式同上。
// 单独成块是因为它们的结果需要人判断（有没有超长值、有没有外键/触发器），不能混进照单执行的块里。
const legacyKeyCheckSQLHeader = "执行前的人工核对（只读查询，结果必须人工看过）："

// writeSQLBlock 输出一个 SQL 块：标题行 + 每条语句独占一行、4 空格缩进、分号结尾。
func writeSQLBlock(b *strings.Builder, header string, statements []string) {
	if len(statements) == 0 {
		return
	}
	b.WriteString("\n" + header)
	for _, stmt := range statements {
		b.WriteString("\n    ")
		b.WriteString(stmt)
		b.WriteString(";")
	}
}

// strictModeSQL 两条迁移路径的第一条语句：把当前会话切成严格模式。
// 非严格模式下 MODIFY COLUMN 与 INSERT ... SELECT 遇到装不下的值是**静默截断**后继续——
// 影子表路径会把截断后的数据 RENAME 成正式表，原地 ALTER 路径会把列里的值改短，两种都不可回滚。
// NULLIF 是为了 sql_mode 为空串时也成立（空串直接 CONCAT 会得到 ",STRICT_ALL_TABLES"）；
// 已含该模式时重复追加是幂等的。MySQL 26.7 与 TiDB v8.5 实测通过。
const strictModeSQL = "SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'STRICT_ALL_TABLES')"

// legacyValueCheckSQL 每个旧形态列一条：目标列装不装得下、有多少行是 NULL（NULL 要改写成空串，
// 会与已有的空串及彼此撞键）。string 按字符数、bytes 按字节数，与写入前的校验同口径。
func (m *MessageTable) legacyValueCheckSQL(legacy []legacyKeyColumn) []string {
	table := escapeMySQLName(m.tableName)
	checks := make([]string, 0, len(legacy))
	for _, col := range legacy {
		name := string(col.field.Name())
		online := escapeMySQLName(col.onlineName)
		lengthExpr, alias := "CHAR_LENGTH", "max_chars"
		if col.field.Kind() == protoreflect.BytesKind {
			lengthExpr, alias = "LENGTH", "max_bytes"
		}
		checks = append(checks, fmt.Sprintf(
			"SELECT MAX(%s(%s)) AS %s, SUM(%s IS NULL) AS null_rows, COUNT(*) AS rows_total FROM %s",
			lengthExpr, online, alias, online, table)+
			fmt.Sprintf(" /* 目标列 %s 上限 %d */", name, m.keyColumnLength(name)))
	}
	return checks
}

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
// 返回 (人工核对查询, 可直接执行的语句)。
func (m *MessageTable) legacyKeyAlterSQL(
	b *strings.Builder, legacy []legacyKeyColumn,
	existingIndexes map[string]indexMeta, indexesKnown bool,
) (checks, statements []string) {
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
	b.WriteString("\n  1) 预检（下面的核对查询给出实际数字）：NULL 改成 '' 后会互相重复、也会与已有的 '' 撞键，" +
		"须先人工改成唯一值或删除；string 列的 CHAR_LENGTH、bytes 列的 LENGTH 不得超过目标长度，" +
		"否则 MODIFY 会失败（SQL 块第一条已把会话切成严格模式，超长是报错而不是静默截断）")
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
					extra = append(extra, fmt.Sprintf("%s %s", name, formatIndexMeta(meta)))
					break
				}
			}
		}
		sort.Strings(extra)
		if len(extra) > 0 {
			fmt.Fprintf(b, "\n  注意：线上另有包含这些列、但不是本库声明的索引 %v；"+
				"TiDB 上须先 DROP INDEX 掉它们才能 MODIFY，迁移后按这里的定义人工重建（本库不会替你动它们）", extra)
		}
	} else {
		fmt.Fprintf(b, "\n  注意：本次没有读取线上二级索引，执行前请 SHOW INDEX FROM %s 核对："+
			"包含这些列的索引都要先删掉才能 MODIFY（TiDB 上是硬性要求），迁移后按需人工重建", escapeMySQLName(m.tableName))
	}

	table := escapeMySQLName(m.tableName)
	statements = []string{strictModeSQL}
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
	return m.legacyValueCheckSQL(legacy), statements
}

// shadowColumn 影子表里的一列：proto 字段、它对应的线上列，以及影子表要用的列定义。
type shadowColumn struct {
	onlineName string // 线上列名；线上没有这一列时为空
	meta       columnMeta
	exists     bool
	legacy     bool
	colType    string // 影子表里的列定义（含 NOT NULL/DEFAULT 等属性，不含 COMMENT）
}

// shadowColumns 把 proto 字段与线上列对上，并算出影子表里每列的定义。
// 匹配顺序与 buildColumnClauses 一致：先按列名（MySQL 大小写不敏感），再按 COMMENT 'pb:N' 的字段号，
// 后者按列名字典序取最小的候选，保证改过名的列在 INSERT ... SELECT 里引用的是线上那个名字。
func (m *MessageTable) shadowColumns(legacy []legacyKeyColumn, currentCols map[string]columnMeta) map[string]shadowColumn {
	legacyByName := make(map[string]struct{}, len(legacy))
	for _, col := range legacy {
		legacyByName[string(col.field.Name())] = struct{}{}
	}

	fields := m.Descriptor.Fields()
	columns := make(map[string]shadowColumn, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		name := string(field.Name())
		onlineName, meta, ok, _ := m.lookupColumnMeta(currentCols, name)
		if !ok {
			for candidate, candidateMeta := range currentCols {
				if candidateMeta.fieldNum == field.Number() && (!ok || candidate < onlineName) {
					onlineName, meta, ok = candidate, candidateMeta, true
				}
			}
		}
		_, isLegacy := legacyByName[name]
		columns[name] = shadowColumn{
			onlineName: onlineName,
			meta:       meta,
			exists:     ok,
			legacy:     isLegacy,
			colType:    m.shadowColumnType(field, meta, ok, isLegacy),
		}
	}
	return columns
}

// shadowColumnType 影子表里这一列的定义。
//
// 旧形态键列用目标类型——把它改成 VARCHAR/VARBINARY 整列索引正是本次迁移的目的。
// 其余列沿用**同步路径**的规则：线上装得下目标（isTypeMatch）时保留线上的类型本体与排序规则，
// 否则用目标类型拓宽。按 proto 类型一律重建会把同步刻意保留得更宽的列收窄（线上 bigint 而 proto
// int32、线上 LONGTEXT 而 proto MEDIUMTEXT、线上 VARCHAR(255) 键列而 max_length 调小），
// 严格模式下迁移中途失败，非严格模式下截断后的数据会被 RENAME 成正式表。
func (m *MessageTable) shadowColumnType(field protoreflect.FieldDescriptor, meta columnMeta, exists, legacy bool) string {
	target := m.getMySQLFieldType(field)
	if legacy || !exists || !isTypeMatch(meta.colType, target) {
		return target
	}
	aligned := alignedColumnType(meta.colType, target)
	// 排序规则同理：影子表默认排序规则与线上列不同时不显式写出来，比较语义（大小写、尾部空格）
	// 会被静默改掉，而同步路径根本不碰这一列。字符集由排序规则名隐含。
	if meta.collation == "" || strings.EqualFold(meta.collation, defaultTableCollation) ||
		strings.Contains(strings.ToUpper(aligned), "COLLATE") {
		return aligned
	}
	parts := strings.Fields(aligned)
	return strings.Join(append([]string{parts[0], "COLLATE", meta.collation}, parts[1:]...), " ")
}

// legacyKeyRebuildSQL 旧形态列在主键里时按影子表重建：TiDB 聚簇主键既不允许 DROP PRIMARY KEY，
// 也不允许给带索引的列改排序规则（均为 Error 8200，先建空表再 MODIFY 也一样），原地 ALTER 走不通；
// 建新表 → INSERT ... SELECT → RENAME 在 MySQL 与 TiDB 上都可执行。
// 影子表只换表名，索引名保留原表的 idx_<表名>_N / uk_<表名>，RENAME 回来之后同步才认得出这些索引。
// 返回 (人工核对查询, 可直接执行的语句)。
func (m *MessageTable) legacyKeyRebuildSQL(
	b *strings.Builder, legacy []legacyKeyColumn, currentCols map[string]columnMeta,
	existingIndexes map[string]indexMeta, indexesKnown bool,
) (checks, statements []string) {
	table := escapeMySQLName(m.tableName)
	shadowName := truncateIdentifier(m.tableName + "__p2m_new")
	shadow := escapeMySQLName(shadowName)
	backup := escapeMySQLName(truncateIdentifier(m.tableName + "__p2m_old"))

	columns := m.shadowColumns(legacy, currentCols)
	var targets, sources []string
	matched := make(map[string]struct{}, len(currentCols))
	fields := m.Descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		column := columns[name]
		if !column.exists {
			continue
		}
		matched[column.onlineName] = struct{}{}
		source := escapeMySQLName(column.onlineName)
		if column.legacy && column.meta.nullable {
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

	extraIndexes, keptNames, lostIndexes := m.shadowExtraIndexes(columns, existingIndexes, indexesKnown)
	preserved := m.preservedShadowColumns(columns)

	b.WriteString("\n迁移步骤（停写或维护窗口内执行）：主键列上的旧形态无法原地修改——TiDB 聚簇主键既不允许 " +
		"DROP PRIMARY KEY，也不允许给带索引的列改排序规则（Error 8200），因此按影子表重建，MySQL 与 TiDB 通用：")
	b.WriteString("\n  1) 预检（下面的核对查询给出实际数字）：NULL 改成 '' 后会互相重复、也会与已有的 '' 撞键" +
		"（线上未声明的唯一索引同样会撞），须先人工改成唯一值或删除；string 列的 CHAR_LENGTH、bytes 列的 LENGTH " +
		"不得超过目标长度，否则拷贝会失败（SQL 块第一条已把会话切成严格模式，超长是报错而不是静默截断）")
	fmt.Fprintf(b, "\n  2) 核对外键与触发器：RENAME 只搬表本身——引用本表的外键会跟着指向 %s，本表上的触发器也留在 %s 上；"+
		"CHECK 约束、FULLTEXT/SPATIAL 索引、分区同样不会进影子表。执行下面的核对查询，"+
		"并用 SHOW CREATE TABLE %s 逐项对照，RENAME 之后人工重建", backup, backup, table)
	fmt.Fprintf(b, "\n  3) 按下面的建表语句建影子表 %s，把数据拷进去（旧形态列的 NULL 改写成 ''）", shadow)
	if len(preserved) > 0 {
		fmt.Fprintf(b, "\n     影子表保留了线上比 proto 更宽（或排序规则不同）的列，不收窄：%v", preserved)
	}
	if len(keptNames) > 0 {
		fmt.Fprintf(b, "\n     影子表按线上定义带上了本库未声明的索引 %v（只还原唯一性、列顺序与前缀长度；"+
			"FULLTEXT/SPATIAL、降序、不可见等属性请按 SHOW CREATE TABLE 的输出人工核对）", keptNames)
	}
	if m.autoIncreaseKey != "" {
		fmt.Fprintf(b, "\n  4) 把影子表的自增计数器抬到旧表的水位（SQL 块里已包含）："+
			"影子表的计数器只跟着拷进去的最大 %s 走，删掉过尾部行的表会把那些 id 重新发一遍", escapeMySQLName(m.autoIncreaseKey))
	}
	fmt.Fprintf(b, "\n  %d) RENAME 原子换表；核对数据无误后再手工 DROP TABLE %s（本库不会替你删）",
		4+boolToInt(m.autoIncreaseKey != ""), backup)
	fmt.Fprintf(b, "\n  中途任何一步失败：先 DROP TABLE %s 再从头执行（影子表还没接客，丢弃是安全的）", shadow)
	if len(orphans) > 0 {
		fmt.Fprintf(b, "\n  注意：线上还有 proto 未声明的列 %v，影子表不含它们；执行前把它们补进建表语句和 INSERT 列表，否则这些数据只留在旧表里", orphans)
	}
	if len(lostIndexes) > 0 {
		fmt.Fprintf(b, "\n  注意：线上这些本库未声明的索引无法自动重建，请人工处理：%s", strings.Join(lostIndexes, "；"))
	}
	if !indexesKnown {
		fmt.Fprintf(b, "\n  注意：本次没有读取线上二级索引，影子表里只有本库声明的那些；"+
			"执行前必须 SHOW INDEX FROM %s 核对，把本库未声明的索引手工补进建表语句，否则 RENAME 之后它们就没了", table)
	}

	createSQL := m.buildCreateTableSQL(createTableSpec{
		tableName: shadowName,
		inline:    true,
		columnType: func(field protoreflect.FieldDescriptor) string {
			return columns[string(field.Name())].colType
		},
		extraIndexes: extraIndexes,
	})
	statements = []string{
		strictModeSQL,
		createSQL,
		fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
			shadow, strings.Join(targets, ", "), strings.Join(sources, ", "), table),
	}
	statements = append(statements, m.autoIncrementRebaseSQL(shadow)...)
	statements = append(statements, fmt.Sprintf("RENAME TABLE %s TO %s, %s TO %s", table, backup, shadow, table))

	checks = m.legacyValueCheckSQL(legacy)
	checks = append(checks,
		fmt.Sprintf("SELECT CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_NAME, REFERENCED_TABLE_NAME "+
			"FROM information_schema.REFERENTIAL_CONSTRAINTS "+
			"WHERE (UNIQUE_CONSTRAINT_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME = %[1]s) "+
			"OR (CONSTRAINT_SCHEMA = DATABASE() AND TABLE_NAME = %[1]s)", quoteSQLLiteral(m.tableName)),
		fmt.Sprintf("SELECT TRIGGER_NAME, ACTION_TIMING, EVENT_MANIPULATION FROM information_schema.TRIGGERS "+
			"WHERE TRIGGER_SCHEMA = DATABASE() AND EVENT_OBJECT_TABLE = %s", quoteSQLLiteral(m.tableName)),
	)
	return checks, statements
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// quoteSQLLiteral 把标识符拼进 SQL 字符串字面量（information_schema 过滤条件、CONCAT 的前缀）。
// 转义口径与 escapeMySQLComment 相同：双写反斜杠 + 倍写单引号。
// 倍写单引号在任何 SQL mode 下都成立；双写反斜杠只在默认 mode 下还原成一个反斜杠，
// NO_BACKSLASH_ESCAPES 下会变成两个——只影响表名里真的带反斜杠的表，
// 后果是这条核对查询匹配不到行，不会拼出别的语义。
func quoteSQLLiteral(value string) string {
	return "'" + escapeMySQLComment(value) + "'"
}

// preservedShadowColumns 影子表里没按 proto 目标类型重建、而是沿用线上定义的列，供迁移步骤点名。
func (m *MessageTable) preservedShadowColumns(columns map[string]shadowColumn) []string {
	var preserved []string
	fields := m.Descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		column := columns[string(field.Name())]
		if !column.exists || column.legacy || column.colType == m.getMySQLFieldType(field) {
			continue
		}
		preserved = append(preserved, fmt.Sprintf("%s %s", escapeMySQLName(string(field.Name())), column.colType))
	}
	return preserved
}

// autoIncrementRebaseSQL 把影子表的自增计数器抬到不低于旧表的计数器，放在 INSERT 之后、RENAME 之前。
//
// 不抬的话，影子表的计数器只由拷进去的最大值决定：旧表 AUTO_INCREMENT=100001 而 MAX(id)=95000 时
// （删过尾部行就会这样），迁移后 95001..100000 会被**重新发一遍**，撞上按旧 id 存下的外部引用。
//
// 计数器值只能在运行时读，而 DDL 不接受表达式，所以只能 PREPARE 一条拼出来的 ALTER：
// MySQL 26.7 与 TiDB v8.5 实测都支持 PREPARE 执行 DDL。
// information_schema.TABLES.AUTO_INCREMENT 在 MySQL 8 受 information_schema_stats_expiry
// 缓存影响（默认 24 小时），先把它设成 0 才读得到当前值；MySQL 5.7 没有这个变量，用版本注释跳过。
// 旧表那一列若根本没有 AUTO_INCREMENT（会另行报 auto_increment mismatch 漂移），读到 NULL，
// COALESCE 成 1，MySQL 与 TiDB 都会把它夹到 MAX(id)+1，不会反而把计数器调低。
func (m *MessageTable) autoIncrementRebaseSQL(shadow string) []string {
	if m.autoIncreaseKey == "" {
		return nil
	}
	return []string{
		"/*!80000 SET SESSION information_schema_stats_expiry = 0 */",
		fmt.Sprintf("SET @p2m_auto_increment = COALESCE((SELECT AUTO_INCREMENT FROM information_schema.TABLES "+
			"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s), 1)", quoteSQLLiteral(m.tableName)),
		fmt.Sprintf("SET @p2m_rebase_sql = CONCAT(%s, @p2m_auto_increment)",
			quoteSQLLiteral("ALTER TABLE "+shadow+" AUTO_INCREMENT = ")),
		"PREPARE p2m_rebase_auto_increment FROM @p2m_rebase_sql",
		"EXECUTE p2m_rebase_auto_increment",
		"DEALLOCATE PREPARE p2m_rebase_auto_increment",
	}
}

// shadowExtraIndexes 线上存在、本库未声明的二级索引：能映射到影子表列的按线上定义重建（保留原名、
// 唯一性与列顺序），其余逐个点名。
//
// 不带上它们的话，RENAME 之后这些索引就没了——DBA 按查询模式加的索引、别的工具建的唯一约束
// 都会静默消失，而本库对线上多出来的索引一贯是"只加不删"。
// 旧形态键列上的前缀长度必须去掉：这些列在影子表里是 VARCHAR/VARBINARY 整列索引，
// 前缀长度大于等于列宽时建表直接报 Error 1089。其余 TEXT/BLOB 列保留线上的 SUB_PART。
func (m *MessageTable) shadowExtraIndexes(
	columns map[string]shadowColumn, existingIndexes map[string]indexMeta, indexesKnown bool,
) (clauses []string, kept []string, lost []string) {
	if !indexesKnown {
		return nil, nil, nil
	}

	declared := make(map[string]struct{}, len(m.indexes)+1)
	if m.uniqueKeys != "" {
		declared[strings.ToLower(m.uniqueKeyName())] = struct{}{}
	}
	for i := range m.indexes {
		declared[strings.ToLower(m.indexNameFor(i))] = struct{}{}
	}
	// 线上列名 → proto 字段名，改过名的列也能映射回影子表里的新名字。
	fieldByOnlineName := make(map[string]string, len(columns))
	for name, column := range columns {
		if column.exists {
			fieldByOnlineName[strings.ToLower(column.onlineName)] = name
		}
	}

	names := make([]string, 0, len(existingIndexes))
	for name := range existingIndexes {
		if _, ok := declared[strings.ToLower(name)]; !ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	for _, name := range names {
		meta := existingIndexes[name]
		parts := append([]indexColumnMeta(nil), meta.columns...)
		sort.Slice(parts, func(i, j int) bool { return parts[i].sequence < parts[j].sequence })

		var rendered []string
		bytes, budgetKnown := 0, true
		var reason string
		for _, part := range parts {
			fieldName, ok := fieldByOnlineName[strings.ToLower(part.name)]
			if !ok {
				reason = fmt.Sprintf("引用了影子表没有的列 %q", part.name)
				break
			}
			column := columns[fieldName]
			subPart := part.subPart
			if column.legacy {
				subPart = sql.NullInt64{} // 整列索引，前缀长度在 VARCHAR/VARBINARY 上不再成立
			}
			escaped := escapeMySQLName(fieldName)
			if subPart.Valid {
				escaped = fmt.Sprintf("%s(%d)", escaped, subPart.Int64)
			}
			rendered = append(rendered, escaped)
			if n, ok := shadowIndexPartBytes(column.colType, subPart); ok {
				bytes += n
			} else {
				budgetKnown = false
			}
		}
		if reason == "" && budgetKnown && bytes > MaxIndexKeyBytes {
			reason = fmt.Sprintf("去掉旧形态键列的前缀长度后键长 %d 字节，超过单个索引 %d 字节的上限（建表会报 Error 1071）",
				bytes, MaxIndexKeyBytes)
		}
		if reason != "" {
			lost = append(lost, fmt.Sprintf("%s %s：%s", name, formatIndexMeta(meta), reason))
			continue
		}
		keyword := "INDEX"
		if meta.unique {
			keyword = "UNIQUE KEY"
		}
		clauses = append(clauses, fmt.Sprintf("%s %s (%s)", keyword, escapeMySQLName(name), strings.Join(rendered, ",")))
		kept = append(kept, name)
	}
	return clauses, kept, lost
}

// shadowIndexPartBytes 影子表里这一列在索引键里占的字节数；类型无法判断时返回 false（不做预算判断）。
// 口径同 indexKeyPartBytes：影子表字符集固定 utf8mb4，字符列每字符按 4 字节，二进制列每字节按 1。
func shadowIndexPartBytes(colType string, subPart sql.NullInt64) (int, bool) {
	info := parseMySQLType(colType)
	length := info.length
	if subPart.Valid {
		length = int(subPart.Int64)
	}
	switch normalizeBaseType(info.baseType) {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		if length <= 0 {
			return 0, false
		}
		return 4 * length, true
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		if length <= 0 {
			return 0, false
		}
		return length, true
	case "tinyint":
		return 1, true
	case "smallint":
		return 2, true
	case "mediumint":
		return 3, true
	case "int", "float":
		return 4, true
	case "bigint", "double", "datetime":
		return 8, true
	}
	return 0, false
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
