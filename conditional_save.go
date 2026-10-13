package proto2mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gorm.io/gorm"
)

// ── SaveConditional：带栅栏的整行落库 ─────────────────────────────────────
//
// 解决的问题：多个进程可能先后把同一行的不同版本写进来（异步存盘队列被重放、旧属主
// 进程假死后恢复），普通 Save 是「后到者覆盖」，旧版本会把新数据整行覆盖回去，且没有任何报错。
// SaveConditional 让行自己携带一组单调递增的版本列（栅栏），只有库内栅栏按字典序**更旧**时
// 才写入；判断与写入是同一条 UPDATE，不依赖任何外部锁的租约。
//
// 语义与 Save 同构：按完整主键「有则更新、无则插入」，只写本进程认识的列，备用唯一键
// 撞到别的行时返回 ErrDuplicateKey 且不修改那一行。差别是多了栅栏条件，并把结局报给调用方。

var (
	// ErrInvalidSaveOptions SaveOptions 不合法：栅栏列 / 保留列不存在、类型或归属不对，
	// 或严格模式下消息的栅栏全为 0（条件恒假，写入会被静默丢弃）。总是在发出任何 SQL 之前返回。
	ErrInvalidSaveOptions = errors.New("invalid save options")

	// ErrConditionalSaveRace 两轮内条件都成立，但条件 UPDATE 都没有改到这一行，且又不满足
	// ErrSaveMatchMismatch 的条件（两轮状态不同，或栅栏严格更旧、根本不需要整行比较）。
	// 最常见的原因是期间有并发写者在改这一行，单独重试这次调用即可；
	// 返回结果里的 Stored 是最后一次读到的库内栅栏值。它不包装 ErrDuplicateKey。
	ErrConditionalSaveRace = errors.New("conditional save kept racing with concurrent writers")

	// ErrSaveMatchMismatch 两轮都做了整行比较、读到完全相同的库内状态：条件成立、行不是目标值，
	// 但条件 UPDATE 两次都没有改到它。最可能的原因是本库的逐列比较与 MySQL 的「值是否变化」
	// 判定不一致（例如线上列是 DATETIME(0)，而写入值带小数秒），需要核对表结构，重试不会好转。
	// 只在栅栏相等（AllowEqual）或不带栅栏时可能出现；这两种情况下也无法完全排除「同一栅栏值上
	// 恰好连续两次并发改写」，偶发一次可以重试，反复出现就是表结构问题。
	ErrSaveMatchMismatch = errors.New("conditional save cannot reconcile the current row with the target values")
)

// conditionalSaveMaxRounds 条件 UPDATE 最多发几轮。第二轮只用来吸收「读状态与 UPDATE 之间
// 恰好有人改了这一行」的窗口；再多一轮并不能让结论更可靠，只会在热点行上自旋。
const conditionalSaveMaxRounds = 2

// SaveOptions 是 SaveConditional 的写入条件。零值的写入语义等同 Save（UPDATE 语句相同），
// 区别是 UPDATE 影响 0 行时多一条存在性查询、错误集合多了本文件的三个哨兵，并返回结局。
type SaveOptions struct {
	// FenceFields 栅栏列，按字典序的优先级排列，例如 {"owner_epoch", "save_seq"}。
	// 只允许 uint32 / uint64 标量列；不得是主键、repeated、map、nullable 或自增列，不得重复。
	// 写入条件：库内这些列组成的元组 < 消息里的同一组值（AllowEqual 时为 <=）。
	// 栅栏列本身随整行一起写成消息里的值，所以库内栅栏只会前进、不会倒退。
	FenceFields []string

	// AllowEqual 为 true 时，库内栅栏与消息相等也写入（条件变为 <=）。
	// 用于「同一版本可以被重复写」的旧写入方；严格模式下同版本重放的结局是 SaveRejected，
	// 且 Stored 等于消息里的栅栏值，调用方据此判定为重复。
	AllowEqual bool

	// PreserveFields 更新已有行时不写这些列，库内原值保持不动；插入新行时照常写消息里的值。
	// 典型用法是不认识栅栏的旧写入方：它的消息里栅栏为 0，整行写回会把栅栏清零。
	// 不得与主键、FenceFields 重叠，也不能覆盖全部非主键列。
	PreserveFields []string
}

// SaveOutcome 是 SaveConditional 的结局。零值 SaveOutcomeUnknown 只会和错误一起出现。
type SaveOutcome uint8

const (
	SaveOutcomeUnknown SaveOutcome = iota
	// SaveUpdated 已有行被条件 UPDATE 改写。
	SaveUpdated
	// SaveInserted 行不存在，已插入。
	SaveInserted
	// SaveUnchanged 条件成立，但库内这一行已经是目标值（只在 AllowEqual 或不带栅栏时出现）。
	SaveUnchanged
	// SaveRejected 栅栏条件不成立，什么都没写。Stored 是库内的栅栏值。
	SaveRejected
)

func (o SaveOutcome) String() string {
	switch o {
	case SaveUpdated:
		return "updated"
	case SaveInserted:
		return "inserted"
	case SaveUnchanged:
		return "unchanged"
	case SaveRejected:
		return "rejected"
	default:
		return "unknown"
	}
}

// ConditionalSaveResult 是 SaveConditional 的返回值。
type ConditionalSaveResult struct {
	Outcome SaveOutcome
	// Stored 库内的栅栏值，顺序与 FenceFields 一致。只在执行过状态查询时非 nil：
	// SaveRejected 时必有；SaveUnchanged、以及 ErrConditionalSaveRace / ErrSaveMatchMismatch 时
	// 是最后一次读到的值；SaveUpdated / SaveInserted 时为 nil。不带栅栏时恒为 nil。
	Stored []uint64
}

// Applied 库内这一行现在是否就是本次的目标值（已更新、已插入或本来就是）。
func (r ConditionalSaveResult) Applied() bool {
	switch r.Outcome {
	case SaveUpdated, SaveInserted, SaveUnchanged:
		return true
	default:
		return false
	}
}

// SaveConditional 带栅栏条件的整行落库，见 SaveOptions。
//
// 常见路径的语句数：SaveUpdated 1 条；SaveRejected 2 条；SaveInserted 3 条；SaveUnchanged 3 条。
// 并发插入撞 1062 时多一次重读，撞上并发写者时最多再来一轮。
//
// 状态查询与整行比较一律带 FOR UPDATE：强制 current read（读最新已提交值并等待在途写者），
// 也让读写分离中间件 / gorm dbresolver 把它留在主库——发到滞后的从库会把旧属主的重放误判成
// 「已是目标值」，正好在栅栏要防的场景里失效。autocommit 下锁只持续到这条语句结束。
// 超时与取消通过 WithContext 传入，作用到每一条语句。
//
// 返回错误时 Outcome 为 SaveOutcomeUnknown。语句已经发出之后的错误（例如超时）意味着
// 结局未知：那条 UPDATE / INSERT 可能已在服务端生效。
//
// 显式事务（默认 REPEATABLE READ）里两个写者并发首插同一段主键间隙时，InnoDB 可能返回
// 1213 并回滚整个事务（Save 也一样），须整体重试事务；它不会被归为 ErrConditionalSaveRace。
// 首写尽量在 autocommit 下做。
//
// ⚠️ 启用栅栏的表只应经 SaveConditional 写入：普通 Save / Update 会把栅栏写成任意值；
// 物理 DELETE 之后行不存在，任何旧版本都会被当成新行插入。
func (p *DB) SaveConditional(message proto.Message, opts SaveOptions) (ConditionalSaveResult, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return ConditionalSaveResult{}, err
	}
	plan, err := table.newConditionalSavePlan(message, opts)
	if err != nil {
		return ConditionalSaveResult{}, err
	}
	result, err := runConditionalSave(coreConditionalSaveExecutor{db: p}, plan)
	// 只有确定什么都没写（被拒）时才保留缓存。出错可能是「语句已生效、结局未知」，
	// 删缓存只多一次未命中，永远安全；事务内推迟到提交后，回滚则不删。
	if result.Outcome != SaveRejected {
		p.invalidateMessages(table, message)
	}
	return result, err
}

// SaveConditional 语义与 DB.SaveConditional 完全一致，共用同一份判定流程。
// 超时与取消请用 gorm 的 WithContext 传入。
func (p *GormDB) SaveConditional(message proto.Message, opts SaveOptions) (ConditionalSaveResult, error) {
	table, err := p.tableForMessage(message)
	if err != nil {
		return ConditionalSaveResult{}, err
	}
	plan, err := table.newConditionalSavePlan(message, opts)
	if err != nil {
		return ConditionalSaveResult{}, err
	}
	return runConditionalSave(gormConditionalSaveExecutor{db: p.DB}, plan)
}

// conditionalSavePlan 一次 SaveConditional 要用到的全部列与参数。
//
// 列和值成对保存（setFields[i] ↔ setArgs[i]），各条语句都从这里生成，
// 不从某条已生成语句的参数列表里按位置反推——带保留列和栅栏参数之后，位置推算一错就会
// 把值比到别的列上，静默误判成「已是目标值」。
type conditionalSavePlan struct {
	table       *MessageTable
	fenceFields []protoreflect.FieldDescriptor
	fenceValues []uint64
	allowEqual  bool
	setFields   []protoreflect.FieldDescriptor
	setArgs     []interface{}
	pkWhere     string
	pkArgs      []interface{}
	insert      *SqlWithArgs
}

func (m *MessageTable) newConditionalSavePlan(message proto.Message, opts SaveOptions) (*conditionalSavePlan, error) {
	if err := m.validateMessageDescriptor(message); err != nil {
		return nil, err
	}
	if len(m.primaryKey) == 0 {
		return nil, ErrPrimaryKeyNotFound
	}
	pkSet := make(map[string]bool, len(m.primaryKey))
	for _, raw := range m.primaryKey {
		pkSet[strings.TrimSpace(raw)] = true
	}

	plan := &conditionalSavePlan{table: m, allowEqual: opts.AllowEqual}
	reflection := message.ProtoReflect()
	fenceSet := make(map[string]bool, len(opts.FenceFields))
	for _, name := range opts.FenceFields {
		field, err := m.conditionalSaveFenceField(name, pkSet, fenceSet)
		if err != nil {
			return nil, err
		}
		fenceSet[name] = true
		plan.fenceFields = append(plan.fenceFields, field)
		plan.fenceValues = append(plan.fenceValues, reflection.Get(field).Uint())
	}
	if len(plan.fenceValues) > 0 && !opts.AllowEqual && !slices.ContainsFunc(plan.fenceValues, func(v uint64) bool { return v != 0 }) {
		return nil, fmt.Errorf("%w: 表 %s 的栅栏 %v 在消息里全为 0，严格模式下「库内 < 0」恒不成立，写入会被静默丢弃；"+
			"不认识栅栏的写入方请用 AllowEqual 或 PreserveFields", ErrInvalidSaveOptions, m.tableName, opts.FenceFields)
	}

	preserveSet := make(map[string]bool, len(opts.PreserveFields))
	for _, name := range opts.PreserveFields {
		if _, ok := m.fieldNameToDesc[name]; !ok {
			return nil, fmt.Errorf("%w: 保留列 %s 不在表 %s 里", ErrInvalidSaveOptions, name, m.tableName)
		}
		switch {
		case pkSet[name]:
			return nil, fmt.Errorf("%w: 保留列 %s 是表 %s 的主键", ErrInvalidSaveOptions, name, m.tableName)
		case fenceSet[name]:
			return nil, fmt.Errorf("%w: 列 %s 不能同时是栅栏列和保留列", ErrInvalidSaveOptions, name)
		case preserveSet[name]:
			return nil, fmt.Errorf("%w: 保留列 %s 重复", ErrInvalidSaveOptions, name)
		}
		preserveSet[name] = true
	}

	for i := 0; i < m.Descriptor.Fields().Len(); i++ {
		field := m.Descriptor.Fields().Get(i)
		name := string(field.Name())
		if pkSet[name] || preserveSet[name] {
			continue
		}
		value, err := m.serializeColumnValue(message, field)
		if err != nil {
			return nil, fmt.Errorf("serialize save field %s: %w", name, err)
		}
		plan.setFields = append(plan.setFields, field)
		plan.setArgs = append(plan.setArgs, value)
	}
	if len(plan.setFields) == 0 && len(preserveSet) > 0 {
		return nil, fmt.Errorf("%w: 保留列覆盖了表 %s 的全部非主键列，没有可写的列", ErrInvalidSaveOptions, m.tableName)
	}

	where, whereArgs, err := m.primaryKeyWhere(message)
	if err != nil {
		return nil, err
	}
	plan.pkWhere, plan.pkArgs = where, whereArgs

	insert, err := m.GetInsertSQLWithArgs(message)
	if err != nil {
		return nil, fmt.Errorf("generate conditional save insert for table %s: %w", m.tableName, err)
	}
	plan.insert = insert
	return plan, nil
}

// conditionalSaveFenceField 校验一个栅栏列。只认 uint32 / uint64：本库不支持 fixed*，
// 有符号列会让「更新」的方向依赖调用方约定；nullable 列上 NULL < x 是 UNKNOWN，条件永远不成立。
func (m *MessageTable) conditionalSaveFenceField(name string, pkSet, seen map[string]bool) (protoreflect.FieldDescriptor, error) {
	field, ok := m.fieldNameToDesc[name]
	if !ok {
		return nil, fmt.Errorf("%w: 栅栏列 %s 不在表 %s 里", ErrInvalidSaveOptions, name, m.tableName)
	}
	switch {
	case seen[name]:
		return nil, fmt.Errorf("%w: 栅栏列 %s 重复", ErrInvalidSaveOptions, name)
	case pkSet[name]:
		return nil, fmt.Errorf("%w: 栅栏列 %s 是表 %s 的主键", ErrInvalidSaveOptions, name, m.tableName)
	case field.IsList() || field.IsMap():
		return nil, fmt.Errorf("%w: 栅栏列 %s 是 repeated/map", ErrInvalidSaveOptions, name)
	case field.Kind() != protoreflect.Uint32Kind && field.Kind() != protoreflect.Uint64Kind:
		return nil, fmt.Errorf("%w: 栅栏列 %s 是 %s，只支持 uint32 / uint64", ErrInvalidSaveOptions, name, field.Kind())
	case m.isNullableField(name):
		return nil, fmt.Errorf("%w: 栅栏列 %s 声明为 nullable", ErrInvalidSaveOptions, name)
	case m.isAutoIncrementField(name):
		return nil, fmt.Errorf("%w: 栅栏列 %s 是自增列", ErrInvalidSaveOptions, name)
	}
	return field, nil
}

func (p *conditionalSavePlan) fenced() bool { return len(p.fenceFields) > 0 }

// conditionHolds 库内栅栏 stored 是否满足写入条件：字典序 stored < 消息（AllowEqual 时 <=）。
func (p *conditionalSavePlan) conditionHolds(stored []uint64) bool {
	c := slices.Compare(stored, p.fenceValues)
	return c < 0 || (c == 0 && p.allowEqual)
}

// updateStatement 条件 UPDATE：SET 只含 setFields，WHERE 是完整主键加栅栏谓词。
func (p *conditionalSavePlan) updateStatement() *SqlWithArgs {
	clauses := make([]string, 0, len(p.setFields))
	for _, field := range p.setFields {
		clauses = append(clauses, escapeMySQLName(string(field.Name()))+" = ?")
	}
	if len(clauses) == 0 {
		// 整张表只有主键列：与 Save 一样用合法 no-op 占位，影响 0 行后靠状态查询判定。
		pk := escapeMySQLName(strings.TrimSpace(p.table.primaryKey[0]))
		clauses = append(clauses, pk+" = "+pk)
	}
	args := make([]interface{}, 0, len(p.setArgs)+len(p.pkArgs)+2*len(p.fenceFields))
	args = append(args, p.setArgs...)
	args = append(args, p.pkArgs...)
	where := p.pkWhere
	if p.fenced() {
		predicate, predicateArgs := fencePredicate(p.fenceFields, p.fenceValues, p.allowEqual)
		where += " AND " + predicate
		args = append(args, predicateArgs...)
	}
	return &SqlWithArgs{
		Sql:  fmt.Sprintf("UPDATE %s SET %s WHERE %s", escapeMySQLName(p.table.tableName), strings.Join(clauses, ", "), where),
		Args: args,
	}
}

// fencePredicate 生成「库内栅栏元组按字典序 < 目标元组」的谓词（allowEqual 时最后一位用 <=）：
//
//	(`a` < ? OR (`a` = ? AND (`b` < ?)))
//
// 展开成只含 < / = 的 OR/AND，不用行值比较 (a, b) < (?, ?)：展开写法在 MySQL 5.7/8.x 与 TiDB 上
// 语义一目了然，且每个参数都按 Go uint64 绑定——若以十进制字符串下发，MySQL 会把整数列与字符串
// 按 DOUBLE 比较，超过 2^53 的相邻版本号会被判成相等，栅栏就放行了旧写。
func fencePredicate(fields []protoreflect.FieldDescriptor, values []uint64, allowEqual bool) (string, []interface{}) {
	last := len(fields) - 1
	op := "<"
	if allowEqual {
		op = "<="
	}
	predicate := escapeMySQLName(string(fields[last].Name())) + " " + op + " ?"
	args := []interface{}{values[last]}
	for i := last - 1; i >= 0; i-- {
		name := escapeMySQLName(string(fields[i].Name()))
		predicate = name + " < ? OR (" + name + " = ? AND (" + predicate + "))"
		args = append([]interface{}{values[i], values[i]}, args...)
	}
	return "(" + predicate + ")", args
}

// stateStatement 按主键读库内状态：带栅栏时只取栅栏列（不回传大字段），不带时只探存在性。
// 一律 FOR UPDATE，理由见 DB.SaveConditional 的注释。
func (p *conditionalSavePlan) stateStatement() *SqlWithArgs {
	columns := "1"
	if p.fenced() {
		names := make([]string, 0, len(p.fenceFields))
		for _, field := range p.fenceFields {
			names = append(names, escapeMySQLName(string(field.Name())))
		}
		columns = strings.Join(names, ", ")
	}
	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s FOR UPDATE", columns, escapeMySQLName(p.table.tableName), p.pkWhere)
	return &SqlWithArgs{Sql: query, Args: append([]interface{}(nil), p.pkArgs...)}
}

// matchStatement 判定库内这一行是否已经是目标值：逐个 setFields 列与本次要写的值做 NULL-safe 比较。
// 比较规则与 Save 的重复键归类查询一致（字符串/BLOB 按字节比、数值参数还原成带类型的值）。
// 一律 FOR UPDATE，理由同 stateStatement。
func (p *conditionalSavePlan) matchStatement() (*SqlWithArgs, error) {
	parts := make([]string, 0, len(p.setFields)+1)
	parts = append(parts, p.pkWhere)
	args := make([]interface{}, 0, len(p.pkArgs)+len(p.setArgs))
	args = append(args, p.pkArgs...)
	for i, field := range p.setFields {
		escaped := escapeMySQLName(string(field.Name()))
		if saveValueNeedsBinaryComparison(field) {
			parts = append(parts, "CAST("+escaped+" AS BINARY) <=> CAST(? AS BINARY)")
		} else {
			parts = append(parts, escaped+" <=> ?")
		}
		arg, err := saveCurrentRowMatchArg(field, p.setArgs[i])
		if err != nil {
			return nil, fmt.Errorf("normalize comparison field %s: %w", field.Name(), err)
		}
		args = append(args, arg)
	}
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE %s FOR UPDATE", escapeMySQLName(p.table.tableName), strings.Join(parts, " AND "))
	return &SqlWithArgs{Sql: query, Args: args}, nil
}

// conditionalSaveExecutor 把判定流程与具体后端（database/sql 或 gorm）隔开。
// exec 返回的错误必须已把 1062 包装成可 errors.Is(err, ErrDuplicateKey) 判断的形式。
type conditionalSaveExecutor interface {
	exec(query string, args []interface{}) (int64, error)
	query(query string, args []interface{}) (*sql.Rows, error)
}

type coreConditionalSaveExecutor struct{ db *DB }

func (x coreConditionalSaveExecutor) exec(query string, args []interface{}) (int64, error) {
	result, err := x.db.conn().Exec(query, args...)
	if err != nil {
		return 0, wrapExecErr(err)
	}
	return result.RowsAffected()
}

func (x coreConditionalSaveExecutor) query(query string, args []interface{}) (*sql.Rows, error) {
	return x.db.conn().Query(query, args...)
}

// gormConditionalSaveExecutor 参数原样交给 gorm。注意 gorm 会把紧跟在 "(" 后面的切片 / 数组参数
// 展开成多个占位符：serializeColumnValue 对 bytes / message / map / list 返回 string 而不是 []byte，
// 所以 `VALUES (?` 与 `CAST(? AS BINARY)` 不会被展开；序列化层若改成返回 []byte，这里要一起改。
type gormConditionalSaveExecutor struct{ db *gorm.DB }

func (x gormConditionalSaveExecutor) exec(query string, args []interface{}) (int64, error) {
	result := x.db.Exec(query, args...)
	if result.Error != nil {
		return 0, wrapGormExecErr(result.Error)
	}
	return result.RowsAffected, nil
}

func (x gormConditionalSaveExecutor) query(query string, args []interface{}) (*sql.Rows, error) {
	return x.db.Raw(query, args...).Rows()
}

// conditionalRowState 一次状态查询读到的库内状态。
type conditionalRowState struct {
	exists bool
	stored []uint64
}

func (s conditionalRowState) equal(other conditionalRowState) bool {
	return s.exists == other.exists && slices.Equal(s.stored, other.stored)
}

// runConditionalSave 是两个后端共用的判定流程：
//
//	UPDATE（带条件）→ 改到行：SaveUpdated
//	  └ 影响 0 行 → 读状态
//	      ├ 行不存在
//	      │   ├ 本次调用已撞过 1062（上一轮重读时行还在）→ 行被删了：包装原 1062 的错误，不再 INSERT
//	      │   └ INSERT → 成功：SaveInserted
//	      │            └ 1062 → 再读状态：仍不存在 = 撞到备用唯一键：ErrDuplicateKey；存在 → 按下列状态继续
//	      ├ 条件不成立 → SaveRejected{Stored}
//	      ├ 条件成立、栅栏严格更旧 → 行不可能已是目标值，不做整行比较，直接下一轮
//	      └ 条件成立、栅栏相等（AllowEqual）或不带栅栏 → 整行比较
//	          ├ 已是目标值 → SaveUnchanged
//	          └ 不是 → 下一轮；两轮都比较过且状态相同 → ErrSaveMatchMismatch
//	两轮用完仍没有结论 → ErrConditionalSaveRace
//
// 先读状态再 INSERT（而不是 Save 那样先 INSERT）：拒绝路径只多一条只取栅栏列的小查询；
// 若先 INSERT，RC 级别显式事务里两个被拒的写会各持 1062 留下的 S 锁、再互相请求 X 锁而死锁。
// 显式事务里的并发首插仍可能 1213，见 DB.SaveConditional 的注释。
func runConditionalSave(x conditionalSaveExecutor, plan *conditionalSavePlan) (ConditionalSaveResult, error) {
	tableName := plan.table.tableName
	update := plan.updateStatement()
	// duplicateErr 本次调用 INSERT 撞到的 1062，nil 表示还没插过。core 后端里它带着原始 *mysql.MySQLError；
	// gorm 开了 TranslateError 时原始错误已被翻译成 gorm.ErrDuplicatedKey，只保证 errors.Is(ErrDuplicateKey)。
	var duplicateErr error
	// compared 上一轮做过整行比较且不匹配时读到的状态；两轮都比较过且状态相同才判 ErrSaveMatchMismatch。
	var compared *conditionalRowState
	var lastStored []uint64

	for round := 1; round <= conditionalSaveMaxRounds; round++ {
		affected, err := x.exec(update.Sql, update.Args)
		if err != nil {
			return ConditionalSaveResult{}, fmt.Errorf("conditional save update on table %s: %w", tableName, err)
		}
		if affected > 0 {
			return ConditionalSaveResult{Outcome: SaveUpdated}, nil
		}

		state, err := readConditionalRowState(x, plan)
		if err != nil {
			return ConditionalSaveResult{}, err
		}
		if !state.exists {
			if duplicateErr != nil {
				// 上一轮插入撞 1062 后按主键读到过这一行，这一轮却没了：只能是被物理删除了。
				// 栅栏表不应做 DELETE，这里不再插第二次，交给调用方处理。
				return ConditionalSaveResult{}, fmt.Errorf("conditional save on table %s: row disappeared after a duplicate-key retry "+
					"(fenced tables must not be deleted): %w", tableName, duplicateErr)
			}
			_, err := x.exec(plan.insert.Sql, plan.insert.Args)
			if err == nil {
				return ConditionalSaveResult{Outcome: SaveInserted}, nil
			}
			if !errors.Is(err, ErrDuplicateKey) {
				return ConditionalSaveResult{}, fmt.Errorf("conditional save insert on table %s: %w", tableName, err)
			}
			// 1062：可能是并发写者刚插入了同一主键，也可能撞的是备用唯一键。按主键再读一次就能分清。
			duplicateErr = err
			state, err = readConditionalRowState(x, plan)
			if err != nil {
				return ConditionalSaveResult{}, err
			}
			if !state.exists {
				return ConditionalSaveResult{}, fmt.Errorf("conditional save on table %s conflicts with a different unique row: %w",
					tableName, duplicateErr)
			}
		}

		lastStored = state.stored
		if plan.fenced() && !plan.conditionHolds(state.stored) {
			return ConditionalSaveResult{Outcome: SaveRejected, Stored: state.stored}, nil
		}
		// 条件成立而 UPDATE 影响 0 行，最常见的原因是这一行本来就是目标值
		// （clientFoundRows=false 时「匹配但没变化」的行不计入影响行数）。只有栅栏与消息相等
		// （AllowEqual）或不带栅栏时才可能如此；栅栏不同则行不可能已是目标值，不必查。
		if plan.fenced() && !slices.Equal(state.stored, plan.fenceValues) {
			compared = nil
			continue
		}
		matches, err := conditionalRowMatchesTarget(x, plan)
		if err != nil {
			return ConditionalSaveResult{}, err
		}
		if matches {
			return ConditionalSaveResult{Outcome: SaveUnchanged, Stored: state.stored}, nil
		}
		if compared != nil && compared.equal(state) {
			return ConditionalSaveResult{Stored: lastStored}, fmt.Errorf("conditional save on table %s, stored fence %v: %w",
				tableName, lastStored, ErrSaveMatchMismatch)
		}
		compared = &state
	}
	return ConditionalSaveResult{Stored: lastStored}, fmt.Errorf("conditional save on table %s after %d rounds, stored fence %v: %w",
		tableName, conditionalSaveMaxRounds, lastStored, ErrConditionalSaveRace)
}

// readConditionalRowState 执行状态查询。栅栏值扫进 sql.Null[uint64]：
// 直接扫 int64 会在值超过 MaxInt64 时溢出；线上列若被改成可空并存了 NULL，明确报错而不是当成 0。
func readConditionalRowState(x conditionalSaveExecutor, plan *conditionalSavePlan) (conditionalRowState, error) {
	tableName := plan.table.tableName
	stmt := plan.stateStatement()
	rows, err := x.query(stmt.Sql, stmt.Args)
	if err != nil {
		return conditionalRowState{}, fmt.Errorf("read conditional save state on table %s: %w", tableName, err)
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return conditionalRowState{}, fmt.Errorf("read conditional save state on table %s: %w", tableName, err)
		}
		return conditionalRowState{}, nil
	}
	state := conditionalRowState{exists: true}
	if plan.fenced() {
		cells := make([]sql.Null[uint64], len(plan.fenceFields))
		dest := make([]interface{}, len(cells))
		for i := range cells {
			dest[i] = &cells[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return conditionalRowState{}, fmt.Errorf("scan fence columns on table %s: %w", tableName, err)
		}
		state.stored = make([]uint64, len(cells))
		for i, cell := range cells {
			if !cell.Valid {
				return conditionalRowState{}, fmt.Errorf("%w: 表 %s 的栅栏列 %s 是 NULL，栅栏列必须 NOT NULL",
					ErrSchemaDrift, tableName, plan.fenceFields[i].Name())
			}
			state.stored[i] = cell.V
		}
	}
	if rows.Next() {
		return conditionalRowState{}, fmt.Errorf("read conditional save state on table %s: %w", tableName, ErrMultipleRowsFound)
	}
	if err := rows.Err(); err != nil {
		return conditionalRowState{}, fmt.Errorf("read conditional save state on table %s: %w", tableName, err)
	}
	return state, nil
}

func conditionalRowMatchesTarget(x conditionalSaveExecutor, plan *conditionalSavePlan) (bool, error) {
	tableName := plan.table.tableName
	stmt, err := plan.matchStatement()
	if err != nil {
		return false, fmt.Errorf("generate conditional save match query for table %s: %w", tableName, err)
	}
	rows, err := x.query(stmt.Sql, stmt.Args)
	if err != nil {
		return false, fmt.Errorf("compare current row for conditional save on table %s: %w", tableName, err)
	}
	matches := rows.Next()
	rowsErr := rows.Err()
	_ = rows.Close()
	if rowsErr != nil {
		return false, fmt.Errorf("compare current row for conditional save on table %s: %w", tableName, rowsErr)
	}
	return matches, nil
}
