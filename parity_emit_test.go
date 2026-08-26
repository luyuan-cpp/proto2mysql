package proto2mysql

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
)

// 跨语言对拍：Go 侧的语料发射器。
//
// 「Go 版与 Python 版产出逐字节相同的 SQL」是本库的核心契约——两边跑同一份 .proto
// 必须产出同一份 DDL/DML，否则"并存迁移"就没有可验证的基准。
//
// 但这条契约在此之前**全靠人手把 Go 的字符串抄进 Python 的测试文件**，
// 没有任何自动化在守。抄漏一条、抄错一个反引号，谁也不会知道——
// 而 2026-08 那一轮就实测到了两处真实分叉（TEXT 索引前缀长度、sqlgen 排序键）。
//
// 用法：
//
//	PARITY_OUT=parity.go.json go test -count=1 -run TestEmitParityCorpus .
//
// 然后与 Python 侧的产物比对（见 tools/parity_diff.py）。
// 不设 PARITY_OUT 时本用例直接跳过，不影响日常 go test。
//
// ⚠️ 语料的用例清单是**两边共同的规格**：新增/删除用例必须两边同步改，
// 否则对拍器会报"用例集不一致"——那正是它该报的。

type parityCase struct {
	Name string   `json:"name"`
	SQL  string   `json:"sql"`
	Args []string `json:"args"`
}

type parityCorpus struct {
	CorpusVersion int          `json:"corpus_version"`
	Lang          string       `json:"lang"`
	Cases         []parityCase `json:"cases"`
}

// parityArg 把一个参数归一成**跨语言可比**的字符串。
//
// 两边的参数是不同语言的原生值，直接比没有意义。而且同一个逻辑值在两边的类型
// 还不一样：子消息的序列化字节，Go 这边是 string（Go 的 string 能装任意字节），
// Python 那边是 bytes——第一次跑对拍就是被这个绊了 10 条。
//
// 所以归一规则必须**先落到原始字节**，再按同一套判据决定怎么写。
// 两边逐字一致：
//
//	nil / None                      → "<nil>"
//	原始字节是可打印的 UTF-8 文本    → 原样保留（diff 可读）
//	否则                            → "0x" + 小写十六进制
func parityArg(v interface{}) string {
	if v == nil {
		return "<nil>"
	}
	var raw []byte
	switch x := v.(type) {
	case []byte:
		raw = x
	case string:
		raw = []byte(x)
	default:
		return fmt.Sprint(x)
	}
	if isPrintableUTF8(raw) {
		return string(raw)
	}
	return "0x" + hex.EncodeToString(raw)
}

// isPrintableUTF8 是不是「合法 UTF-8 且不含控制字符」。
// 判据必须与 Python 侧逐字一致，否则同一个值两边会落进不同分支。
func isPrintableUTF8(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	for _, r := range string(raw) {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func parityArgs(args []interface{}) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		out = append(out, parityArg(a))
	}
	return out
}

func TestEmitParityCorpus(t *testing.T) {
	out := os.Getenv("PARITY_OUT")
	if out == "" {
		t.Skip("跳过对拍语料发射：设置 PARITY_OUT=<文件> 以启用")
	}

	corpus := parityCorpus{CorpusVersion: 1, Lang: "go"}
	add := func(name, sql string, args []interface{}) {
		corpus.Cases = append(corpus.Cases, parityCase{Name: name, SQL: sql, Args: parityArgs(args)})
	}
	// 柯里化：Go 不允许「字面量 + 多返回值」混在同一次调用里，
	// 所以写成 emit("name")(b.Insert(x))——内层调用正好吃掉那两个返回值。
	emit := func(name string) func(*SqlWithArgs, error) {
		return func(stmt *SqlWithArgs, err error) {
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			add(name, stmt.Sql, stmt.Args)
		}
	}

	// ── 样本消息（两边必须逐字一致）────────────────────────────────────
	full := &testpb.GolangTest{
		Id: 7, Ip: "10.0.0.1", Port: 3306, GroupId: 42,
		Player: &testpb.Player{PlayerId: 99, Name: "alice"}, PlayerId: 1001,
	}
	sparse := &testpb.GolangTest{Id: 7, Ip: "x"} // 只赋两个字段，验 proto3 零值语义
	pkOnly := &testpb.GolangTest{Id: 7}
	batch := []proto.Message{
		&testpb.GolangTest{Id: 1, Ip: "a"},
		&testpb.GolangTest{Id: 2, Ip: "b"},
	}

	// ── DDL ────────────────────────────────────────────────────────────
	add("ddl/create/plain", NewSQLBuilder(&testpb.GolangTest{}).CreateTable(), nil)
	add("ddl/create/pk_autoinc",
		NewSQLBuilder(&testpb.GolangTest{}, WithPrimaryKey("id"), WithAutoIncrementKey("id")).CreateTable(), nil)
	add("ddl/create/index_unique",
		NewSQLBuilder(&testpb.GolangTest{}, WithPrimaryKey("id"),
			WithIndexes("player_id,group_id"), WithUniqueKey("ip")).CreateTable(), nil)
	add("ddl/create/nullable",
		NewSQLBuilder(&testpb.GolangTest{}, WithPrimaryKey("id"), WithNullableFields("port")).CreateTable(), nil)

	// ── schema.sql 的**文件级顺序**（曾经的真实分叉）─────────────────────
	//
	// Go 早先按注册键（proto full name）排、Python 按 table_name 排。两者在没声明
	// table_name 时恰好相同，所以长期看不出来；一旦用了 WithTableName，
	// 同一批语句会以不同顺序落进 schema.sql，逐字节就不一致了。
	// 逐表断言的 golden 盖不到这个，只有这条对拍能守住。
	schemaDB := NewDB()
	schemaDB.RegisterTable(&testpb.GolangTest{}, WithTableName("zzz_first"), WithPrimaryKey("id"))
	schemaDB.RegisterTable(&testpb.GolangTest1{}, WithTableName("aaa_second"), WithPrimaryKey("id"))
	var schemaBuf strings.Builder
	if err := schemaDB.WriteCreateTableSQL(&schemaBuf); err != nil {
		t.Fatalf("WriteCreateTableSQL: %v", err)
	}
	add("ddl/schema_file/multi_table_order", schemaBuf.String(), nil)

	// ── ALTER（本轮修复集中的地方）─────────────────────────────────────
	alterTable := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	ipType := alterTable.getMySQLFieldType(alterTable.Descriptor.Fields().ByName("ip"))
	for _, c := range []struct {
		name string
		cols map[string]columnMeta
	}{
		{"alter/empty_table", map[string]columnMeta{}},
		{"alter/missing_one_column", map[string]columnMeta{
			"id": {colType: "int unsigned", fieldNum: 1}, "ip": {colType: "mediumtext", fieldNum: 2},
			"port": {colType: "int unsigned", fieldNum: 3}, "group_id": {colType: "int unsigned", fieldNum: 4},
			"player": {colType: "mediumblob", fieldNum: 5},
		}},
		{"alter/rename_by_field_number", map[string]columnMeta{
			"id": {colType: "int unsigned", fieldNum: 1}, "old_ip": {colType: ipType, fieldNum: 2},
			"port": {colType: "int unsigned", fieldNum: 3}, "group_id": {colType: "int unsigned", fieldNum: 4},
			"player": {colType: "mediumblob", fieldNum: 5}, "player_id": {colType: "bigint unsigned", fieldNum: 6},
		}},
		{"alter/backfill_comment", map[string]columnMeta{
			"id": {colType: "int unsigned"}, "ip": {colType: "mediumtext"},
			"port": {colType: "int unsigned"}, "group_id": {colType: "int unsigned"},
			"player": {colType: "mediumblob"}, "player_id": {colType: "bigint unsigned"},
		}},
		{"alter/wider_online_column_untouched", map[string]columnMeta{
			"id": {colType: "bigint unsigned", fieldNum: 1}, "ip": {colType: "longtext", fieldNum: 2},
			"port": {colType: "bigint unsigned", fieldNum: 3}, "group_id": {colType: "bigint unsigned", fieldNum: 4},
			"player": {colType: "longblob", fieldNum: 5}, "player_id": {colType: "bigint unsigned", fieldNum: 6},
		}},
	} {
		clauses, err := alterTable.buildAlterClauses(c.cols, false)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		for i, clause := range clauses {
			add(fmt.Sprintf("%s/#%d", c.name, i), clause, nil)
		}
		if len(clauses) == 0 {
			add(c.name+"/#none", "", nil)
		}
	}

	// ── DML ────────────────────────────────────────────────────────────
	b := NewSQLBuilder(&testpb.GolangTest{}, WithPrimaryKey("id"))
	emit("dml/insert")(b.Insert(full))
	emit("dml/insert_set_fields")(b.InsertSetFields(sparse))
	emit("dml/insert_ignore")(b.InsertIgnore(full))
	emit("dml/replace")(b.Replace(full))
	emit("dml/save")(b.Table().GetSaveSQLWithArgs(full))
	emit("dml/batch_insert")(b.BatchInsert(batch))
	emit("dml/batch_replace")(b.BatchReplace(batch))
	emit("dml/batch_save")(b.Table().GetBatchSaveSQLWithArgs(batch))
	emit("dml/upsert")(b.Upsert(full, "ip", "port"))
	emit("dml/upsert_add")(b.UpsertAdd(full, "port"))
	emit("dml/upsert_keep_old")(b.UpsertKeepOld(full))

	emit("dml/update_by_pk")(b.UpdateByPK(sparse))
	emit("dml/update_by_pk_if")(b.UpdateByPKIf(sparse, "`group_id` = ?", []interface{}{3}))
	emit("dml/update_fields_by_pk")(b.UpdateFieldsByPK(pkOnly, "port"))
	emit("dml/incr_by_pk")(b.IncrByPK(pkOnly, "port", 5))
	emit("dml/decr_by_pk_if_enough")(b.DecrByPKIfEnough(pkOnly, "port", 5))

	emit("dml/select_by_pk")(b.SelectByPK(pkOnly))
	emit("dml/select_by_pk_for_update")(b.SelectByPKForUpdate(pkOnly))
	add("dml/select_where_plain", b.SelectWhere("`port` > ?", []interface{}{100}, QueryOptions{}).Sql,
		[]interface{}{100})
	add("dml/select_where_paged",
		b.SelectWhere("`port` > ?", []interface{}{100},
			QueryOptions{OrderBy: "`id` DESC", Limit: 20, Offset: 40}).Sql, []interface{}{100})
	add("dml/count", b.Count("`port` > ?", []interface{}{100}).Sql, []interface{}{100})
	add("dml/exists", b.Exists("`port` > ?", []interface{}{100}).Sql, []interface{}{100})
	emit("dml/delete_by_pk")(b.DeleteByPK(pkOnly))

	// ── 补齐其余公开方法（覆盖闸会强制这里不许漏）─────────────────────
	emit("dml/insert_ignore_set_fields")(b.InsertIgnoreSetFields(sparse))
	emit("dml/batch_insert_ignore")(b.BatchInsertIgnore(batch))
	emit("dml/batch_upsert")(b.BatchUpsert(batch, "ip"))
	emit("dml/batch_upsert_with")(b.BatchUpsertWith(batch, AddNew("port")))
	emit("dml/upsert_with")(b.UpsertWith(full, SetNew("ip"), AddNew("port")))
	add("dml/create_table", b.CreateTable(), nil)

	pkWhere, pkArgs, err := b.PrimaryKeyWhere(pkOnly)
	if err != nil {
		t.Fatalf("PrimaryKeyWhere: %v", err)
	}
	add("dml/primary_key_where", pkWhere, pkArgs)
	add("dml/table_name", b.TableName(), nil)

	emit("dml/select_columns")(b.SelectColumns([]string{"id", "ip"}, "`port` > ?",
		[]interface{}{100}, QueryOptions{OrderBy: "`id` ASC", Limit: 5}))
	emit("dml/select_by_kv_in")(b.SelectByKVIn("port", []interface{}{80, 443}, QueryOptions{}))
	emit("dml/select_by_pk_in")(b.SelectByPKIn([]interface{}{1, 2, 3}, QueryOptions{}))
	emit("dml/exists_by_pk_for_update")(b.ExistsByPKForUpdate(pkOnly))

	emit("dml/update_where")(b.UpdateWhere(sparse, "`port` > ?", []interface{}{100}))
	emit("dml/update_assigns_by_pk")(b.UpdateAssignsByPK(pkOnly, AddCol("port", 5), SetCol("ip", "z")))
	emit("dml/update_assigns_where")(b.UpdateAssignsWhere([]Assign{SubCol("port", 3)}, "`id` = ?", []interface{}{7}))

	emit("dml/delete_by_pk_if")(b.DeleteByPKIf(pkOnly, "`port` = ?", []interface{}{0}))
	emit("dml/delete_by_pk_in")(b.DeleteByPKIn([]interface{}{1, 2}))
	emit("dml/delete_by_kv_in")(b.DeleteByKVIn("port", []interface{}{80, 443}))
	emit("dml/delete_where")(b.DeleteWhere("`port` = ?", []interface{}{0}))
	emit("dml/delete_where_limit")(b.DeleteWhereLimit("`port` = ?", []interface{}{0}, "`id` ASC", 10))

	data, err := json.MarshalIndent(corpus, "", "  ")
	if err != nil {
		t.Fatalf("序列化语料失败: %v", err)
	}
	if err := os.WriteFile(out, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("写 %s 失败: %v", out, err)
	}
	t.Logf("已发射 %d 条用例 -> %s", len(corpus.Cases), out)
}

// TestParityCorpusCoversEveryPublicAPI 覆盖闸：**每个产 SQL 的公开方法都必须在对拍语料里**。
//
// 为什么需要它：语料本身也会腐烂。加了新 API 却忘了加对拍用例，
// 对拍照样报绿——于是新方法上的跨语言分叉可以一路溜到线上。
// 实测过一次：41 个公开方法里有 **20 个**（近一半）当时不在语料里。
//
// 这道闸用反射枚举 *SQLBuilder 的公开方法，逐个检查方法名有没有出现在语料的
// 用例名里（去下划线后不分大小写比对）。漏一个就红——**这才是让语料不腐烂的机制**，
// 光靠"记得加"是不行的。
//
// 白名单里只放**确实不产 SQL** 的方法，加进去要写明理由。
func TestParityCorpusCoversEveryPublicAPI(t *testing.T) {
	// 不产 SQL 的访问器，不需要对拍
	exempt := map[string]string{
		"Table": "返回 *MessageTable 本身，不是 SQL",
	}

	tmp := t.TempDir() + "/parity.json"
	t.Setenv("PARITY_OUT", tmp)
	// 复用发射器产出语料（它自己会写文件）
	t.Run("emit", func(t *testing.T) { TestEmitParityCorpus(t) })

	raw, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatalf("读语料失败: %v", err)
	}
	var corpus parityCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatalf("解析语料失败: %v", err)
	}
	var haystack strings.Builder
	for _, c := range corpus.Cases {
		haystack.WriteString(strings.ToLower(strings.ReplaceAll(c.Name, "_", "")))
		haystack.WriteString(" ")
	}
	covered := haystack.String()

	typ := reflect.TypeOf(&SQLBuilder{})
	var missing []string
	for i := 0; i < typ.NumMethod(); i++ {
		name := typ.Method(i).Name
		if _, ok := exempt[name]; ok {
			continue
		}
		if !strings.Contains(covered, strings.ToLower(name)) {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		t.Errorf("以下 %d 个公开方法没有对拍用例：\n  %s\n\n"+
			"每个产 SQL 的公开方法都必须在语料里，否则它上面的跨语言分叉没人守。\n"+
			"两边都要加：Go 在 parity_emit_test.go，Python 在 tools/parity_emit.py。\n"+
			"确实不产 SQL 的，加进本用例的 exempt 白名单并写明理由。",
			len(missing), strings.Join(missing, "\n  "))
	}
}
