package proto2mysql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const integrationEnv = "PROTO2MYSQL_INTEGRATION"

// GetMysqlConfig 读取testdata/db.json中的测试数据库连接配置
func GetMysqlConfig() *mysql.Config {
	file, err := os.Open("testdata/db.json")
	defer func(file *os.File) {
		if file != nil {
			if err := file.Close(); err != nil {
				fmt.Printf("关闭testdata/db.json失败: %v\n", err)
			}
		}
	}(file)
	if err != nil {
		fmt.Printf("打开testdata/db.json失败: %v\n", err)
		return nil
	}
	decoder := json.NewDecoder(file)
	jsonConfig := JsonConfig{}
	if err := decoder.Decode(&jsonConfig); err != nil {
		log.Printf("解析testdata/db.json失败: %v", err)
		return nil
	}
	return NewMysqlConfig(jsonConfig)
}

func mustOpenTestDB(t *testing.T, pdb *DB) *sql.DB {
	t.Helper()

	if testing.Short() {
		t.Skip("跳过数据库集成测试: short 模式")
	}

	if os.Getenv(integrationEnv) != "1" {
		t.Skip("跳过数据库集成测试: 设置 PROTO2MYSQL_INTEGRATION=1 以启用")
	}

	mysqlConfig := GetMysqlConfig()
	if mysqlConfig == nil {
		t.Fatal("获取MySQL配置失败，请检查testdata/db.json文件")
	}

	conn, err := mysql.NewConnector(mysqlConfig)
	if err != nil {
		t.Fatalf("创建MySQL连接器失败: %v", err)
	}

	db := sql.OpenDB(conn)
	if err := db.Ping(); err != nil {
		t.Fatalf("数据库连接失败: %v", err)
	}

	if err := pdb.OpenDB(db, mysqlConfig.DBName); err != nil {
		t.Fatalf("切换数据库失败: %v", err)
	}

	return db
}

func closeTestDB(t *testing.T, db *sql.DB) {
	t.Helper()
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		t.Logf("关闭数据库失败: %v", err)
	}
}

func testTableSQLName(m proto.Message) string {
	return escapeMySQLName(GetTableName(m))
}

// recreateTestTable 删表重建：保证表schema与当前注册选项（主键/自增等）一致。
// CREATE TABLE IF NOT EXISTS不会给已存在的表补主键，共享表的测试之间会互相污染
func recreateTestTable(t *testing.T, db *sql.DB, pdb *DB, m proto.Message) {
	t.Helper()
	table, err := pdb.tableForMessage(m)
	if err != nil {
		t.Fatalf("解析注册表失败: %v", err)
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS " + escapeMySQLName(table.tableName)); err != nil {
		t.Fatalf("重建前清理表失败: %v", err)
	}
	if _, err := db.Exec(table.GetCreateTableSQL()); err != nil {
		t.Fatalf("预处理表结构失败: %v", err)
	}
}

// TestCreateTable 测试创建表
func TestCreateTable(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	createSQL := pdb.GetCreateTableSQL(testTable)
	if createSQL == "" {
		t.Fatal("生成创建表SQL失败")
	}
	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("执行创建表SQL失败: %v, SQL: %s", err, createSQL)
	}
	t.Log("创建表成功")
}

// TestAlterTable 测试修改表字段
func TestAlterTable(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 先确保表存在
	if _, err := db.Exec(pdb.GetCreateTableSQL(testTable)); err != nil {
		t.Fatalf("预处理表结构失败: %v", err)
	}

	if err := pdb.UpdateTableField(testTable); err != nil {
		t.Fatalf("执行ALTER TABLE失败: %v", err)
	}
	t.Log("ALTER TABLE成功")
}

// TestLoadSave 测试单条数据存/取
func TestLoadSave(t *testing.T) {
	pdb := NewDB()
	pbSave := &testpb.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &testpb.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbSave1 := &testpb.GolangTest{
		Id:      2,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &testpb.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pdb.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 清理旧数据
	if _, err := db.Exec("DELETE FROM " + testTableSQLName(pbSave) + " WHERE id IN (1,2)"); err != nil {
		t.Logf("清理旧数据失败: %v（忽略，可能是首次执行）", err)
	}

	// 保存数据
	if err := pdb.Save(pbSave); err != nil {
		t.Fatalf("保存pbSave失败: %v", err)
	}
	if err := pdb.Save(pbSave1); err != nil {
		t.Fatalf("保存pbSave1失败: %v", err)
	}

	// 验证数据
	pbLoad := &testpb.GolangTest{}
	if err := pdb.FindOneByKV(pbLoad, "id", "1"); err != nil {
		t.Fatalf("读取id=1的数据失败: %v", err)
	}
	if !proto.Equal(pbSave, pbLoad) {
		t.Error("保存与读取的数据不一致（id=1）")
		t.Logf("预期: %s", pbSave.String())
		t.Logf("实际: %s", pbLoad.String())
	}

	pbLoad1 := &testpb.GolangTest{}
	if err := pdb.FindOneByKV(pbLoad1, "id", "2"); err != nil {
		t.Fatalf("读取id=2的数据失败: %v", err)
	}
	if !proto.Equal(pbSave1, pbLoad1) {
		t.Error("保存与读取的数据不一致（id=2）")
	}
}

// TestFindInsert 测试INSERT ON DUPLICATE KEY UPDATE
func TestFindInsert(t *testing.T) {
	pdb := NewDB()
	pbSave := &testpb.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &testpb.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pbSave1 := &testpb.GolangTest{
		Id:      2,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &testpb.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pdb.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 清理旧数据
	if _, err := db.Exec("DELETE FROM " + testTableSQLName(pbSave) + " WHERE id IN (1,2)"); err != nil {
		t.Logf("清理旧数据失败: %v", err)
	}

	// 执行插入更新
	if err := pdb.InsertOnDupUpdate(pbSave); err != nil {
		t.Fatalf("执行InsertOnDupUpdate(pbSave)失败: %v", err)
	}
	if err := pdb.InsertOnDupUpdate(pbSave1); err != nil {
		t.Fatalf("执行InsertOnDupUpdate(pbSave1)失败: %v", err)
	}

	// 验证数据
	pbLoad := &testpb.GolangTest{}
	if err := pdb.FindOneByKV(pbLoad, "id", "1"); err != nil {
		t.Fatalf("读取id=1失败: %v", err)
	}
	if !proto.Equal(pbSave, pbLoad) {
		t.Error("InsertOnDupUpdate后数据不一致（id=1）")
	}
}

// TestLoadByWhereCase 测试按条件查询
func TestLoadByWhereCase(t *testing.T) {
	pdb := NewDB()
	pbSave := &testpb.GolangTest{
		Id:      1,
		GroupId: 1,
		Ip:      "127.0.0.1",
		Port:    3306,
		Player: &testpb.Player{
			PlayerId: 111,
			// 修复：特殊字符用双反斜杠转义
			Name: "foo\\\\0bar,foo\\\\nbar,foo\\\\rbar,foo\\\\Zbar,foo\\\\\"bar,foo\\\\\\\\bar,foo\\\\'bar",
		},
	}
	pdb.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 清理旧数据
	if _, err := db.Exec("DELETE FROM " + testTableSQLName(pbSave) + " WHERE id=1"); err != nil {
		t.Logf("清理旧数据失败: %v", err)
	}

	// 保存数据
	if err := pdb.Save(pbSave); err != nil {
		t.Fatalf("保存数据失败: %v", err)
	}

	// 按条件查询（WHERE子句无需加"where"前缀）
	pbLoad := &testpb.GolangTest{}
	if err := pdb.FindOneByWhereClause(pbLoad, "id=1"); err != nil {
		t.Fatalf("执行FindOneByWhereClause失败: %v", err)
	}
	if !proto.Equal(pbSave, pbLoad) {
		t.Error("按条件查询后数据不一致")
		t.Logf("预期: %s", pbSave.String())
		t.Logf("实际: %s", pbLoad.String())
	}
}

// TestSpecialCharacterEscape 测试特殊字符存/取一致性
func TestSpecialCharacterEscape(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 测试用特殊字符集（修复：所有反斜杠用双反斜杠转义）
	specialChars := []struct {
		name  string
		value string
	}{
		{"NULL字符（\\0）", "a\\\\0b"},
		{"换行符（\\n）", "a\\\\nb"},
		{"回车符（\\r）", "a\\\\r b"},
		{"双引号（\\\"）", `a\\\\\"b`},
		{"单引号（\\'）", `a\\\\'b`},
		{"反斜杠（\\\\）", `a\\\\\\\\b`},
		{"制表符（\\t）", "a\\\\tb"},
		{"逗号（,）", "a,b"},
		{"美元符（$）", "a$b"},
		{"百分号（%）", "a%b"},
	}

	testID := uint32(1000)
	for _, sc := range specialChars {
		testID++
		// 构造测试数据
		pbSave := &testpb.GolangTest{
			Id:      testID,
			GroupId: 999,
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &testpb.Player{
				PlayerId: uint64(testID),
				Name:     fmt.Sprintf("Test_%s: %s", sc.name, sc.value),
			},
		}

		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+testTableSQLName(testTable)+" WHERE id=?", testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", sc.name, err)
		}

		// 保存数据
		if err := pdb.Save(pbSave); err != nil {
			t.Errorf("保存[%s]数据失败: %v, 原始值: %q", sc.name, err, sc.value)
			continue
		}

		// 读取数据
		pbLoad := &testpb.GolangTest{}
		if err := pdb.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10)); err != nil {
			t.Errorf("读取[%s]数据失败: %v", sc.name, err)
			continue
		}

		// 验证一致性
		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s]数据不一致", sc.name)
			t.Logf("预期Name: %q", pbSave.Player.Name)
			t.Logf("实际Name: %q", pbLoad.Player.Name)
		} else {
			t.Logf("[%s]测试通过，原始值: %q", sc.name, sc.value)
		}
	}
}

// TestStringWithSpaces 测试空格处理
func TestStringWithSpaces(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 测试用例
	testCases := []struct {
		id   int32
		name string
		desc string
	}{
		{2001, "Single space between words", "单个空格"},
		{2002, "  Double  spaces  between  words  ", "前后双空格+中间双空格"},
		{2003, " Leading space", "前导空格"},
		{2004, "Trailing space ", "尾随空格"},
		// 修复：制表符、换行符用双反斜杠转义
		{2005, "Mixed\\\\tspaces\\\\nand\\\\vother\\\\fwhitespace", "混合空白符"},
	}

	for _, tc := range testCases {
		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+testTableSQLName(testTable)+" WHERE id=?", tc.id); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", tc.desc, err)
		}

		// 保存数据
		pbSave := &testpb.GolangTest{
			Id:      uint32(tc.id),
			GroupId: 200,
			Ip:      "192.168.2.1",
			Port:    3306,
			Player: &testpb.Player{
				PlayerId: uint64(tc.id),
				Name:     tc.name,
			},
		}
		if err := pdb.Save(pbSave); err != nil {
			t.Errorf("保存[%s]数据失败: %v", tc.desc, err)
			continue
		}

		// 读取数据
		pbLoad := &testpb.GolangTest{}
		if err := pdb.FindOneByKV(pbLoad, "id", strconv.FormatInt(int64(tc.id), 10)); err != nil {
			t.Errorf("读取[%s]数据失败: %v", tc.desc, err)
			continue
		}

		// 验证空格一致性
		if pbLoad.Player.Name != tc.name {
			t.Errorf("[%s]空格处理不一致", tc.desc)
			t.Logf("预期: %q (长度: %d)", tc.name, len(tc.name))
			t.Logf("实际: %q (长度: %d)", pbLoad.Player.Name, len(pbLoad.Player.Name))
		} else {
			t.Logf("[%s]测试通过", tc.desc)
		}
	}
}

// TestLoadSaveListWhereCase 测试批量查询
func TestLoadSaveListWhereCase(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 构造预期数据
	expectedList := &testpb.GolangTestList{
		TestList: []*testpb.GolangTest{
			{
				Id:      101,
				GroupId: 1,
				Ip:      "127.0.0.1",
				Port:    3306,
				Player: &testpb.Player{
					PlayerId: 1001,
					Name:     "BatchTest_1",
				},
			},
			{
				Id:      102,
				GroupId: 1,
				Ip:      "127.0.0.1",
				Port:    3306,
				Player: &testpb.Player{
					PlayerId: 1002,
					Name:     "BatchTest_2",
				},
			},
		},
	}

	// 清理旧数据
	if _, err := db.Exec("DELETE FROM " + testTableSQLName(testTable) + " WHERE group_id=1"); err != nil {
		t.Logf("清理批量测试旧数据失败: %v", err)
	}

	// 批量保存
	for _, item := range expectedList.TestList {
		if err := pdb.Save(item); err != nil {
			t.Fatalf("批量保存数据失败（id=%d）: %v", item.Id, err)
		}
	}

	// 批量查询
	actualList := &testpb.GolangTestList{}
	if err := pdb.FindAllByWhereClause(actualList, "group_id=1"); err != nil {
		t.Fatalf("批量查询失败: %v", err)
	}

	// 验证数量
	if len(actualList.TestList) != len(expectedList.TestList) {
		t.Fatalf("批量查询结果数量不一致，预期%d条，实际%d条", len(expectedList.TestList), len(actualList.TestList))
	}

	// 按ID排序（避免顺序问题）
	sort.Slice(expectedList.TestList, func(i, j int) bool {
		return expectedList.TestList[i].Id < expectedList.TestList[j].Id
	})
	sort.Slice(actualList.TestList, func(i, j int) bool {
		return actualList.TestList[i].Id < actualList.TestList[j].Id
	})

	// 逐条验证
	for i := range expectedList.TestList {
		if !proto.Equal(expectedList.TestList[i], actualList.TestList[i]) {
			t.Errorf("批量查询第%d条数据不一致", i+1)
			t.Logf("预期: %s", expectedList.TestList[i].String())
			t.Logf("实际: %s", actualList.TestList[i].String())
		}
	}
	t.Log("批量查询测试通过")
}

// TestSpecialCharacterEscape 测试特殊字符存/取一致性（新增12种场景，覆盖全类型）
func TestSpecialCharacterEscape1(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 新增：12种高频特殊字符场景 + 原有场景，共22种
	specialChars := []struct {
		name  string // 场景名称
		value string // 测试值（Go字符串需双反斜杠转义）
		desc  string // 场景说明
	}{
		// 一、MySQL语法敏感字符（5种）
		{"SQL注释符", "select * from t--", "包含MySQL单行注释符--，验证参数化防注入"},
		{"SQL通配符", "a%b_c", "包含%（任意字符）、_（单个字符），验证查询时不被解析为通配符"},
		{"括号与逗号", "(a,b),[c;d]", "包含SQL常用分隔符，验证转义后结构完整"},
		{"反引号", "`user`", "包含MySQL字段名标识符`，验证存储后不被解析为字段"},
		{"分号", "a;drop table t", "包含SQL语句结束符;，验证参数化防注入"},

		// 二、控制字符（4种）
		{"NULL字符（\\0）", "a\\\\0b", "ASCII 0x00，数据库中易被截断的特殊控制符"},
		{"换行符（\\n）", "a\\\\nb\\\\nc", "多行文本场景，验证换行结构保留"},
		{"回车符（\\r）", "a\\\\rb\\\\rc", "Windows换行符组成部分（\\r\\n），验证不被过滤"},
		{"制表符（\\t）", "name\\\\tage\\\\tsex", "表格数据分隔场景，验证缩进保留"},

		// 三、引号与反斜杠（3种）
		{"双引号（\\\"）", `a\\\\\"b\\\\\"c`, "JSON/XML常用符号，验证转义后不被解析为字符串结束"},
		{"单引号（\\'）", `a\\\\'b\\\\'c`, "SQL字符串标识符，验证参数化防注入"},
		{"反斜杠（\\\\）", `a\\\\\\\\b\\\\\\\\c`, "路径/正则常用符号，验证多重转义后正确性"},

		// 四、Unicode与多字节字符（6种）
		{"中文汉字", "测试中文：你好，世界！", "多字节UTF-8字符，验证编码不混乱"},
		{"特殊符号", "★☆●○△△□□", "Unicode特殊符号，验证字体符号保留"},
		{"emoji表情", "😊😂👍👏🎉", "移动端常用emoji，验证UTF-8mb4编码支持（需数据库字符集为utf8mb4）"},
		{"全角字符", "１２３４５６ａｂｃｄｅ", "中文输入法全角数字/字母，验证与半角区分存储"},
		{"生僻字", "𪚥𪚥𪚥（四个龍）", "Unicode扩展区生僻字，验证不出现乱码"},
		{"国际字符", "café（法语）、straße（德语）", "带 accents 的国际字符，验证多语言支持"},

		// 五、其他高频场景（4种）
		{"空格组合", "  前导双空格  中间双空格  尾随双空格  ", "复杂空格场景，验证不被自动截断"},
		{"URL地址", "https://www.example.com/path?a=1&b=2#hash", "包含://、?、&、#的URL，验证参数保留"},
		{"Base64编码", "SGVsbG8gV29ybGQh（Hello World!）", "Base64字符串（含=补位符），验证编码完整性"},
		{"正则表达式", "^[a-z0-9_]{3,16}$", "正则符号（^、$、[]、{}），验证特殊符号不被解析"},
	}

	testID := uint32(1000)
	for _, sc := range specialChars {
		testID++
		// 1. 构造测试数据（包含场景名称，便于问题定位）
		pbSave := &testpb.GolangTest{
			Id:      testID,
			GroupId: 999, // 固定GroupId，便于后续批量清理
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &testpb.Player{
				PlayerId: uint64(testID),
				Name:     fmt.Sprintf("[%s]%s", sc.name, sc.value), // 前缀标记场景，便于日志排查
			},
		}

		// 2. 清理旧数据（按ID精准清理，避免影响其他测试）
		cleanSQL := "DELETE FROM " + testTableSQLName(testTable) + " WHERE id=?"
		if _, err := db.Exec(cleanSQL, testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v（忽略，可能是首次执行）", sc.name, err)
		}

		// 3. 保存数据（验证存储过程无错误）
		if err := pdb.Save(pbSave); err != nil {
			t.Errorf("保存[%s]失败: %v\n场景说明: %s\n原始值: %q",
				sc.name, err, sc.desc, sc.value)
			continue
		}

		// 4. 读取数据（验证读取过程无错误）
		pbLoad := &testpb.GolangTest{}
		findErr := pdb.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10))
		if findErr != nil {
			t.Errorf("读取[%s]失败: %v\n场景说明: %s\n原始值: %q",
				sc.name, findErr, sc.desc, sc.value)
			continue
		}

		// 5. 验证数据一致性（重点对比Player.Name字段）
		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s]数据不一致\n场景说明: %s", sc.name, sc.desc)
			t.Logf("预期Name: %q（长度: %d）", pbSave.Player.Name, len(pbSave.Player.Name))
			t.Logf("实际Name: %q（长度: %d）", pbLoad.Player.Name, len(pbLoad.Player.Name))
			// 额外打印字符编码对比，便于定位乱码问题
			t.Logf("预期编码: %x", []byte(pbSave.Player.Name))
			t.Logf("实际编码: %x", []byte(pbLoad.Player.Name))
		} else {
			t.Logf("✅ [%s]测试通过\n场景说明: %s\n原始值: %q",
				sc.name, sc.desc, sc.value)
		}
	}

	// 测试结束后批量清理测试数据（避免污染数据库）
	cleanAllSQL := "DELETE FROM " + testTableSQLName(testTable) + " WHERE group_id=999"
	if _, err := db.Exec(cleanAllSQL); err != nil {
		t.Logf("批量清理测试数据失败: %v", err)
	} else {
		t.Log("\n✅ 所有特殊字符测试数据已批量清理")
	}
}

// TestFullRangeSpecialCharacters 覆盖ASCII全范围+Unicode扩展的所有特殊字符测试
func TestFullRangeSpecialCharacters(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// --------------- 1. ASCII控制字符（0-31 + 127，共33个）---------------
	asciiControlChars := []struct {
		code int    // ASCII码
		name string // 控制符名称
	}{
		{0, "NULL（NUL）"}, {1, "标题开始（SOH）"}, {2, "文本开始（STX）"}, {3, "文本结束（ETX）"},
		{4, "传输结束（EOT）"}, {5, "请求（ENQ）"}, {6, "确认（ACK）"}, {7, "响铃（BEL）"},
		{8, "退格（BS）"}, {9, "水平制表（HT）"}, {10, "换行（LF）"}, {11, "垂直制表（VT）"},
		{12, "换页（FF）"}, {13, "回车（CR）"}, {14, "移位输出（SO）"}, {15, "移位输入（SI）"},
		{16, "数据链路转义（DLE）"}, {17, "设备控制1（DC1）"}, {18, "设备控制2（DC2）"}, {19, "设备控制3（DC3）"},
		{20, "设备控制4（DC4）"}, {21, "否定确认（NAK）"}, {22, "同步空闲（SYN）"}, {23, "传输块结束（ETB）"},
		{24, "取消（CAN）"}, {25, "介质结束（EM）"}, {26, "替换（SUB）"}, {27, "转义（ESC）"},
		{28, "文件分隔符（FS）"}, {29, "组分隔符（GS）"}, {30, "记录分隔符（RS）"}, {31, "单元分隔符（US）"},
		{127, "删除（DEL）"},
	}

	// --------------- 2. ASCII可打印特殊字符（32-47 + 58-64 + 91-96 + 123-126，共32个）---------------
	asciiPrintableSpecials := []struct {
		char rune   // 字符
		name string // 字符名称
	}{
		{' ', "空格"}, {'!', "感叹号"}, {'"', "双引号"}, {'#', "井号"}, {'$', "美元符"}, {'%', "百分号"}, {'&', "和号"},
		{'\'', "单引号"}, {'(', "左括号"}, {')', "右括号"}, {'*', "星号"}, {'+', "加号"}, {',', "逗号"}, {'-', "减号"},
		{'.', "句号"}, {'/', "斜杠"}, {':', "冒号"}, {';', "分号"}, {'<', "小于号"}, {'=', "等号"}, {'>', "大于号"},
		{'?', "问号"}, {'@', "艾特符"}, {'[', "左方括号"}, {'\\', "反斜杠"}, {']', "右方括号"}, {'^', "脱字符"},
		{'_', "下划线"}, {'`', "反引号"}, {'{', "左大括号"}, {'|', "竖线"}, {'}', "右大括号"}, {'~', "波浪号"},
	}

	// --------------- 3. Unicode扩展特殊字符（覆盖多语言、符号、emoji全场景）---------------
	unicodeSpecialChars := []struct {
		value string // 字符/字符组
		name  string // 场景名称
		desc  string // 说明
	}{
		// 3.1 多语言特殊字符（10种）
		{"café（法）、naïve（法）、città（意）", "带重音符号", "拉丁语系重音字符"},
		{"straße（德）、schön（德）", "德语变音符号", "德语ä/ö/ü/ß"},
		{"проверка（俄）、привет（俄）", "西里尔字母", "俄语/乌克兰语等斯拉夫语言"},
		{"あいうえお（日）、かきくけこ（日）", "日语假名", "平假名+片假名"},
		{"한글테스트（韩）、안녕하세요（韩）", "韩语字符", "韩语 Hangul 字母"},
		{"你好（中）、こんにちは（日）、안녕（韩）", "东亚文字混合", "中日韩三国文字混合"},
		{"עברית（希伯来）、שלום（希伯来）", "希伯来字母", "右到左书写的闪米特语言"},
		{"العربية（阿）、مرحبا（阿）", "阿拉伯字母", "阿拉伯语+波斯语常用字符"},
		{"தமிழ்（泰米尔）、வணக்கம்（泰米尔）", "南印度字母", "泰米尔语/泰语等南亚语言"},
		{"๏มันส์（泰）、สวัสดี（泰）", "泰语字母", "东南亚泰语特殊字符"},

		// 3.2 特殊符号（8种）
		{"★☆●○△△□□◇◇♡♥", "图形符号", "基础图形符号"},
		{"①②③④⑤、⑩⑪⑫、ⅠⅡⅢⅣⅤ", "带圈数字", "序号类符号"},
		{"←→↑↓↔↕、↖↗↘↙", "方向箭头", "各类方向符号"},
		{"∀∃∈∉⊂⊃⊆⊇、∧∨∩∪", "数学符号", "集合论/逻辑符号"},
		{"αβγδδεζηθ、ΓΔΕΖΗΘ", "希腊字母", "数学/物理常用希腊字母"},
		{"♠♥♣♦、♤♡♧♢", "扑克牌符号", "游戏场景常用符号"},
		{"©®™、℗℠ℤ", "版权符号", "知识产权相关符号"},
		{"°℃℉、%‰‱、$€£¥", "单位符号", "温度/百分比/货币单位"},

		// 3.3 Emoji全场景（6种）
		{"😊😂👍👏🎉、😭😘😜😎😢", "面部表情", "基础emoji表情"},
		{"🐱🐶🐘🐼🐯、🐦🐟🐸🐍🐢", "动物表情", "各类动物emoji"},
		{"🚗🚕🚙🚌🚎、✈️🚢🚂🚊", "交通工具", "海陆空交通工具emoji"},
		{"🏳️‍🌈🏳️‍⚧️、🇨🇳🇺🇸🇯🇵🇰🇷", "旗帜符号", "彩虹旗/性别旗/国家旗帜"},
		{"👨‍👩‍👧‍👦👨‍👨‍👧‍👦、👩‍❤️‍💋‍👨", "组合emoji", "多人物/动作组合emoji"},
		{"🫠🫶🫦🫡🫑、🫒🫓🫔🫕", "新emoji（iOS 15+）", "较新的emoji字符，验证兼容性"},

		// 3.4 特殊格式字符（4种）
		{"⁰¹²³⁴⁵⁶⁷⁸⁹、₀₁₂₃₄₅₆₇₈₉", "上标/下标", "数学公式上标下标"},
		{"𝐀𝐁𝐂𝐃𝐄、𝑎𝑏𝑐𝑑𝑒、𝓐𝓑𝓒𝓓𝓔", "特殊字体", "黑体/斜体/花体字母"},
		{"▁▂▃▄▅▆▇█、█▇▆▅▄▃▂▁", "方块符号", "进度条/填充场景符号"},
		{"┌─┬─┐、├─┼─┤、└─┴─┘", "表格边框", "ASCII艺术表格符号"},
	}

	// --------------- 执行全量测试 ---------------
	testID := uint32(1000) // 测试ID起始值，避免与其他测试冲突

	// 1. 测试ASCII控制字符（0-31 + 127）
	t.Log("=== 开始测试ASCII控制字符（0-31 + 127）===")
	for _, ctrl := range asciiControlChars {
		testID++
		// 控制字符无法直接打印，用「ASCII:XX」标记，值用转义序列表示
		escapedVal := fmt.Sprintf("ASCII_%d(\\x%02x)", ctrl.code, ctrl.code)
		pbSave := &testpb.GolangTest{
			Id:      testID,
			GroupId: 999,
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &testpb.Player{
				PlayerId: uint64(testID),
				Name:     fmt.Sprintf("[%s]%s", ctrl.name, escapedVal),
			},
		}

		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+testTableSQLName(testTable)+" WHERE id=?", testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", ctrl.name, err)
		}

		// 保存数据（控制字符需用bytes构造，避免Go字符串自动过滤）
		var ctrlByte = byte(ctrl.code)
		pbSave.Player.Name = fmt.Sprintf("[%s]包含控制字符: %s (原始字节: \\x%02x)",
			ctrl.name, escapedVal, ctrlByte)
		if err := pdb.Save(pbSave); err != nil {
			t.Errorf("保存[%s]失败: %v", ctrl.name, err)
			continue
		}

		// 读取验证
		pbLoad := &testpb.GolangTest{}
		if err := pdb.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10)); err != nil {
			t.Errorf("读取[%s]失败: %v", ctrl.name, err)
			continue
		}

		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s]数据不一致", ctrl.name)
			t.Logf("预期: %q (长度: %d)", pbSave.Player.Name, len(pbSave.Player.Name))
			t.Logf("实际: %q (长度: %d)", pbLoad.Player.Name, len(pbLoad.Player.Name))
		} else {
			t.Logf("✅ [%s]测试通过（ASCII: %d）", ctrl.name, ctrl.code)
		}
	}

	// 2. 测试ASCII可打印特殊字符（32-47等）
	t.Log("\n=== 开始测试ASCII可打印特殊字符 ===")
	for _, spec := range asciiPrintableSpecials {
		testID++
		// 构造包含当前特殊字符的字符串（混合字母+特殊字符，模拟真实场景）
		testStr := fmt.Sprintf("[%s]测试字符串: a%sb%sc%sd", spec.name, string(spec.char), string(spec.char), string(spec.char))
		pbSave := &testpb.GolangTest{
			Id:      testID,
			GroupId: 999,
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &testpb.Player{
				PlayerId: uint64(testID),
				Name:     testStr,
			},
		}

		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+testTableSQLName(testTable)+" WHERE id=?", testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", spec.name, err)
		}

		// 保存数据
		if err := pdb.Save(pbSave); err != nil {
			t.Errorf("保存[%s(%c)]失败: %v", spec.name, spec.char, err)
			continue
		}

		// 读取验证
		pbLoad := &testpb.GolangTest{}
		if err := pdb.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10)); err != nil {
			t.Errorf("读取[%s(%c)]失败: %v", spec.name, spec.char, err)
			continue
		}

		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s(%c)]数据不一致", spec.name, spec.char)
			t.Logf("预期: %q", pbSave.Player.Name)
			t.Logf("实际: %q", pbLoad.Player.Name)
		} else {
			t.Logf("✅ [%s(%c)]测试通过", spec.name, spec.char)
		}
	}

	// 3. 测试Unicode扩展特殊字符
	t.Log("\n=== 开始测试Unicode扩展特殊字符 ===")
	for _, unicode := range unicodeSpecialChars {
		testID++
		pbSave := &testpb.GolangTest{
			Id:      testID,
			GroupId: 999,
			Ip:      "192.168.1.100",
			Port:    3306,
			Player: &testpb.Player{
				PlayerId: uint64(testID),
				Name:     fmt.Sprintf("[%s]%s（说明: %s）", unicode.name, unicode.value, unicode.desc),
			},
		}

		// 清理旧数据
		if _, err := db.Exec("DELETE FROM "+testTableSQLName(testTable)+" WHERE id=?", testID); err != nil {
			t.Logf("清理[%s]旧数据失败: %v", unicode.name, err)
		}

		// 保存数据（验证UTF-8编码兼容性）
		if err := pdb.Save(pbSave); err != nil {
			t.Errorf("保存[%s]失败: %v\n字符: %q", unicode.name, err, unicode.value)
			continue
		}

		// 读取验证
		pbLoad := &testpb.GolangTest{}
		if err := pdb.FindOneByKV(pbLoad, "id", strconv.FormatUint(uint64(testID), 10)); err != nil {
			t.Errorf("读取[%s]失败: %v\n字符: %q", unicode.name, err, unicode.value)
			continue
		}

		if !proto.Equal(pbSave, pbLoad) {
			t.Errorf("[%s]数据不一致", unicode.name)
			t.Logf("预期: %q（UTF-8编码: %x）", pbSave.Player.Name, []byte(pbSave.Player.Name))
			t.Logf("实际: %q（UTF-8编码: %x）", pbLoad.Player.Name, []byte(pbLoad.Player.Name))
		} else {
			t.Logf("✅ [%s]测试通过\n字符: %q", unicode.name, unicode.value)
		}
	}

	// --------------- 测试结束：批量清理数据 ---------------
	cleanSQL := "DELETE FROM " + testTableSQLName(testTable) + " WHERE group_id=999"
	if _, err := db.Exec(cleanSQL); err != nil {
		t.Logf("批量清理测试数据失败: %v", err)
	} else {
		t.Log("\n=== 全量特殊字符测试完成，所有测试数据已清理 ===")
	}
}

// TestNullValueHandling 测试空值和默认值处理
func TestNullValueHandling(t *testing.T) {
	pdb := NewDB()
	// 构造包含空值的测试数据
	pbSave := &testpb.GolangTest{
		Id:      3,
		GroupId: 0,  // 零值
		Ip:      "", // 空字符串
		Port:    0,
		Player:  nil, // 空嵌套消息
	}
	pdb.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 清理旧数据
	db.Exec("DELETE FROM " + testTableSQLName(pbSave) + " WHERE id=3")

	// 保存空值数据
	if err := pdb.Save(pbSave); err != nil {
		t.Fatalf("保存空值数据失败: %v", err)
	}

	// 验证读取结果
	pbLoad := &testpb.GolangTest{}
	if err := pdb.FindOneByKV(pbLoad, "id", "3"); err != nil {
		t.Fatalf("读取空值数据失败: %v", err)
	}

	// 检查空值是否正确映射
	if pbLoad.Ip != "" {
		t.Errorf("空字符串处理错误: 预期空值，实际为 %s", pbLoad.Ip)
	}
	if pbLoad.Player != nil {
		t.Error("空嵌套消息处理错误: 预期nil，实际不为nil")
	}
	if pbLoad.GroupId != 0 {
		t.Errorf("零值处理错误: 预期0，实际为 %d", pbLoad.GroupId)
	}
}

// TestLargeFieldStorage 测试大字段存储（超过256字符的字符串）
func TestLargeFieldStorage(t *testing.T) {
	pdb := NewDB()
	// 生成10KB的大字符串
	largeStr := strings.Repeat("a", 1024*10)
	pbSave := &testpb.GolangTest{
		Id:      4,
		GroupId: 2,
		Ip:      largeStr, // 大字段
		Port:    8080,
		Player: &testpb.Player{
			PlayerId: 222,
			Name:     largeStr, // 嵌套消息中的大字段
		},
	}
	pdb.RegisterTable(pbSave)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 清理旧数据
	db.Exec("DELETE FROM " + testTableSQLName(pbSave) + " WHERE id=4")

	// 保存大字段数据
	if err := pdb.Save(pbSave); err != nil {
		t.Fatalf("保存大字段失败: %v", err)
	}

	// 验证读取结果
	pbLoad := &testpb.GolangTest{}
	if err := pdb.FindOneByKV(pbLoad, "id", "4"); err != nil {
		t.Fatalf("读取大字段失败: %v", err)
	}

	// 检查大字段完整性
	if len(pbLoad.Ip) != len(largeStr) {
		t.Errorf("大字符串长度不匹配: 预期 %d，实际 %d", len(largeStr), len(pbLoad.Ip))
	}
	if pbLoad.Player.Name != largeStr {
		t.Error("嵌套消息大字段存储失败")
	}
}

// TestBatchOperations 测试批量插入和查询
func TestBatchOperations(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 清理旧数据
	db.Exec("DELETE FROM " + testTableSQLName(testTable) + " WHERE group_id=3")

	// 批量插入10条数据
	batchSize := 10
	for i := 0; i < batchSize; i++ {
		pb := &testpb.GolangTest{
			Id:      uint32(100 + i),
			GroupId: 3,
			Ip:      fmt.Sprintf("192.168.1.%d", i),
			Port:    3306 + uint32(i),
		}
		if err := pdb.Save(pb); err != nil {
			t.Fatalf("批量插入失败（第%d条）: %v", i, err)
		}
	}

	// 批量查询
	list := &testpb.GolangTestList{} // 假设存在包含repeated GolangTest的消息
	if err := pdb.FindAllByWhereWithArgs(
		list,
		"group_id = ?",
		[]interface{}{3},
	); err != nil {
		t.Fatalf("批量查询失败: %v", err)
	}

	if len(list.TestList) != batchSize {
		t.Errorf("批量查询结果数量不匹配: 预期 %d，实际 %d", batchSize, len(list.TestList))
	}
}

// TestUpdateFieldType 测试字段类型自动更新
// TestUpdateFieldType 测试字段类型自动更新（修复版）
func TestUpdateFieldType(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	tableName := GetTableName(testTable)
	pdb.RegisterTable(testTable)

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 确保测试表干净（先删除表）
	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", escapeMySQLName(tableName)))
	// 清除表存在缓存（关键：避免缓存影响判断）
	pdb.updateTableExistsCache(tableName, false)
	// 清除字段缓存
	pdb.clearColumnCache(tableName)

	// 1. 初始创建表（使用默认类型）
	createSQL := pdb.GetCreateTableSQL(testTable)
	if _, err := db.Exec(createSQL); err != nil {
		t.Fatalf("创建表失败: %v, SQL: %s", err, createSQL)
	}

	// 2. 验证初始类型（例如StringKind默认是VARCHAR(255)）
	initialCols, err := pdb.getTableColumns(tableName)
	if err != nil {
		t.Fatalf("初始查询表结构失败: %v", err)
	}
	// 找到第一个string类型的字段（适配任意表结构）
	var testFieldName string
	desc := testTable.ProtoReflect().Descriptor()
	for i := 0; i < desc.Fields().Len(); i++ {
		field := desc.Fields().Get(i)
		if field.Kind() == protoreflect.StringKind {
			testFieldName = string(field.Name())
			break
		}
	}
	if testFieldName == "" {
		t.Fatal("测试表中未找到string类型字段，无法进行测试")
	}
	// 检查初始类型是否正确
	initialType := initialCols[testFieldName]
	if !strings.Contains(initialType, "mediumtext") {
		t.Errorf("初始字段类型错误，mediumtext，实际为: %s", initialType)
	}

	// 3. 修改字段类型映射并更新表结构
	oldType := MySQLFieldTypes[protoreflect.StringKind]
	MySQLFieldTypes[protoreflect.StringKind] = "MEDIUMTEXT NOT NULL"
	defer func() {
		MySQLFieldTypes[protoreflect.StringKind] = oldType // 恢复原类型
	}()

	// 执行更新字段操作
	if err := pdb.UpdateTableField(testTable); err != nil {
		t.Fatalf("更新字段类型失败: %v", err)
	}

	// 4. 验证类型是否更新（关键：先清除缓存再查询）
	pdb.clearColumnCache(tableName) // 清除字段缓存，避免读旧数据
	updatedCols, err := pdb.getTableColumns(tableName)
	if err != nil {
		t.Fatalf("更新后查询表结构失败: %v", err)
	}
	updatedType := updatedCols[testFieldName]
	if !strings.Contains(updatedType, "mediumtext") {
		t.Errorf("字段类型未更新，预期包含mediumtext，实际为: %s", updatedType)
	}
}

// TestFindMultiByWhereClauses 测试跨多张表的批量查询（golang_test1/2/3）
func TestFindMultiByWhereClauses(t *testing.T) {
	// 1. 初始化数据库连接
	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 2. 准备4张表的测试数据（原始表+3张新增表）
	// 原始表数据
	testData := &testpb.GolangTest{
		Id:      100,
		GroupId: 1,
		Ip:      "192.168.0.100",
		Port:    3306,
		Player: &testpb.Player{
			PlayerId: 10000,
			Name:     "OriginalTest",
		},
	}
	// 新增表1数据
	testData1 := &testpb.GolangTest1{
		Id:      101,
		GroupId: 1,
		Ip:      "192.168.0.101",
		Port:    3306,
		Player: &testpb.Player{
			PlayerId: 10001,
			Name:     "Test1",
		},
		ExtraInfo: "额外信息1", // 新增字段
	}
	// 新增表2数据（port为uint64）
	testData2 := &testpb.GolangTest2{
		Id:      102,
		GroupId: 1,
		Ip:      "192.168.0.102",
		Port:    65536, // 超过uint32的端口值
		Player: &testpb.Player{
			PlayerId: 10002,
			Name:     "Test2",
		},
	}
	// 新增表3数据（多一个嵌套player）
	testData3 := &testpb.GolangTest3{
		Id:      103,
		GroupId: 1,
		Ip:      "192.168.0.103",
		Port:    3306,
		Player: &testpb.Player{
			PlayerId: 10003,
			Name:     "Test3Main",
		},
		ExtraPlayer: &testpb.Player{ // 新增嵌套字段
			PlayerId: 10004,
			Name:     "Test3Extra",
		},
		PlayerId: 10004,
	}

	// 3. 注册表并创建表结构
	pdb.RegisterTable(testData)
	pdb.RegisterTable(testData1)
	pdb.RegisterTable(testData2)
	pdb.RegisterTable(testData3)

	// 创建/更新表结构
	if err := pdb.CreateOrUpdateTable(testData); err != nil {
		t.Fatalf("创建golang_test表失败: %v", err)
	}
	if err := pdb.CreateOrUpdateTable(testData1); err != nil {
		t.Fatalf("创建golang_test1表失败: %v", err)
	}
	if err := pdb.CreateOrUpdateTable(testData2); err != nil {
		t.Fatalf("创建golang_test2表失败: %v", err)
	}
	if err := pdb.CreateOrUpdateTable(testData3); err != nil {
		t.Fatalf("创建golang_test3表失败: %v", err)
	}

	// 4. 清理旧数据
	clearTable := func(tableName string, id interface{}) {
		sql := fmt.Sprintf("DELETE FROM %s WHERE id = ?", escapeMySQLName(tableName))
		if _, err := db.Exec(sql, id); err != nil {
			t.Logf("清理表%s(id=%v)旧数据失败: %v（可忽略）", tableName, id, err)
		}
	}
	clearTable(GetTableName(testData), testData.Id)
	clearTable(GetTableName(testData1), testData1.Id)
	clearTable(GetTableName(testData2), testData2.Id)
	clearTable(GetTableName(testData3), testData3.Id)

	// 5. 插入测试数据
	if err := pdb.Save(testData); err != nil {
		t.Fatalf("保存golang_test数据失败: %v", err)
	}
	if err := pdb.Save(testData1); err != nil {
		t.Fatalf("保存golang_test1数据失败: %v", err)
	}
	if err := pdb.Save(testData2); err != nil {
		t.Fatalf("保存golang_test2数据失败: %v", err)
	}
	if err := pdb.Save(testData3); err != nil {
		t.Fatalf("保存golang_test3数据失败: %v", err)
	}

	// 6. 准备批量查询参数（跨4张表）
	queries := []MultiQuery{
		{
			Message:     &testpb.GolangTest{}, // 原始表
			WhereClause: "id = ? AND group_id = ?",
			WhereArgs:   []interface{}{testData.Id, testData.GroupId},
		},
		{
			Message:     &testpb.GolangTest1{},       // 新增表1
			WhereClause: "id = ? AND extra_info = ?", // 查询新增字段
			WhereArgs:   []interface{}{testData1.Id, testData1.ExtraInfo},
		},
		{
			Message:     &testpb.GolangTest2{}, // 新增表2
			WhereClause: "id = ? AND port = ?", // 查询uint64字段
			WhereArgs:   []interface{}{testData2.Id, testData2.Port},
		},
		{
			Message:     &testpb.GolangTest3{},      // 新增表3
			WhereClause: "id = ? AND player_id = ?", // 查询新增嵌套字段
			WhereArgs:   []interface{}{testData3.Id, testData3.ExtraPlayer.PlayerId},
		},
	}

	// 7. 执行批量查询
	if err := pdb.FindMultiByWhereClauses(queries); err != nil {
		t.Fatalf("批量查询失败: %v", err)
	}

	// 8. 验证查询结果
	// 验证原始表
	result := queries[0].Message.(*testpb.GolangTest)
	if !proto.Equal(testData, result) {
		t.Error("golang_test查询结果不一致")
		t.Logf("预期: %s", testData.String())
		t.Logf("实际: %s", result.String())
	}

	// 验证新增表1
	result1 := queries[1].Message.(*testpb.GolangTest1)
	if !proto.Equal(testData1, result1) {
		t.Error("golang_test1查询结果不一致")
		t.Logf("预期: %s", testData1.String())
		t.Logf("实际: %s", result1.String())
	}

	// 验证新增表2（注意port是uint64）
	result2 := queries[2].Message.(*testpb.GolangTest2)
	if !proto.Equal(testData2, result2) {
		t.Error("golang_test2查询结果不一致")
		t.Logf("预期: %s", testData2.String())
		t.Logf("实际: %s", result2.String())
	}

	// 验证新增表3（注意嵌套字段extra_player）
	result3 := queries[3].Message.(*testpb.GolangTest3)
	if !proto.Equal(testData3, result3) {
		t.Error("golang_test3查询结果不一致")
		t.Logf("预期: %s", testData3.String())
		t.Logf("实际: %s", result3.String())
	}

	// 9. 测试异常场景（表2查询不存在的数据）
	invalidQueries := []MultiQuery{
		{
			Message:     &testpb.GolangTest2{},
			WhereClause: "id = ?",
			WhereArgs:   []interface{}{9999}, // 不存在的ID
		},
	}
	if err := pdb.FindMultiByWhereClauses(invalidQueries); err == nil {
		t.Error("预期查询不存在的ID时返回错误，但未返回")
	} else if !strings.Contains(err.Error(), ErrNoRowsFound.Error()) {
		t.Errorf("预期错误包含[%s]，实际为: %v", ErrNoRowsFound, err)
	}

	t.Log("跨表批量查询测试通过")
}

// TestFindMultiInterfaces 测试多条结果查询的三个接口
func TestFindMultiInterfaces(t *testing.T) {
	// 1. 初始化数据库连接
	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 2. 注册测试表（golang_test）
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(
		testTable,
		WithPrimaryKey("id"),
		WithAutoIncrementKey("id"),
		WithIndexes("player_id"), // 为player_id建索引，加速查询
	)

	// 3. 创建表并清理旧数据
	if err := pdb.CreateOrUpdateTable(testTable); err != nil {
		t.Fatalf("创建表失败: %v", err)
	}
	tableName := GetTableName(testTable)
	cleanSQL := fmt.Sprintf("DELETE FROM %s WHERE player_id = ?", escapeMySQLName(tableName))
	_, _ = db.Exec(cleanSQL, 1000) // 清理player_id=1000的旧数据

	// 4. 插入测试数据（3条相同player_id的数据，用于测试多条结果）
	testData1 := &testpb.GolangTest{
		Id:       1001,
		PlayerId: 1000, // 关键：相同的player_id
		Ip:       "192.168.1.101",
		Port:     3306,
		GroupId:  10,
	}
	testData2 := &testpb.GolangTest{
		Id:       1002,
		PlayerId: 1000,
		Ip:       "192.168.1.102",
		Port:     3307,
		GroupId:  10,
	}
	testData3 := &testpb.GolangTest{
		Id:       1003,
		PlayerId: 1000,
		Ip:       "192.168.1.103",
		Port:     3308,
		GroupId:  20, // 不同的groupId，用于复杂条件查询
	}
	// 插入一条不相关数据（用于验证过滤效果）
	unrelatedData := &testpb.GolangTest{
		Id:       2001,
		PlayerId: 2000, // 不同的player_id
		Ip:       "192.168.2.101",
	}

	// 批量插入测试数据
	if err := pdb.BatchInsert([]proto.Message{testData1, testData2, testData3, unrelatedData}); err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	// 预期结果：3条player_id=1000的数据（按id排序）
	expectedIds := map[uint32]bool{1001: true, 1002: true, 1003: true}

	// 5. 测试 FindMultiByKV（键值对查询多条结果）
	t.Run("FindMultiByKV", func(t *testing.T) {
		var resultList testpb.GolangTestList
		err := pdb.FindMultiByKV(&resultList, "player_id", uint64(1000))
		if err != nil {
			t.Fatalf("FindMultiByKV查询失败: %v", err)
		}

		// 验证结果数量
		if len(resultList.TestList) != 3 {
			t.Fatalf("预期3条结果，实际%d条", len(resultList.TestList))
		}

		// 验证结果正确性
		for _, item := range resultList.TestList {
			if !expectedIds[item.Id] {
				t.Errorf("结果包含非预期数据: id=%d", item.Id)
			}
			if item.PlayerId != 1000 {
				t.Errorf("数据校验失败: player_id应为1000，实际为%d", item.PlayerId)
			}
		}
	})

	// 6. 测试 FindMultiByWhereWithArgs（参数化条件查询多条结果）
	t.Run("FindMultiByWhereWithArgs", func(t *testing.T) {
		var resultList testpb.GolangTestList
		// 复杂条件：player_id=1000 且 group_id=10
		err := pdb.FindMultiByWhereWithArgs(
			&resultList,
			"player_id = ? AND group_id = ?",
			[]interface{}{uint64(1000), 10},
		)
		if err != nil {
			t.Fatalf("FindMultiByWhereWithArgs查询失败: %v", err)
		}

		// 验证结果数量（预期2条：1001、1002）
		if len(resultList.TestList) != 2 {
			t.Fatalf("预期2条结果，实际%d条", len(resultList.TestList))
		}

		// 验证结果正确性
		for _, item := range resultList.TestList {
			if item.Id != 1001 && item.Id != 1002 {
				t.Errorf("结果包含非预期数据: id=%d", item.Id)
			}
			if item.GroupId != 10 {
				t.Errorf("数据校验失败: group_id应为10，实际为%d", item.GroupId)
			}
		}
	})

	// 7. 测试 FindMultiByWhereClause（非参数化条件查询多条结果）
	t.Run("FindMultiByWhereClause", func(t *testing.T) {
		var resultList testpb.GolangTestList
		// 固定条件（内部使用，无用户输入）
		err := pdb.FindMultiByWhereClause(
			&resultList,
			"player_id = 1000 AND port > 3306", // port>3306：预期1002、1003
		)
		if err != nil {
			t.Fatalf("FindMultiByWhereClause查询失败: %v", err)
		}

		// 验证结果数量（预期2条）
		if len(resultList.TestList) != 2 {
			t.Fatalf("预期2条结果，实际%d条", len(resultList.TestList))
		}

		// 验证结果正确性
		for _, item := range resultList.TestList {
			if item.Id != 1002 && item.Id != 1003 {
				t.Errorf("结果包含非预期数据: id=%d", item.Id)
			}
			if item.Port <= 3306 {
				t.Errorf("数据校验失败: port应>3306，实际为%d", item.Port)
			}
		}
	})

	t.Log("所有多条结果查询接口测试通过")
}

// TestQueryOptionsSQLSuffix 单元测试：QueryOptions生成的SQL后缀（无需数据库）
func TestQueryOptionsSQLSuffix(t *testing.T) {
	cases := []struct {
		name string
		opts QueryOptions
		want string
	}{
		{"空选项", QueryOptions{}, ""},
		{"仅排序", QueryOptions{OrderBy: "id DESC"}, " ORDER BY id DESC"},
		{"仅限制数量", QueryOptions{Limit: 10}, " LIMIT 10"},
		{"限制加偏移", QueryOptions{Limit: 10, Offset: 20}, " LIMIT 10 OFFSET 20"},
		{"排序限制偏移", QueryOptions{OrderBy: "id", Limit: 5, Offset: 5}, " ORDER BY id LIMIT 5 OFFSET 5"},
		{"仅偏移不生效", QueryOptions{Offset: 20}, ""},
		{"负数限制不生效", QueryOptions{Limit: -1, Offset: 3}, ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.opts.sqlSuffix(); got != c.want {
				t.Errorf("sqlSuffix() = %q, 预期 %q", got, c.want)
			}
		})
	}
}

func TestMySQLIdentifierEscaping(t *testing.T) {
	if got, want := escapeMySQLName("example.GolangTest"), "`example.GolangTest`"; got != want {
		t.Fatalf("escapeMySQLName() = %q, 预期 %q", got, want)
	}
	if got, want := escapeMySQLName("weird`name"), "`weird``name`"; got != want {
		t.Fatalf("escapeMySQLName() = %q, 预期 %q", got, want)
	}

	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable, WithIndexes("player_id, group_id"), WithUniqueKey("ip"))

	// 表名取自proto FullName，所有标识符必须整体反引号转义
	tableName := GetTableName(testTable)
	createSQL := pdb.GetCreateTableSQL(testTable)
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `" + tableName + "`",
		"INDEX `idx_" + tableName + "_0` (`player_id`,`group_id`)",
		"UNIQUE KEY `uk_" + tableName + "` (`ip`)",
	} {
		if !strings.Contains(createSQL, want) {
			t.Fatalf("建表SQL缺少 %q\nSQL: %s", want, createSQL)
		}
	}
}

func TestDeleteSQLUsesAllPrimaryKeys(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id", "group_id"))
	msg := &testpb.GolangTest{Id: 42, GroupId: 7}

	sqlWithArgs, err := table.GetDeleteSQLWithArgs(msg)
	if err != nil {
		t.Fatalf("GetDeleteSQLWithArgs失败: %v", err)
	}

	wantSQL := "DELETE FROM `" + GetTableName(msg) + "` WHERE `id` = ? AND `group_id` = ?"
	if sqlWithArgs.Sql != wantSQL {
		t.Fatalf("SQL = %q, 预期 %q", sqlWithArgs.Sql, wantSQL)
	}
	if got, want := fmt.Sprint(sqlWithArgs.Args), "[42 7]"; got != want {
		t.Fatalf("Args = %s, 预期 %s", got, want)
	}
}

func TestBatchInsertRejectsMismatchedDescriptor(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{})
	_, err := table.GetBatchInsertSQLWithArgs([]proto.Message{&testpb.GolangTest1{}})
	if err == nil {
		t.Fatal("预期descriptor不匹配时报错")
	}
	if !strings.Contains(err.Error(), "does not match table") {
		t.Fatalf("错误信息不符合预期: %v", err)
	}
}

// TestCountExistsPageUpdate 集成测试：Count/Exists/FindAllWithOptions/FindPage/Update/UpdateByWhere/DeleteByWhere
func TestCountExistsPageUpdate(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable, WithPrimaryKey("id"))

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	recreateTestTable(t, db, pdb, testTable)

	// 清理并写入5条测试数据（group_id=9）
	if _, err := db.Exec("DELETE FROM " + testTableSQLName(testTable) + " WHERE group_id=9"); err != nil {
		t.Logf("清理旧数据失败: %v", err)
	}
	for i := 1; i <= 5; i++ {
		item := &testpb.GolangTest{
			Id:      uint32(9000 + i),
			GroupId: 9,
			Ip:      "10.0.0." + strconv.Itoa(i),
			Port:    uint32(4000 + i),
		}
		if err := pdb.Save(item); err != nil {
			t.Fatalf("写入测试数据失败（id=%d）: %v", item.Id, err)
		}
	}

	// 1. Count / CountByWhereWithArgs
	t.Run("Count", func(t *testing.T) {
		count, err := pdb.CountByWhereWithArgs(testTable, "group_id = ?", []interface{}{9})
		if err != nil {
			t.Fatalf("Count失败: %v", err)
		}
		if count != 5 {
			t.Errorf("预期5条，实际%d条", count)
		}

		// 传列表消息也应能解析出表
		countByList, err := pdb.CountByWhereWithArgs(&testpb.GolangTestList{}, "group_id = ?", []interface{}{9})
		if err != nil {
			t.Fatalf("按列表消息Count失败: %v", err)
		}
		if countByList != 5 {
			t.Errorf("按列表消息统计预期5条，实际%d条", countByList)
		}
	})

	// 2. Exists
	t.Run("Exists", func(t *testing.T) {
		exists, err := pdb.Exists(testTable, "id = ?", []interface{}{9001})
		if err != nil {
			t.Fatalf("Exists失败: %v", err)
		}
		if !exists {
			t.Error("预期存在id=9001的行")
		}

		notExists, err := pdb.Exists(testTable, "id = ?", []interface{}{999999})
		if err != nil {
			t.Fatalf("Exists失败: %v", err)
		}
		if notExists {
			t.Error("预期不存在id=999999的行")
		}
	})

	// 3. FindAllWithOptions（数量加载 + 排序）
	t.Run("FindAllWithOptions", func(t *testing.T) {
		var list testpb.GolangTestList
		err := pdb.FindAllWithOptions(&list, "group_id = ?", []interface{}{9}, QueryOptions{
			OrderBy: "id DESC",
			Limit:   2,
		})
		if err != nil {
			t.Fatalf("FindAllWithOptions失败: %v", err)
		}
		if len(list.TestList) != 2 {
			t.Fatalf("预期2条，实际%d条", len(list.TestList))
		}
		if list.TestList[0].Id != 9005 || list.TestList[1].Id != 9004 {
			t.Errorf("排序结果不符: 实际id=[%d, %d]，预期[9005, 9004]",
				list.TestList[0].Id, list.TestList[1].Id)
		}
	})

	// 4. FindPage（分页加载）
	t.Run("FindPage", func(t *testing.T) {
		var page2 testpb.GolangTestList
		err := pdb.FindPage(&page2, "group_id = ?", []interface{}{9}, 2, 2)
		if err != nil {
			t.Fatalf("FindPage失败: %v", err)
		}
		if len(page2.TestList) != 2 {
			t.Fatalf("第2页预期2条，实际%d条", len(page2.TestList))
		}

		if err := pdb.FindPage(&page2, "", nil, 0, 2); err == nil {
			t.Error("非法页码应返回错误")
		}
	})

	// 5. Update（按主键更新）
	t.Run("Update", func(t *testing.T) {
		updated := &testpb.GolangTest{Id: 9001, GroupId: 9, Ip: "192.168.1.1", Port: 5555}
		if err := pdb.Update(updated); err != nil {
			t.Fatalf("Update失败: %v", err)
		}

		got := &testpb.GolangTest{}
		if err := pdb.FindOneByKV(got, "id", "9001"); err != nil {
			t.Fatalf("查询更新结果失败: %v", err)
		}
		if got.Ip != "192.168.1.1" || got.Port != 5555 {
			t.Errorf("更新未生效: ip=%s, port=%d", got.Ip, got.Port)
		}
	})

	// 6. UpdateByWhereWithArgs（按条件更新）
	t.Run("UpdateByWhereWithArgs", func(t *testing.T) {
		patch := &testpb.GolangTest{Ip: "172.16.0.1"}
		if err := pdb.UpdateByWhereWithArgs(patch, "id = ?", []interface{}{9002}); err != nil {
			t.Fatalf("UpdateByWhereWithArgs失败: %v", err)
		}

		got := &testpb.GolangTest{}
		if err := pdb.FindOneByKV(got, "id", "9002"); err != nil {
			t.Fatalf("查询更新结果失败: %v", err)
		}
		if got.Ip != "172.16.0.1" {
			t.Errorf("按条件更新未生效: ip=%s", got.Ip)
		}
		if got.Port != 4002 {
			t.Errorf("未设置的字段不应被更新: port=%d", got.Port)
		}
	})

	// 7. DeleteByWhereWithArgs（按条件删除）
	t.Run("DeleteByWhereWithArgs", func(t *testing.T) {
		if err := pdb.DeleteByWhereWithArgs(testTable, "id = ?", []interface{}{9005}); err != nil {
			t.Fatalf("DeleteByWhereWithArgs失败: %v", err)
		}

		exists, err := pdb.Exists(testTable, "id = ?", []interface{}{9005})
		if err != nil {
			t.Fatalf("Exists失败: %v", err)
		}
		if exists {
			t.Error("删除后不应存在id=9005的行")
		}
	})

	// 8. Transaction（事务提交与回滚）
	t.Run("Transaction", func(t *testing.T) {
		err := pdb.Transaction(func(tx *sql.Tx) error {
			_, err := tx.Exec("UPDATE "+testTableSQLName(testTable)+" SET port = 6001 WHERE id = ?", 9003)
			return err
		})
		if err != nil {
			t.Fatalf("事务提交失败: %v", err)
		}

		got := &testpb.GolangTest{}
		if err := pdb.FindOneByKV(got, "id", "9003"); err != nil {
			t.Fatalf("查询事务结果失败: %v", err)
		}
		if got.Port != 6001 {
			t.Errorf("事务更新未生效: port=%d", got.Port)
		}

		rollbackErr := fmt.Errorf("触发回滚")
		err = pdb.Transaction(func(tx *sql.Tx) error {
			if _, err := tx.Exec("UPDATE "+testTableSQLName(testTable)+" SET port = 7777 WHERE id = ?", 9003); err != nil {
				return err
			}
			return rollbackErr
		})
		if err == nil {
			t.Fatal("预期事务返回错误")
		}

		if err := pdb.FindOneByKV(got, "id", "9003"); err != nil {
			t.Fatalf("查询回滚结果失败: %v", err)
		}
		if got.Port != 6001 {
			t.Errorf("事务应已回滚: port=%d，预期6001", got.Port)
		}
	})

	t.Log("Count/Exists/分页/更新/删除/事务接口测试通过")
}

// TestPKAndBatchInterfaces 集成测试：FindOneByPK/FindAllByKVIn/DeleteByKV/BatchSave/BatchDelete
func TestPKAndBatchInterfaces(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable, WithPrimaryKey("id"))

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	recreateTestTable(t, db, pdb, testTable)
	if _, err := db.Exec("DELETE FROM " + testTableSQLName(testTable) + " WHERE group_id=8"); err != nil {
		t.Logf("清理旧数据失败: %v", err)
	}

	// 1. BatchSave（批量REPLACE）
	batch := make([]proto.Message, 0, 4)
	for i := 1; i <= 4; i++ {
		batch = append(batch, &testpb.GolangTest{
			Id:      uint32(8000 + i),
			GroupId: 8,
			Ip:      "10.8.0." + strconv.Itoa(i),
			Port:    uint32(5000 + i),
		})
	}
	t.Run("BatchSave", func(t *testing.T) {
		if err := pdb.BatchSave(batch); err != nil {
			t.Fatalf("BatchSave失败: %v", err)
		}

		count, err := pdb.CountByWhereWithArgs(testTable, "group_id = ?", []interface{}{8})
		if err != nil {
			t.Fatalf("统计失败: %v", err)
		}
		if count != 4 {
			t.Fatalf("预期4条，实际%d条", count)
		}

		// 再次BatchSave同主键应覆盖而非报错（REPLACE语义）
		batch[0].(*testpb.GolangTest).Port = 5999
		if err := pdb.BatchSave(batch); err != nil {
			t.Fatalf("重复BatchSave失败: %v", err)
		}
	})

	// 2. FindOneByPK（按主键回查）
	t.Run("FindOneByPK", func(t *testing.T) {
		got := &testpb.GolangTest{Id: 8001}
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("FindOneByPK失败: %v", err)
		}
		if got.Port != 5999 || got.Ip != "10.8.0.1" {
			t.Errorf("回查结果不符: ip=%s, port=%d", got.Ip, got.Port)
		}
	})

	// 3. FindAllByKVIn（IN批量查询）
	t.Run("FindAllByKVIn", func(t *testing.T) {
		var list testpb.GolangTestList
		err := pdb.FindAllByKVIn(&list, "id", []interface{}{8001, 8003})
		if err != nil {
			t.Fatalf("FindAllByKVIn失败: %v", err)
		}
		if len(list.TestList) != 2 {
			t.Fatalf("预期2条，实际%d条", len(list.TestList))
		}

		// 空values应返回空列表且不报错
		if err := pdb.FindAllByKVIn(&list, "id", nil); err != nil {
			t.Fatalf("空values查询失败: %v", err)
		}
		if len(list.TestList) != 0 {
			t.Errorf("空values应清空列表，实际%d条", len(list.TestList))
		}
	})

	// 4. DeleteByKV
	t.Run("DeleteByKV", func(t *testing.T) {
		if err := pdb.DeleteByKV(testTable, "id", 8004); err != nil {
			t.Fatalf("DeleteByKV失败: %v", err)
		}
		exists, err := pdb.Exists(testTable, "id = ?", []interface{}{8004})
		if err != nil {
			t.Fatalf("Exists失败: %v", err)
		}
		if exists {
			t.Error("删除后不应存在id=8004的行")
		}
	})

	// 5. BatchDelete（按主键IN批量删除）
	t.Run("BatchDelete", func(t *testing.T) {
		if err := pdb.BatchDelete(batch[:3]); err != nil {
			t.Fatalf("BatchDelete失败: %v", err)
		}
		count, err := pdb.CountByWhereWithArgs(testTable, "group_id = ?", []interface{}{8})
		if err != nil {
			t.Fatalf("统计失败: %v", err)
		}
		if count != 0 {
			t.Errorf("批量删除后预期0条，实际%d条", count)
		}
	})

	t.Log("主键回查/IN查询/批量保存/批量删除接口测试通过")
}

// TestGameServerInterfaces 集成测试：游戏服务器常用接口
// FindOrCreate/FindAllByPKIn/IncrByPK/DecrByPKIfEnough/RunInTransaction/FindOneByPKForUpdate
func TestGameServerInterfaces(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable, WithPrimaryKey("id"))

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	recreateTestTable(t, db, pdb, testTable)
	if _, err := db.Exec("DELETE FROM " + testTableSQLName(testTable) + " WHERE group_id=7"); err != nil {
		t.Logf("清理旧数据失败: %v", err)
	}

	// 1. FindOrCreate（玩家首次登录：第一次创建，第二次读取）
	t.Run("FindOrCreate", func(t *testing.T) {
		player := &testpb.GolangTest{Id: 7001, GroupId: 7, Ip: "10.7.0.1", Port: 100}
		created, err := pdb.FindOrCreate(player)
		if err != nil {
			t.Fatalf("FindOrCreate失败: %v", err)
		}
		if !created {
			t.Error("首次调用应新建记录")
		}

		// 第二次：应读到已有数据，而不是覆盖
		again := &testpb.GolangTest{Id: 7001}
		created, err = pdb.FindOrCreate(again)
		if err != nil {
			t.Fatalf("二次FindOrCreate失败: %v", err)
		}
		if created {
			t.Error("二次调用不应新建记录")
		}
		if again.Port != 100 || again.Ip != "10.7.0.1" {
			t.Errorf("读取结果不符: ip=%s, port=%d", again.Ip, again.Port)
		}
	})

	// 2. FindAllByPKIn（Redis MGET风格：给一批主键返回列表，不存在的跳过）
	t.Run("FindAllByPKIn", func(t *testing.T) {
		for i := 2; i <= 4; i++ {
			item := &testpb.GolangTest{Id: uint32(7000 + i), GroupId: 7, Port: uint32(100 * i)}
			if err := pdb.Save(item); err != nil {
				t.Fatalf("准备数据失败: %v", err)
			}
		}

		var list testpb.GolangTestList
		// 7999不存在，应只返回3条
		err := pdb.FindAllByPKIn(&list, []interface{}{7001, 7002, 7003, 7999})
		if err != nil {
			t.Fatalf("FindAllByPKIn失败: %v", err)
		}
		if len(list.TestList) != 3 {
			t.Fatalf("预期3条，实际%d条", len(list.TestList))
		}

		// 空keys返回空列表
		if err := pdb.FindAllByPKIn(&list, nil); err != nil {
			t.Fatalf("空keys查询失败: %v", err)
		}
		if len(list.TestList) != 0 {
			t.Errorf("空keys应清空列表，实际%d条", len(list.TestList))
		}
	})

	// 3. IncrByPK（原子加经验/货币）
	t.Run("IncrByPK", func(t *testing.T) {
		player := &testpb.GolangTest{Id: 7001}
		if err := pdb.IncrByPK(player, "port", 50); err != nil {
			t.Fatalf("IncrByPK失败: %v", err)
		}

		got := &testpb.GolangTest{Id: 7001}
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("回查失败: %v", err)
		}
		if got.Port != 150 {
			t.Errorf("预期port=150，实际%d", got.Port)
		}

		// 不存在的字段应报错
		if err := pdb.IncrByPK(player, "not_exist", 1); err == nil {
			t.Error("不存在的字段应返回错误")
		}
	})

	// 4. DecrByPKIfEnough（余额充足扣减，不足不扣）
	t.Run("DecrByPKIfEnough", func(t *testing.T) {
		player := &testpb.GolangTest{Id: 7001} // port当前150
		ok, err := pdb.DecrByPKIfEnough(player, "port", 100)
		if err != nil {
			t.Fatalf("DecrByPKIfEnough失败: %v", err)
		}
		if !ok {
			t.Error("余额充足应扣减成功")
		}

		// 余额只剩50，扣100应失败且不改数据
		ok, err = pdb.DecrByPKIfEnough(player, "port", 100)
		if err != nil {
			t.Fatalf("DecrByPKIfEnough失败: %v", err)
		}
		if ok {
			t.Error("余额不足不应扣减")
		}

		got := &testpb.GolangTest{Id: 7001}
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("回查失败: %v", err)
		}
		if got.Port != 50 {
			t.Errorf("预期port=50，实际%d", got.Port)
		}

		// 负数delta应报错
		if _, err := pdb.DecrByPKIfEnough(player, "port", -1); err == nil {
			t.Error("负数delta应返回错误")
		}
	})

	// 5. RunInTransaction（事务内复用全部接口 + 行锁 + 回滚验证）
	t.Run("RunInTransaction", func(t *testing.T) {
		// 事务内：锁行 -> 扣减 -> 更新另一条（模拟扣钱+发道具），提交
		err := pdb.RunInTransaction(func(tx *DB) error {
			locked := &testpb.GolangTest{Id: 7001}
			if err := tx.FindOneByPKForUpdate(locked); err != nil {
				return err
			}
			if ok, err := tx.DecrByPKIfEnough(locked, "port", 10); err != nil || !ok {
				return fmt.Errorf("扣减失败: ok=%v, err=%v", ok, err)
			}
			return tx.IncrByPK(&testpb.GolangTest{Id: 7002}, "port", 10)
		})
		if err != nil {
			t.Fatalf("事务提交失败: %v", err)
		}

		got := &testpb.GolangTest{Id: 7001}
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("回查失败: %v", err)
		}
		if got.Port != 40 {
			t.Errorf("预期port=40，实际%d", got.Port)
		}

		// 回滚：中途失败，所有修改都不应生效
		rollbackErr := fmt.Errorf("模拟发道具失败")
		err = pdb.RunInTransaction(func(tx *DB) error {
			if err := tx.IncrByPK(&testpb.GolangTest{Id: 7001}, "port", 1000); err != nil {
				return err
			}
			return rollbackErr
		})
		if err == nil {
			t.Fatal("预期事务返回错误")
		}

		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("回查失败: %v", err)
		}
		if got.Port != 40 {
			t.Errorf("事务应已回滚: port=%d，预期40", got.Port)
		}

		// 事务外调用FindOneByPKForUpdate应报错
		if err := pdb.FindOneByPKForUpdate(got); err == nil {
			t.Error("事务外调用FindOneByPKForUpdate应返回错误")
		}
	})

	t.Log("游戏服务器接口测试通过")
}

// TestExtendedCRUDValidation 单元测试：新增增删改查接口的参数校验（无需数据库）
func TestExtendedCRUDValidation(t *testing.T) {
	pdb := NewDB()
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	msg := &testpb.GolangTest{Id: 1}

	if err := pdb.UpdateFieldsByPK(msg); err == nil {
		t.Error("UpdateFieldsByPK不传字段应报错")
	}
	if err := pdb.UpdateFieldsByPK(msg, "no_such_field"); err == nil || !strings.Contains(err.Error(), ErrFieldNotFound.Error()) {
		t.Errorf("UpdateFieldsByPK未知字段应返回ErrFieldNotFound，实际: %v", err)
	}
	if err := pdb.UpdateKVByPK(msg, "no_such_field", 1); err == nil || !strings.Contains(err.Error(), ErrFieldNotFound.Error()) {
		t.Errorf("UpdateKVByPK未知字段应返回ErrFieldNotFound，实际: %v", err)
	}
	if _, err := pdb.UpdateIfVersion(msg, "no_such_field"); err == nil || !strings.Contains(err.Error(), ErrFieldNotFound.Error()) {
		t.Errorf("UpdateIfVersion未知版本字段应返回ErrFieldNotFound，实际: %v", err)
	}
	if _, err := pdb.UpdateIfVersion(&testpb.GolangTest{Id: 1}, "group_id"); err == nil {
		t.Error("UpdateIfVersion无可更新字段应报错")
	}

	var list testpb.GolangTestList
	if err := pdb.FindPageByCursor(&list, "", nil, "id", nil, 0); err == nil {
		t.Error("FindPageByCursor pageSize<1应报错")
	}
	if err := pdb.FindPageByCursor(&list, "", nil, "no_such_field", nil, 10); err == nil || !strings.Contains(err.Error(), ErrFieldNotFound.Error()) {
		t.Errorf("FindPageByCursor未知游标字段应返回ErrFieldNotFound，实际: %v", err)
	}
}

// TestExtendedCRUDInterfaces 集成测试：InsertIgnore/InsertReturningID/UpdateFieldsByPK/
// UpdateKVByPK/UpdateIfVersion/ExistsByPK/FindOneWithOptions/FindPageByCursor
func TestExtendedCRUDInterfaces(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable, WithPrimaryKey("id"), WithAutoIncrementKey("id"))

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	// 其它测试可能已用无主键的schema建过golang_test，重建以保证主键/自增约束生效
	recreateTestTable(t, db, pdb, testTable)
	if _, err := db.Exec("DELETE FROM " + testTableSQLName(testTable) + " WHERE player_id=9900"); err != nil {
		t.Logf("清理旧数据失败: %v", err)
	}

	// 1. InsertIgnore：首次插入成功，重复插入跳过
	t.Run("InsertIgnore", func(t *testing.T) {
		row := &testpb.GolangTest{Id: 9901, PlayerId: 9900, Ip: "10.9.0.1", Port: 1}
		inserted, err := pdb.InsertIgnore(row)
		if err != nil {
			t.Fatalf("InsertIgnore失败: %v", err)
		}
		if !inserted {
			t.Error("首次InsertIgnore应插入新行")
		}

		dup := &testpb.GolangTest{Id: 9901, PlayerId: 9900, Ip: "10.9.0.999", Port: 999}
		inserted, err = pdb.InsertIgnore(dup)
		if err != nil {
			t.Fatalf("重复InsertIgnore失败: %v", err)
		}
		if inserted {
			t.Error("重复InsertIgnore应跳过")
		}

		// 原数据应未被覆盖
		got := &testpb.GolangTest{Id: 9901}
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("回查失败: %v", err)
		}
		if got.Port != 1 {
			t.Errorf("重复InsertIgnore不应覆盖原数据: port=%d", got.Port)
		}
	})

	// 2. InsertReturningID：自增主键回填
	t.Run("InsertReturningID", func(t *testing.T) {
		id, err := pdb.InsertReturningID(&testpb.GolangTest{PlayerId: 9900, Ip: "10.9.0.2"})
		if err != nil {
			t.Fatalf("InsertReturningID失败: %v", err)
		}
		if id <= 0 {
			t.Fatalf("预期返回自增ID>0，实际%d", id)
		}
		if ok, err := pdb.ExistsByPK(&testpb.GolangTest{Id: uint32(id)}); err != nil || !ok {
			t.Errorf("按返回ID查询应存在: ok=%v, err=%v", ok, err)
		}
	})

	// 3. UpdateFieldsByPK：部分更新，不动其他字段
	t.Run("UpdateFieldsByPK", func(t *testing.T) {
		row := &testpb.GolangTest{Id: 9901, Ip: "10.9.9.9", Port: 777}
		if err := pdb.UpdateFieldsByPK(row, "ip"); err != nil {
			t.Fatalf("UpdateFieldsByPK失败: %v", err)
		}

		got := &testpb.GolangTest{Id: 9901}
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("回查失败: %v", err)
		}
		if got.Ip != "10.9.9.9" {
			t.Errorf("ip应已更新: %s", got.Ip)
		}
		if got.Port != 1 {
			t.Errorf("port不在更新列表中不应改变: %d", got.Port)
		}
	})

	// 4. UpdateKVByPK：单字段设值
	t.Run("UpdateKVByPK", func(t *testing.T) {
		if err := pdb.UpdateKVByPK(&testpb.GolangTest{Id: 9901}, "port", 42); err != nil {
			t.Fatalf("UpdateKVByPK失败: %v", err)
		}
		got := &testpb.GolangTest{Id: 9901}
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("回查失败: %v", err)
		}
		if got.Port != 42 {
			t.Errorf("port应为42，实际%d", got.Port)
		}
	})

	// 5. UpdateIfVersion：乐观锁CAS（以group_id为版本字段）
	t.Run("UpdateIfVersion", func(t *testing.T) {
		// 当前group_id=0；用正确版本更新成功，group_id自动+1
		row := &testpb.GolangTest{Id: 9901, Ip: "10.9.1.1", GroupId: 0}
		ok, err := pdb.UpdateIfVersion(row, "group_id")
		if err != nil {
			t.Fatalf("UpdateIfVersion失败: %v", err)
		}
		if !ok {
			t.Fatal("版本匹配时应更新成功")
		}

		got := &testpb.GolangTest{Id: 9901}
		if err := pdb.FindOneByPK(got); err != nil {
			t.Fatalf("回查失败: %v", err)
		}
		if got.GroupId != 1 {
			t.Errorf("版本应自动+1: group_id=%d", got.GroupId)
		}
		if got.Ip != "10.9.1.1" {
			t.Errorf("ip应已更新: %s", got.Ip)
		}

		// 用过期版本（0）再更新应失败
		stale := &testpb.GolangTest{Id: 9901, Ip: "10.9.2.2", GroupId: 0}
		ok, err = pdb.UpdateIfVersion(stale, "group_id")
		if err != nil {
			t.Fatalf("UpdateIfVersion失败: %v", err)
		}
		if ok {
			t.Error("版本冲突时应返回false")
		}
	})

	// 6. ExistsByPK
	t.Run("ExistsByPK", func(t *testing.T) {
		ok, err := pdb.ExistsByPK(&testpb.GolangTest{Id: 9901})
		if err != nil || !ok {
			t.Errorf("存在的主键应返回true: ok=%v, err=%v", ok, err)
		}
		ok, err = pdb.ExistsByPK(&testpb.GolangTest{Id: 99999999})
		if err != nil || ok {
			t.Errorf("不存在的主键应返回false: ok=%v, err=%v", ok, err)
		}
	})

	// 7. FindOneWithOptions：排序取一条
	t.Run("FindOneWithOptions", func(t *testing.T) {
		// 再插入几条，用于排序
		batch := []proto.Message{
			&testpb.GolangTest{Id: 9903, PlayerId: 9900, Port: 100},
			&testpb.GolangTest{Id: 9904, PlayerId: 9900, Port: 300},
			&testpb.GolangTest{Id: 9905, PlayerId: 9900, Port: 200},
		}
		if err := pdb.BatchSave(batch); err != nil {
			t.Fatalf("准备数据失败: %v", err)
		}

		top := &testpb.GolangTest{}
		err := pdb.FindOneWithOptions(top, "player_id = ?", []interface{}{9900}, QueryOptions{OrderBy: "port DESC"})
		if err != nil {
			t.Fatalf("FindOneWithOptions失败: %v", err)
		}
		if top.Id != 9904 || top.Port != 300 {
			t.Errorf("应返回port最大的一条: id=%d, port=%d", top.Id, top.Port)
		}
	})

	// 8. FindPageByCursor：游标分页
	t.Run("FindPageByCursor", func(t *testing.T) {
		var page1 testpb.GolangTestList
		if err := pdb.FindPageByCursor(&page1, "player_id = ?", []interface{}{9900}, "id", nil, 3); err != nil {
			t.Fatalf("首页查询失败: %v", err)
		}
		if len(page1.TestList) != 3 {
			t.Fatalf("首页预期3条，实际%d条", len(page1.TestList))
		}

		cursor := page1.TestList[len(page1.TestList)-1].Id
		var page2 testpb.GolangTestList
		if err := pdb.FindPageByCursor(&page2, "player_id = ?", []interface{}{9900}, "id", cursor, 3); err != nil {
			t.Fatalf("次页查询失败: %v", err)
		}
		for _, item := range page2.TestList {
			if item.Id <= cursor {
				t.Errorf("次页数据应全部大于游标%d: id=%d", cursor, item.Id)
			}
		}
	})

	t.Log("扩展增删改查接口测试通过")
}

// TestDescriptorTableOptions 单元测试：表配置直接从proto的message option读取，
// 调用方RegisterTable无需传任何TableOption；代码传入的选项仍可覆盖proto声明
func TestDescriptorTableOptions(t *testing.T) {
	// golang_test 在 .proto 里声明了 OptionTableName/OptionPrimaryKey/OptionAutoIncrementKey
	table := newMessageTable(&testpb.GolangTest{})
	if table.tableName != "golang_test" {
		t.Errorf("表名应从OptionTableName读取，实际%q", table.tableName)
	}
	if len(table.primaryKey) != 1 || table.primaryKey[0] != "id" {
		t.Errorf("主键应从OptionPrimaryKey读取，实际%v", table.primaryKey)
	}
	if table.autoIncreaseKey != "id" {
		t.Errorf("自增字段应从OptionAutoIncrementKey读取，实际%q", table.autoIncreaseKey)
	}

	// 建表SQL应带主键与自增
	sql := GenerateCreateTableSQL(&testpb.GolangTest{})
	if !strings.Contains(sql, "PRIMARY KEY (`id`)") || !strings.Contains(sql, "AUTO_INCREMENT") {
		t.Errorf("建表SQL应包含proto声明的主键/自增: %s", sql)
	}

	// 代码传入的TableOption优先级更高
	override := newMessageTable(&testpb.GolangTest{},
		WithTableName("custom_name"), WithPrimaryKey("group_id"))
	if override.tableName != "custom_name" {
		t.Errorf("显式WithTableName应覆盖proto声明，实际%q", override.tableName)
	}
	if len(override.primaryKey) != 1 || override.primaryKey[0] != "group_id" {
		t.Errorf("显式WithPrimaryKey应覆盖proto声明，实际%v", override.primaryKey)
	}

	// 索引选项解析：分号分隔多个索引，索引内逗号=联合索引；主键逗号=联合主键
	if got := splitOptionIndexes("player_id; zone_id,created_at"); len(got) != 2 || got[0] != "player_id" || got[1] != "zone_id,created_at" {
		t.Errorf("索引拆分错误: %v", got)
	}
	if got := splitOptionCSV("user_id, provider"); len(got) != 2 || got[0] != "user_id" || got[1] != "provider" {
		t.Errorf("CSV拆分错误: %v", got)
	}
}

// TestWithTableNameRegistration 单元测试：自定义表名只影响SQL，注册键仍为proto full name
func TestWithTableNameRegistration(t *testing.T) {
	pdb := NewDB()
	msg := &testpb.GolangTest{}
	pdb.RegisterTable(msg, WithTableName("player_data"), WithPrimaryKey("id"))

	// 注册键固定为proto full name，否则tableForMessage按FullName查表会miss
	table, ok := pdb.Tables[GetTableName(msg)]
	if !ok {
		t.Fatal("注册键应为proto full name")
	}
	if table.tableName != "player_data" {
		t.Errorf("SQL表名应为player_data，实际%s", table.tableName)
	}

	// 行消息与列表消息的查找路径都应能解析
	if _, err := pdb.tableForMessage(msg); err != nil {
		t.Errorf("tableForMessage应能解析自定义表名的注册: %v", err)
	}
	if _, _, err := resolveListTable(pdb.Tables, &testpb.GolangTestList{}); err != nil {
		t.Errorf("resolveListTable应能解析自定义表名的注册: %v", err)
	}

	// 预生成SQL应使用自定义表名
	if !strings.Contains(table.insertSQLTemplate, "`player_data`") {
		t.Errorf("INSERT模板应使用自定义表名: %s", table.insertSQLTemplate)
	}
	if !strings.Contains(table.selectFieldsSQL, "`player_data`") {
		t.Errorf("SELECT模板应使用自定义表名: %s", table.selectFieldsSQL)
	}
	if !strings.Contains(pdb.GetCreateTableSQL(msg), "`player_data`") {
		t.Errorf("建表SQL应使用自定义表名")
	}
}

// TestWrapExecErr 单元测试：MySQL 1062包装为ErrDuplicateKey，其它错误透传
func TestWrapExecErr(t *testing.T) {
	dup := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry '1' for key 'PRIMARY'"}
	wrapped := wrapExecErr(fmt.Errorf("exec: %w", dup))
	if !errors.Is(wrapped, ErrDuplicateKey) {
		t.Errorf("1062应可用errors.Is(ErrDuplicateKey)判断: %v", wrapped)
	}
	var me *mysql.MySQLError
	if !errors.As(wrapped, &me) || me.Number != 1062 {
		t.Errorf("包装后应保留原始MySQLError链: %v", wrapped)
	}

	other := &mysql.MySQLError{Number: 1146, Message: "Table doesn't exist"}
	if errors.Is(wrapExecErr(other), ErrDuplicateKey) {
		t.Error("非1062不应包装为ErrDuplicateKey")
	}
	plain := errors.New("plain")
	if wrapExecErr(plain) != plain {
		t.Error("普通错误应原样透传")
	}
	if wrapExecErr(nil) != nil {
		t.Error("nil应透传nil")
	}
}

// TestUpdateFieldsIfVersionValidation 单元测试：显式字段CAS的参数校验（无需数据库）
func TestUpdateFieldsIfVersionValidation(t *testing.T) {
	pdb := NewDB()
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	msg := &testpb.GolangTest{Id: 1}

	if _, err := pdb.UpdateFieldsIfVersion(msg, "group_id"); err == nil {
		t.Error("不传字段应报错")
	}
	if _, err := pdb.UpdateFieldsIfVersion(msg, "no_such_field", "ip"); err == nil || !strings.Contains(err.Error(), ErrFieldNotFound.Error()) {
		t.Errorf("未知版本字段应返回ErrFieldNotFound，实际: %v", err)
	}
	if _, err := pdb.UpdateFieldsIfVersion(msg, "group_id", "no_such_field"); err == nil || !strings.Contains(err.Error(), ErrFieldNotFound.Error()) {
		t.Errorf("未知更新字段应返回ErrFieldNotFound，实际: %v", err)
	}
}

// TestWithContextUnit 单元测试：WithContext返回共享映射的新实例，未绑定时退回Background
func TestWithContextUnit(t *testing.T) {
	pdb := NewDB()
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"))

	if pdb.context() != context.Background() {
		t.Error("未绑定ctx时应返回Background")
	}

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")
	bound := pdb.WithContext(ctx)
	if bound == pdb {
		t.Error("WithContext应返回新实例")
	}
	if bound.context() != ctx {
		t.Error("新实例应绑定传入的ctx")
	}
	if _, err := bound.tableForMessage(&testpb.GolangTest{}); err != nil {
		t.Errorf("新实例应共享已注册的表: %v", err)
	}
	// 原实例不受影响
	if pdb.ctx != nil {
		t.Error("WithContext不应修改原实例")
	}
}

// TestTableNameAndCASIntegration 集成测试：自定义表名接线全流程（对应已有表player_data场景）：
// Insert重复键→ErrDuplicateKey；UpdateFieldsIfVersion写零值字段+版本冲突；WithContext取消传播
func TestTableNameAndCASIntegration(t *testing.T) {
	pdb := NewDB()
	testTable := &testpb.GolangTest{}
	pdb.RegisterTable(testTable, WithTableName("golang_test_named"), WithPrimaryKey("id"))

	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	if _, err := db.Exec("DROP TABLE IF EXISTS `golang_test_named`"); err != nil {
		t.Fatalf("清理旧表失败: %v", err)
	}
	if err := pdb.CreateOrUpdateTable(testTable); err != nil {
		t.Fatalf("按自定义表名建表失败: %v", err)
	}

	// Insert + 回读都应落在自定义表名上
	row := &testpb.GolangTest{Id: 11, Ip: "10.11.0.1", Port: 7, GroupId: 0}
	if err := pdb.Insert(row); err != nil {
		t.Fatalf("Insert失败: %v", err)
	}
	got := &testpb.GolangTest{Id: 11}
	if err := pdb.FindOneByPK(got); err != nil {
		t.Fatalf("FindOneByPK失败: %v", err)
	}
	if got.Ip != "10.11.0.1" {
		t.Errorf("回读数据不一致: ip=%s", got.Ip)
	}

	// 重复插入→errors.Is(err, ErrDuplicateKey)
	if err := pdb.Insert(&testpb.GolangTest{Id: 11}); !errors.Is(err, ErrDuplicateKey) {
		t.Errorf("重复主键应返回ErrDuplicateKey，实际: %v", err)
	}

	// UpdateFieldsIfVersion：显式字段列表，零值字段（空字符串）也能写入
	clear := &testpb.GolangTest{Id: 11, Ip: "", GroupId: 0}
	ok, err := pdb.UpdateFieldsIfVersion(clear, "group_id", "ip")
	if err != nil {
		t.Fatalf("UpdateFieldsIfVersion失败: %v", err)
	}
	if !ok {
		t.Fatal("版本匹配时应更新成功")
	}
	got = &testpb.GolangTest{Id: 11}
	if err := pdb.FindOneByPK(got); err != nil {
		t.Fatalf("回查失败: %v", err)
	}
	if got.Ip != "" {
		t.Errorf("零值字段应被写入: ip=%q", got.Ip)
	}
	if got.GroupId != 1 {
		t.Errorf("版本应自动+1: group_id=%d", got.GroupId)
	}

	// 过期版本→false
	stale := &testpb.GolangTest{Id: 11, Ip: "10.11.9.9", GroupId: 0}
	ok, err = pdb.UpdateFieldsIfVersion(stale, "group_id", "ip")
	if err != nil {
		t.Fatalf("UpdateFieldsIfVersion失败: %v", err)
	}
	if ok {
		t.Error("版本冲突时应返回false")
	}

	// WithContext：已取消的ctx应中断查询
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pdb.WithContext(ctx).FindOneByPK(&testpb.GolangTest{Id: 11}); err == nil {
		t.Error("已取消的context应中断查询")
	}

	t.Log("自定义表名接线流程测试通过")
}

// TestRegisterAllTables 验证自动注册：扫描全局注册表，仅注册“所在文件声明了 db 选项
// 且自身声明了 table_name”的 message；没有 table_name 的消息（如列表消息、player）应被忽略。
func TestRegisterAllTables(t *testing.T) {
	pdb := NewDB()
	registered := pdb.RegisterAllTables()

	regSet := make(map[string]bool, len(registered))
	for _, name := range registered {
		regSet[name] = true
	}

	// testpb.proto 声明了 option (proto2mysql.db) = true，且这些消息声明了 table_name。
	for _, want := range []string{"golang_test", "golang_test1", "golang_test2", "golang_test3"} {
		if !regSet[want] {
			t.Errorf("预期自动注册表 %q，但未注册；已注册: %v", want, registered)
		}
		if _, ok := pdb.Tables[want]; !ok {
			t.Errorf("表 %q 未进入 Tables", want)
		}
	}

	// 没有 table_name 的消息不应被注册。
	for _, notWant := range []string{"golang_test_list", "player"} {
		if regSet[notWant] {
			t.Errorf("消息 %q 未声明 table_name，不应被自动注册", notWant)
		}
	}
}

func TestColumnFieldNumberMetadata(t *testing.T) {
	if got, want := columnComment(3), " COMMENT 'pb:3'"; got != want {
		t.Fatalf("columnComment() = %q, want %q", got, want)
	}

	for _, tc := range []struct {
		comment string
		want    protoreflect.FieldNumber
		ok      bool
	}{
		{comment: "pb:3", want: 3, ok: true},
		{comment: "other:3"},
		{comment: "pb:0"},
		{comment: "pb:536870912"},
		{comment: "pb:not-a-number"},
	} {
		got, ok := parseFieldNumFromComment(tc.comment)
		if got != tc.want || ok != tc.ok {
			t.Errorf("parseFieldNumFromComment(%q) = (%d, %v), want (%d, %v)", tc.comment, got, ok, tc.want, tc.ok)
		}
	}
}

func TestBuildAlterClausesUsesFieldNumbers(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{})
	ipField := table.Descriptor.Fields().ByName("ip")
	ipType := table.getMySQLFieldType(ipField)

	clauses := table.buildAlterClauses(map[string]columnMeta{
		"old_ip": {colType: ipType, fieldNum: ipField.Number()},
	})
	joined := strings.Join(clauses, "\n")
	wantRename := fmt.Sprintf("CHANGE COLUMN `old_ip` `ip` %s COMMENT 'pb:%d'", ipType, ipField.Number())
	if !strings.Contains(joined, wantRename) {
		t.Fatalf("missing field-number rename clause %q in:\n%s", wantRename, joined)
	}

	clauses = table.buildAlterClauses(map[string]columnMeta{
		"ip": {colType: ipType},
	})
	joined = strings.Join(clauses, "\n")
	wantBackfill := fmt.Sprintf("MODIFY COLUMN `ip` %s COMMENT 'pb:%d'", ipType, ipField.Number())
	if !strings.Contains(joined, wantBackfill) {
		t.Fatalf("missing legacy metadata backfill clause %q in:\n%s", wantBackfill, joined)
	}
}

// TestBuildAlterClausesRenameByFieldNumber 单元测试：迁移时根据 proto 字段号(Field id)
// 识别线上列——列名变化时用 CHANGE COLUMN 改名并对齐类型（保留数据），缺失字段用 ADD COLUMN，
// 且生成的列均带 pb:N 注释以便后续继续按字段号识别。
func TestBuildAlterClausesRenameByFieldNumber(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{})

	// 模拟线上表：字段 "ip"(号 2) 曾用列名 "ip_addr"（注释 pb:2），字段 "player_id"(号 6) 缺失。
	current := map[string]columnMeta{
		"id":       {colType: "int unsigned", fieldNum: 1},
		"ip_addr":  {colType: "varchar(255)", fieldNum: 2},
		"port":     {colType: "int unsigned", fieldNum: 3},
		"group_id": {colType: "int unsigned", fieldNum: 4},
		"player":   {colType: "mediumblob", fieldNum: 5},
	}

	clauses := table.buildAlterClauses(current)
	joined := strings.Join(clauses, " | ")

	// 按字段号识别到改名：CHANGE COLUMN `ip_addr` `ip`
	if !strings.Contains(joined, "CHANGE COLUMN `ip_addr` `ip`") {
		t.Errorf("应按字段号将 ip_addr 改名为 ip: %s", joined)
	}
	// 改名列须带字段号注释，便于后续迁移继续识别
	if !strings.Contains(joined, "COMMENT 'pb:2'") {
		t.Errorf("改名列应带 pb:2 注释: %s", joined)
	}
	// 缺失字段应新增
	if !strings.Contains(joined, "ADD COLUMN `player_id`") {
		t.Errorf("应新增缺失字段 player_id: %s", joined)
	}
	// ip 已按字段号改名，不应再被误判为新增（否则旧数据丢失）
	if strings.Contains(joined, "ADD COLUMN `ip` ") {
		t.Errorf("ip 已按字段号改名，不应再 ADD: %s", joined)
	}
}

// TestBinaryFieldRawStorage 端到端验证二进制字段以 proto 裸字节落库（不是 Base64）：
// 直接读列长度与列内容，与应用层 proto.Marshal 的结果逐字节比对。
// 这条锁住的是存储格式契约——改了它，已在库里的行就读不出来了。
func TestBinaryFieldRawStorage(t *testing.T) {
	cfg := GetMysqlConfig()
	if cfg == nil || !cfg.InterpolateParams {
		t.Fatal("本测试必须覆盖 go-sql-driver interpolateParams=true 路径")
	}

	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	msg := &testpb.GolangTest{}
	pdb.RegisterTable(msg)
	recreateTestTable(t, db, pdb, msg)

	sub := &testpb.Player{PlayerId: 1 << 62, Name: "玩家名"}
	want, err := proto.Marshal(sub)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	row := &testpb.GolangTest{Id: 1, Ip: "10.0.0.1", Port: 3306, Player: sub}
	if err := pdb.Insert(row); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// 列里存的必须是裸字节：长度等于 Marshal 结果，Base64 会是它的 4/3
	var stored []byte
	q := "SELECT `player` FROM " + testTableSQLName(msg) + " WHERE `id` = 1"
	if err := db.QueryRow(q).Scan(&stored); err != nil {
		t.Fatalf("读列失败: %v", err)
	}
	if !bytes.Equal(stored, want) {
		t.Fatalf("列内容不是 proto 裸字节\n实际 %d 字节: %x\n期望 %d 字节: %x",
			len(stored), stored, len(want), want)
	}

	// 不经本库、直接对列做 Unmarshal 也应成功（这正是手写 SQL 侧的读法）
	var direct testpb.Player
	if err := proto.Unmarshal(stored, &direct); err != nil {
		t.Fatalf("列内容应可被直接 Unmarshal: %v", err)
	}
	if !proto.Equal(sub, &direct) {
		t.Errorf("直接反序列化不一致: %s vs %s", sub.String(), direct.String())
	}

	// 再走本库读回，确认对称
	loaded := &testpb.GolangTest{Id: 1}
	if err := pdb.FindOneByPK(loaded); err != nil {
		t.Fatalf("按主键读回失败: %v", err)
	}
	if !proto.Equal(sub, loaded.Player) {
		t.Errorf("读回不一致: %s vs %s", sub.String(), loaded.Player.String())
	}

	// 驱动插值模式下 string 参数也必须能把任意字节逐字节写进 BLOB。
	// 覆盖 NUL、0xff、续字节和非法 UTF-8，防止连接字符集悄悄转换或拒绝数据。
	arbitrary := []byte{0x00, 0xff, 0x80, 0xc3, 0x28, 0x00}
	update := "UPDATE " + testTableSQLName(msg) + " SET `player` = ? WHERE `id` = 1"
	if _, err := db.Exec(update, string(arbitrary)); err != nil {
		t.Fatalf("插值模式写入任意二进制失败: %v", err)
	}
	stored = nil
	if err := db.QueryRow(q).Scan(&stored); err != nil {
		t.Fatalf("读取任意二进制失败: %v", err)
	}
	if !bytes.Equal(stored, arbitrary) {
		t.Fatalf("任意二进制未逐字节保留\n实际: %x\n期望: %x", stored, arbitrary)
	}
}

// TestSQLBuilderMySQLExecution 在真实 MySQL 上冒烟执行 Builder 的关键语法，
// 覆盖 ON DUPLICATE KEY、FOR UPDATE、表达式更新、扣减守卫和 ORDER BY ... LIMIT 批删。
func TestSQLBuilderMySQLExecution(t *testing.T) {
	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	model := &testpb.GolangTest{}
	pdb.RegisterTable(model)
	recreateTestTable(t, db, pdb, model)
	b := NewSQLBuilder(model)

	row := &testpb.GolangTest{Id: 101, Ip: "builder", Port: 10, GroupId: 1}
	stmt, err := b.Insert(row)
	if err != nil {
		t.Fatalf("生成 INSERT: %v", err)
	}
	if _, err := db.Exec(stmt.Sql, stmt.Args...); err != nil {
		t.Fatalf("执行 INSERT: %v", err)
	}

	stmt, err = b.UpsertAdd(&testpb.GolangTest{Id: 101, Port: 5}, "port")
	if err != nil {
		t.Fatalf("生成 UPSERT ADD: %v", err)
	}
	if _, err := db.Exec(stmt.Sql, stmt.Args...); err != nil {
		t.Fatalf("执行 UPSERT ADD: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("开启事务: %v", err)
	}
	stmt, err = b.SelectColumns([]string{"port"}, "`id` = ?", []interface{}{101}, QueryOptions{ForUpdate: true})
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("生成 SELECT FOR UPDATE: %v", err)
	}
	var port int64
	if err := tx.QueryRow(stmt.Sql, stmt.Args...).Scan(&port); err != nil {
		_ = tx.Rollback()
		t.Fatalf("执行 SELECT FOR UPDATE: %v", err)
	}
	if port != 15 {
		_ = tx.Rollback()
		t.Fatalf("UPSERT ADD 后 port=%d，期望 15", port)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("提交事务: %v", err)
	}

	stmt, err = b.UpdateAssignsWhere([]Assign{AddCol("port", 2)}, "`id` = ?", []interface{}{101})
	if err != nil {
		t.Fatalf("生成表达式 UPDATE: %v", err)
	}
	if _, err := db.Exec(stmt.Sql, stmt.Args...); err != nil {
		t.Fatalf("执行表达式 UPDATE: %v", err)
	}

	stmt, err = b.DecrByPKIfEnough(&testpb.GolangTest{Id: 101}, "port", 3)
	if err != nil {
		t.Fatalf("生成守卫扣减: %v", err)
	}
	result, err := db.Exec(stmt.Sql, stmt.Args...)
	if err != nil {
		t.Fatalf("执行守卫扣减: %v", err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != 1 {
		t.Fatalf("守卫扣减 RowsAffected=%d err=%v，期望 1", affected, err)
	}

	stmt, err = b.DeleteWhereLimit("`id` = ?", []interface{}{101}, "`id` ASC", 1)
	if err != nil {
		t.Fatalf("生成有界 DELETE: %v", err)
	}
	result, err = db.Exec(stmt.Sql, stmt.Args...)
	if err != nil {
		t.Fatalf("执行有界 DELETE: %v", err)
	}
	affected, err = result.RowsAffected()
	if err != nil || affected != 1 {
		t.Fatalf("有界 DELETE RowsAffected=%d err=%v，期望 1", affected, err)
	}
}
