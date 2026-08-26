package proto2mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	testpb "github.com/luyuancpp/proto2mysql/internal/testpb"
)

func TestCrossFamilyTypeChangeFailsClosed(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithAutoIncrementKey(""))
	current := map[string]columnMeta{
		"port": {colType: "mediumtext", fieldNum: 3},
	}

	_, err := table.buildAlterClauses(current, false)
	if !errors.Is(err, ErrUnsafeSchemaConversion) {
		t.Fatalf("同名字段从 MEDIUMTEXT 自动改成 INT 会把非数字整列转成 0，必须 fail-closed，实际: %v", err)
	}
}

func TestRenamedColumnAttributeDriftFailsClosed(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithAutoIncrementKey(""))
	current := map[string]columnMeta{
		// pb:3 表明这是 port 的旧名字；真实元数据却允许 NULL 且默认值为 7。
		"legacy_port": {
			colType:          "int unsigned",
			fieldNum:         3,
			nullable:         true,
			defaultValue:     sql.NullString{String: "7", Valid: true},
			metadataComplete: true,
		},
	}

	_, err := table.planSchemaAlignment(current, nil, false, nil, false)
	if !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("按 pb:N 改名也必须先校验旧列的 NULL/DEFAULT/AUTO_INCREMENT 漂移，实际: %v", err)
	}
}

func TestSchemaDriftNullableFailsClosed(t *testing.T) {
	pdb, conn := newFakeDB(t)
	cols := golangTestAlignedCols()
	cols[2] = colRowAttrs("port", "int unsigned", 3, true, "")
	queueLockedSchemaSync(conn,
		rows(row(int64(1))), // table exists
		cols,
		rows(indexRow("PRIMARY", true, 1, "id", nil)), // current primary key
	)

	err := pdb.CreateOrUpdateTable(&testpb.GolangTest{})
	if !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("nullable drift must fail closed with ErrSchemaDrift, got %v", err)
	}
	if conn.countSQL("ALTER TABLE") != 0 {
		t.Fatalf("drift validation must happen before ALTER, SQL: %v", conn.sqls())
	}
}

func TestSchemaDriftIndexColumnsFailsClosed(t *testing.T) {
	sqlDB, conn := openFakeDB()
	t.Cleanup(func() { _ = sqlDB.Close() })
	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithIndexes("player_id"))

	queueLockedSchemaSync(conn,
		rows(row(int64(1))),
		golangTestAlignedCols(),
		rows(indexRow("idx_golang_test_0", false, 1, "id", nil)), // same name, wrong columns
		rows(indexRow("PRIMARY", true, 1, "id", nil)),
	)

	err := pdb.CreateOrUpdateTable(&testpb.GolangTest{})
	if !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("same-name index mismatch must fail closed with ErrSchemaDrift, got %v", err)
	}
	if conn.countSQL("ALTER TABLE") != 0 {
		t.Fatalf("drift validation must happen before ALTER, SQL: %v", conn.sqls())
	}
}

func TestIndexNamesAreCaseInsensitiveDuringSchemaSync(t *testing.T) {
	tests := []struct {
		name      string
		indexCol  string
		wantDrift bool
	}{
		{
			name:     "same definition does not add duplicate index",
			indexCol: "player_id",
		},
		{
			name:      "different definition still fails closed",
			indexCol:  "port",
			wantDrift: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB, conn := openFakeDB()
			t.Cleanup(func() { _ = sqlDB.Close() })
			pdb := NewDB()
			pdb.DB = sqlDB
			pdb.DBName = "testdb"
			pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithIndexes("player_id"))

			queueLockedSchemaSync(conn,
				rows(row(int64(1))),
				golangTestAlignedCols(),
				rows(indexRow("IDX_GOLANG_TEST_0", false, 1, tc.indexCol, nil)),
				rows(indexRow("PRIMARY", true, 1, "id", nil)),
			)

			err := pdb.CreateOrUpdateTable(&testpb.GolangTest{})
			if tc.wantDrift {
				if !errors.Is(err, ErrSchemaDrift) {
					t.Fatalf("case-insensitive name match must still compare definitions: %v", err)
				}
			} else if err != nil {
				t.Fatalf("index names differing only by case must be treated as the same index: %v", err)
			}
			if conn.countSQL("ALTER TABLE") != 0 {
				t.Fatalf("existing uppercase index name must not generate ADD INDEX: %v", conn.sqls())
			}
		})
	}
}

func TestColumnNamesAreCaseInsensitiveDuringSchemaPlanning(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithAutoIncrementKey(""))
	current := alignedCols(table, nil)
	port := current["port"]
	port.fieldNum = 0 // 存量表没有 pb:N 注释，不能靠字段号兜底识别。
	delete(current, "port")
	current["PORT"] = port

	clauses, err := table.buildAlterClauses(current, false)
	if err != nil {
		t.Fatalf("大小写不同的同一列不应被判成缺列: %v", err)
	}
	joined := strings.Join(clauses, " | ")
	if strings.Contains(joined, "ADD COLUMN `port`") {
		t.Fatalf("MySQL 列名不区分大小写，线上 PORT 不得再生成 port: %s", joined)
	}
	if !strings.Contains(joined, "MODIFY COLUMN `port`") {
		t.Fatalf("旧列缺少 pb:N 时仍应在同一列上回填注释，实际: %s", joined)
	}
}

func TestCaseFoldedColumnAmbiguityFailsClosed(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithAutoIncrementKey(""))
	current := alignedCols(table, nil)
	upper := current["port"]
	upper.fieldNum = 0
	current["PORT"] = upper

	_, err := table.buildAlterClauses(current, false)
	if !errors.Is(err, ErrSchemaDrift) || !strings.Contains(err.Error(), "PORT") {
		t.Fatalf("大小写折叠后多个候选必须 fail-closed 并点名冲突列，实际: %v", err)
	}
}

func TestCaseInsensitiveColumnMatchStillChecksAttributeDrift(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithAutoIncrementKey(""))
	current := alignedCols(table, nil)
	port := current["port"]
	port.fieldNum = 0
	port.metadataComplete = true
	port.nullable = true
	port.defaultValue = sql.NullString{String: "0", Valid: true}
	delete(current, "port")
	current["PORT"] = port

	err := table.validateSchemaDrift(current, nil, false, nil)
	if !errors.Is(err, ErrSchemaDrift) || !strings.Contains(err.Error(), "PORT->port nullable mismatch") {
		t.Fatalf("大小写不同的同一列仍必须执行属性漂移校验，实际: %v", err)
	}
}

func TestSchemaDriftPrimaryKeyOrderFailsClosed(t *testing.T) {
	pdb, conn := newFakeDB(t)
	registryKey := GetTableName(&testpb.GolangTest{})
	pdb.Tables[registryKey].primaryKey = []string{"id", "port"}
	queueLockedSchemaSync(conn,
		rows(row(int64(1))),
		golangTestAlignedCols(),
		rows(
			indexRow("PRIMARY", true, 1, "port", nil),
			indexRow("PRIMARY", true, 2, "id", nil),
		), // same columns, wrong order
	)

	err := pdb.CreateOrUpdateTable(&testpb.GolangTest{})
	if !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("primary-key order drift must fail closed with ErrSchemaDrift, got %v", err)
	}
	if conn.countSQL("ALTER TABLE") != 0 {
		t.Fatalf("drift validation must happen before ALTER, SQL: %v", conn.sqls())
	}
}

func TestColumnMetadataReadsDefaultAndMarksComplete(t *testing.T) {
	pdb, conn := newFakeDB(t)
	conn.queueRows(rows(colRowAttrsDefault("port", "int unsigned", 3, false, "", "7")))

	metas, err := pdb.getTableColumnMeta(GetTableName(&testpb.GolangTest{}))
	if err != nil {
		t.Fatalf("getTableColumnMeta: %v", err)
	}
	got := metas["port"]
	if !got.metadataComplete {
		t.Fatal("information_schema metadata must be marked complete")
	}
	if !got.defaultValue.Valid || got.defaultValue.String != "7" {
		t.Fatalf("COLUMN_DEFAULT was not preserved: %+v", got.defaultValue)
	}
}

func TestSchemaDriftDefaultFailsClosed(t *testing.T) {
	pdb, conn := newFakeDB(t)
	cols := golangTestAlignedCols()
	cols[2] = colRowAttrsDefault("port", "int unsigned", 3, false, "", "7")
	queueLockedSchemaSync(conn,
		rows(row(int64(1))),
		cols,
		rows(indexRow("PRIMARY", true, 1, "id", nil)),
	)

	err := pdb.CreateOrUpdateTable(&testpb.GolangTest{})
	if !errors.Is(err, ErrSchemaDrift) || !strings.Contains(err.Error(), "default mismatch") {
		t.Fatalf("default drift must fail closed with a useful ErrSchemaDrift, got %v", err)
	}
	if conn.countSQL("ALTER TABLE") != 0 {
		t.Fatalf("default drift must not generate a destructive MODIFY, SQL: %v", conn.sqls())
	}
}

func TestSchemaDriftAutoIncrementFailsClosed(t *testing.T) {
	pdb, conn := newFakeDB(t)
	cols := golangTestAlignedCols()
	cols[0] = colRowAttrsDefault("id", "int unsigned", 1, false, "", nil)
	queueLockedSchemaSync(conn,
		rows(row(int64(1))),
		cols,
		rows(indexRow("PRIMARY", true, 1, "id", nil)),
	)

	err := pdb.CreateOrUpdateTable(&testpb.GolangTest{})
	if !errors.Is(err, ErrSchemaDrift) || !strings.Contains(err.Error(), "auto_increment mismatch") {
		t.Fatalf("auto_increment drift must fail closed with a useful ErrSchemaDrift, got %v", err)
	}
	if conn.countSQL("ALTER TABLE") != 0 {
		t.Fatalf("auto_increment drift must not generate a destructive MODIFY, SQL: %v", conn.sqls())
	}
}

func TestIncompleteColumnMetadataDoesNotInventDrift(t *testing.T) {
	table := newMessageTable(&testpb.GolangTest{}, WithPrimaryKey("id"))
	cols := make(map[string]columnMeta)
	fields := table.Descriptor.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		cols[string(field.Name())] = columnMeta{
			colType:  table.getMySQLFieldType(field),
			fieldNum: field.Number(),
			// nullable/default/extra intentionally unknown
		}
	}
	primary := table.expectedIndexMeta("id", true)

	plan, err := table.planSchemaAlignment(cols, nil, false, &primary, false)
	if err != nil {
		t.Fatalf("incomplete synthetic metadata must not be treated as observed drift: %v", err)
	}
	if !plan.empty() {
		t.Fatalf("type-aligned incomplete metadata should not produce DDL: %+v", plan)
	}
}

func TestSchemaDriftIndexDefinitionFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		opts      []TableOption
		indexRows [][]driver.Value
	}{
		{
			name:      "ordinary index is unexpectedly unique",
			opts:      []TableOption{WithIndexes("player_id")},
			indexRows: rows(indexRow("idx_golang_test_0", true, 1, "player_id", nil)),
		},
		{
			name:      "ordinary text index has no prefix",
			opts:      []TableOption{WithIndexes("ip")},
			indexRows: rows(indexRow("idx_golang_test_0", false, 1, "ip", nil)),
		},
		{
			name:      "declared unique index is ordinary",
			opts:      []TableOption{WithUniqueKey("ip")},
			indexRows: rows(indexRow("uk_golang_test", false, 1, "ip", int64(TextIndexPrefixLength))),
		},
		{
			name:      "unique text prefix length differs",
			opts:      []TableOption{WithUniqueKey("ip")},
			indexRows: rows(indexRow("uk_golang_test", true, 1, "ip", int64(100))),
		},
		{
			name: "composite index order differs",
			opts: []TableOption{WithIndexes("player_id,port")},
			indexRows: rows(
				indexRow("idx_golang_test_0", false, 1, "port", nil),
				indexRow("idx_golang_test_0", false, 2, "player_id", nil),
			),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB, conn := openFakeDB()
			t.Cleanup(func() { _ = sqlDB.Close() })
			pdb := NewDB()
			pdb.DB = sqlDB
			pdb.DBName = "testdb"
			opts := append([]TableOption{WithPrimaryKey("id")}, tc.opts...)
			pdb.RegisterTable(&testpb.GolangTest{}, opts...)

			queueLockedSchemaSync(conn,
				rows(row(int64(1))),
				golangTestAlignedCols(),
				tc.indexRows,
				rows(indexRow("PRIMARY", true, 1, "id", nil)),
			)

			err := pdb.CreateOrUpdateTable(&testpb.GolangTest{})
			if !errors.Is(err, ErrSchemaDrift) {
				t.Fatalf("index definition drift must return ErrSchemaDrift, got %v", err)
			}
			if conn.countSQL("ALTER TABLE") != 0 {
				t.Fatalf("same-name drift must not be auto-rebuilt, SQL: %v", conn.sqls())
			}
		})
	}
}

func TestSchemaDriftPrimaryKeyPrefixFailsClosed(t *testing.T) {
	pdb, conn := newFakeDB(t)
	queueLockedSchemaSync(conn,
		rows(row(int64(1))),
		golangTestAlignedCols(),
		rows(indexRow("PRIMARY", true, 1, "id", int64(TextIndexPrefixLength))),
	)

	err := pdb.CreateOrUpdateTable(&testpb.GolangTest{})
	if !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("primary-key SUB_PART drift must return ErrSchemaDrift, got %v", err)
	}
}

func TestExtraOnlineIndexIsLeftUntouched(t *testing.T) {
	sqlDB, conn := openFakeDB()
	t.Cleanup(func() { _ = sqlDB.Close() })
	pdb := NewDB()
	pdb.DB = sqlDB
	pdb.DBName = "testdb"
	pdb.RegisterTable(&testpb.GolangTest{}, WithPrimaryKey("id"), WithIndexes("player_id"))
	queueLockedSchemaSync(conn,
		rows(row(int64(1))),
		golangTestAlignedCols(),
		rows(
			indexRow("idx_dba_covering", false, 1, "port", nil),
			indexRow("idx_golang_test_0", false, 1, "player_id", nil),
		),
		rows(indexRow("PRIMARY", true, 1, "id", nil)),
	)

	if err := pdb.CreateOrUpdateTable(&testpb.GolangTest{}); err != nil {
		t.Fatalf("an extra online index must be preserved and ignored: %v", err)
	}
	if conn.countSQL("ALTER TABLE") != 0 {
		t.Fatalf("aligned declarations plus a DBA index require no ALTER: %v", conn.sqls())
	}
}

func TestIndexMetadataReadFailureIsReturned(t *testing.T) {
	pdb, conn := newFakeDB(t)
	table := pdb.Tables[GetTableName(&testpb.GolangTest{})]
	table.indexes = []string{"player_id"}
	want := errors.New("statistics unavailable")
	conn.failNext = want

	_, err := pdb.missingIndexClauses(table)
	if !errors.Is(err, want) {
		t.Fatalf("index metadata read failure must be returned, got %v", err)
	}
}

func TestSchemaDriftRealMySQL(t *testing.T) {
	pdb := NewDB()
	db := mustOpenTestDB(t, pdb)
	defer closeTestDB(t, db)

	const tableName = "schema_drift_e2e_probe"
	opts := []TableOption{
		WithTableName(tableName),
		WithPrimaryKey("id"),
		WithAutoIncrementKey("id"),
		WithIndexes("player_id"),
	}
	pdb.RegisterTable(&testpb.GolangTest{}, opts...)
	registryKey := GetTableName(&testpb.GolangTest{})
	table := pdb.Tables[registryKey]
	if _, err := db.Exec("DROP TABLE IF EXISTS " + escapeMySQLName(tableName)); err != nil {
		t.Fatalf("drop probe table: %v", err)
	}
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + escapeMySQLName(tableName)) }()
	if _, err := db.Exec(table.GetCreateTableSQL()); err != nil {
		t.Fatalf("create aligned probe table: %v", err)
	}

	if err := pdb.CreateOrUpdateTable(&testpb.GolangTest{}); err != nil {
		t.Fatalf("real MySQL aligned metadata should pass: %v", err)
	}

	if _, err := db.Exec("ALTER TABLE " + escapeMySQLName(tableName) + " ALTER COLUMN `port` SET DEFAULT 7"); err != nil {
		t.Fatalf("introduce default drift: %v", err)
	}
	if err := pdb.CreateOrUpdateTable(&testpb.GolangTest{}); !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("real MySQL default drift must fail closed, got %v", err)
	}

	if _, err := db.Exec("ALTER TABLE " + escapeMySQLName(tableName) + " ALTER COLUMN `port` SET DEFAULT 0"); err != nil {
		t.Fatalf("restore default: %v", err)
	}
	indexName := table.indexNameFor(0)
	// TiDB 不支持在同一条 ALTER 里 DROP 后立即复用同一个索引名；拆开也更准确地
	// 模拟 DBA 先删后重建错定义的线上漂移。
	if _, err := db.Exec("ALTER TABLE " + escapeMySQLName(tableName) +
		" DROP INDEX " + escapeMySQLName(indexName)); err != nil {
		t.Fatalf("drop aligned index before drift: %v", err)
	}
	if _, err := db.Exec("ALTER TABLE " + escapeMySQLName(tableName) +
		" ADD UNIQUE KEY " + escapeMySQLName(indexName) + " (`player_id`)"); err != nil {
		t.Fatalf("introduce index uniqueness drift: %v", err)
	}
	if err := pdb.CreateOrUpdateTable(&testpb.GolangTest{}); !errors.Is(err, ErrSchemaDrift) {
		t.Fatalf("real MySQL index uniqueness drift must fail closed, got %v", err)
	}
}
