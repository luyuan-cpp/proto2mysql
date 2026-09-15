# proto2mysql

一个高效的 Go 库，用于自动将 Protobuf 消息映射到 MySQL 表结构，并提供简洁的 CRUD 操作接口，无需手动编写 SQL。

## 功能特点

- **自动映射**：Protobuf 消息与 MySQL 表结构自动映射，包括字段类型转换
- **结构管理**：自动创建和更新表结构，支持主键、索引、唯一键和自增字段
- **安全操作**：所有数据库操作使用参数化查询，避免 SQL 注入风险
- **类型处理**：内置支持 Protobuf 特殊类型（如 Timestamp）和集合类型（map/list）
- **批量操作**：支持批量插入等高效操作，提升性能
- **并发安全**：内部使用读写锁保证并发操作安全

## 安装

```bash
go get github.com/your-username/proto2mysql
```

## 快速开始

### 1. 定义 Protobuf 消息（表配置直接写在 proto 里）

```protobuf
syntax = "proto3";
package example;

import "google/protobuf/timestamp.proto";
import "proto2mysql_option.proto";  // 本仓库 proto/ 目录提供

option (proto2mysql.db) = true;  // 文件级标识：本文件用于 proto2mysql 建表（供自动扫描识别）

message User {
  option (proto2mysql.table_name)         = "user";
  option (proto2mysql.primary_key)        = "id";
  option (proto2mysql.auto_increment_key) = "id";
  option (proto2mysql.unique_key)         = "email";        // 逗号分隔 = 联合唯一键
  option (proto2mysql.index)              = "name;age";     // 分号分隔多个索引，索引内逗号 = 联合索引

  int64  id         = 1;
  string name       = 2;
  string email      = 3;
  int32  age        = 4 [(proto2mysql.nullable) = true];    // 该列允许为 NULL
  google.protobuf.Timestamp create_time = 5;
}

message UserList {
  repeated User items = 1;  // 用于批量查询
}
```

### 2. 生成 Go 代码

```bash
protoc --go_out=. --go_opt=paths=source_relative example.proto
```

### 3. 使用 proto2mysql 库

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/your-username/proto2mysql"
	pb "your-module/example"
)

func main() {
	// 1. 连接 MySQL 数据库
	db, err := sql.Open("mysql", "user:password@tcp(localhost:3306)/testdb?parseTime=true")
	if err != nil {
		log.Fatalf("无法连接数据库: %v", err)
	}
	defer db.Close()

	// 2. 初始化 proto2mysql 实例
	pbDB := proto2mysql.NewDB()
	if err := pbDB.OpenDB(db, "testdb"); err != nil {
		log.Fatalf("无法打开数据库: %v", err)
	}

	// 3. 注册 Protobuf 消息与表的映射关系
	// 表配置（表名/主键/自增/索引/唯一键/可空字段）自动从 proto 的 option 中读取，无需传参；
	// 也可以传 TableOption 覆盖 proto 里的声明（proto 未声明时同样可用代码配置）：
	// pbDB.RegisterTable(&pb.User{}, proto2mysql.WithPrimaryKey("id"), ...)
	pbDB.RegisterTable(&pb.User{})

	// 4. 创建或更新表结构
	if err := pbDB.CreateOrUpdateTable(&pb.User{}); err != nil {
		log.Fatalf("创建表失败: %v", err)
	}

	// 5. 执行 CRUD 操作
	// 插入数据
	user := &pb.User{
		Name:       "张三",
		Email:      "zhangsan@example.com",
		Age:        30,
		CreateTime: timestamppb.Now(),
	}
	if err := pbDB.Insert(user); err != nil {
		log.Printf("插入失败: %v", err)
	}

	// 查询数据
	var result pb.User
	if err := pbDB.FindOneByKV(&result, "email", "zhangsan@example.com"); err != nil {
		log.Printf("查询失败: %v", err)
	} else {
		fmt.Printf("查询结果: %+v\n", result)
	}

	// 更新数据
	result.Age = 31
	if err := pbDB.Update(&result); err != nil {
		log.Printf("更新失败: %v", err)
	}

	// 批量查询
	var userList pb.UserList
	if err := pbDB.FindAllByWhereWithArgs(&userList, "age > ?", []interface{}{20}); err != nil {
		log.Printf("批量查询失败: %v", err)
	} else {
		fmt.Printf("批量查询结果: %+v\n", userList)
	}

	// 删除数据
	if err := pbDB.Delete(&result); err != nil {
		log.Printf("删除失败: %v", err)
	}
}
```

### 4. 自动注册（无需逐个 RegisterTable）

如果接入方不想手动为每个消息调用 `RegisterTable`，可以让库自动扫描项目里的
proto 描述符（descriptor option）来注册全部表。规则是：

1. `.proto` 文件顶部声明 `option (proto2mysql.db) = true;`（文件级标识：本文件用于建表）；
2. 文件里的某个 `message` 声明了 `option (proto2mysql.table_name) = "...";`。

同时满足这两点的 message 才会被自动注册；只声明了 `db` 但没有 `table_name`
的消息（如列表消息 `UserList`、内嵌子消息）会被跳过。

前提：这些 `.proto` 生成的 Go 代码已被链接进当前二进制（有任意 `import`，
其 `init` 会把描述符注册到 `protoregistry.GlobalFiles` 全局表）。

```go
pbDB := proto2mysql.NewDB()
if err := pbDB.OpenDB(db, "testdb"); err != nil {
	log.Fatalf("无法打开数据库: %v", err)
}

// 自动扫描全局描述符，注册所有“文件声明了 db 且 message 声明了 table_name”的表。
// 返回被注册的表名（proto full name）列表。
registered := pbDB.RegisterAllTables()
log.Printf("自动注册的表: %v", registered)

// 一次性对所有已注册的表建表 / 对齐字段（表不存在则创建，存在则 ALTER 对齐）。
if err := pbDB.SyncAllTables(); err != nil {
	log.Fatalf("同步表结构失败: %v", err)
}

// 之后即可直接 CRUD，无需再逐个 RegisterTable。
```

> 说明：是否建表只取决于各 `message` 是否声明了 `table_name`；文件级 `db`
> 选项只用于圈定“哪些文件参与自动扫描”，不改变单个消息的建表行为。

## 核心功能

### 表结构管理

- `RegisterTable(m proto.Message, opts ...TableOption)`: 手动注册单个消息与表的映射
- `RegisterAllTables() []string`: 自动扫描全局描述符，注册所有“文件声明了 db 且 message 声明了 table_name”的表，返回被注册的表名
- `SyncAllTables() error`: 对所有已注册的表批量建表/对齐字段
- `CreateOrUpdateTable(m proto.Message)`: 创建表（如果不存在）或更新表结构
- `UpdateTableField(m proto.Message)`: 同步表字段结构
- `IsTableExists(tableName string) (bool, error)`: 检查表是否存在

#### 按 proto 字段号（Field id）迁移，改名/改类型保留数据

建表时每列都会写入注释 `COMMENT 'pb:<字段号>'`，记录该列对应的 proto 字段号。之后调用
`UpdateTableField` / `SyncAllTables` / `GenerateMigrationSQL` 同步结构时，会先扫描线上表
（读取 `information_schema` 的列类型与注释），并按如下优先级对齐：

1. **列名相同**：仅对安全扩容执行 `MODIFY COLUMN`（并回填字段号注释）；signed/unsigned、
   NULL、DEFAULT、AUTO_INCREMENT 等不安全漂移返回 `ErrUnsafeSchemaConversion` / `ErrSchemaDrift`；
2. **列名不同但字段号相同**（即 proto 里把该字段改了名字）：用
   `CHANGE COLUMN 旧列名 新列名 新类型 COMMENT 'pb:N'` 改名并对齐类型，**原有数据保留**；
3. **找不到对应列**：`ADD COLUMN` 新增。

> 注意：旧版本（本特性之前）建的表，列上没有 `pb:N` 注释，因此**首次**同步无法按字段号识别
> 改名（会退化为按列名匹配）。首次同步会为同名列自动回填字段号注释，之后即可正常按字段号
> 识别改名。新建的表从一开始就带注释，改名识别始终有效。
> 该逻辑位于运行时库（需连库）；离线的 `proto2sql` 工具不连库，不做此扫描。

### 数据操作

#### 插入
- `Insert(message proto.Message) error`: 插入单条记录
- `BatchInsert(messages []proto.Message) error`: 批量插入记录
- `InsertOnDupUpdate(message proto.Message) error`: 历史 upsert 名称，现与 `Save` 使用相同的完整主键精确语义
- `InsertIgnore(message proto.Message) (bool, error)`: 普通 INSERT；仅把 1062 唯一键冲突解释为“未插入”，其它数据错误照常返回
- `Save(message proto.Message) error`: 按完整主键整行保存；存在则更新，不存在则插入，备用唯一键冲突返回 `ErrDuplicateKey`
- `BatchSave(messages []proto.Message) error`: `Save` 的逐行批量版（事务外非原子）

#### 查询
- `FindOneByKV(message proto.Message, whereKey string, whereVal string) error`: 按键值对查询单条记录
- `FindOneByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error`: 按条件查询单条记录
- `FindAll(message proto.Message) error`: 查询所有记录
- `FindAllByWhereWithArgs(message proto.Message, whereClause string, whereArgs []interface{}) error`: 按条件查询多条记录

#### 更新
- `Update(message proto.Message) error`: 按主键更新记录

#### 删除
- `Delete(message proto.Message) error`: 按主键删除记录

## 只生成 SQL、不执行（SQLBuilder）

`SQLBuilder` 从一个 `proto.Message` 直接产出参数化的 **DML** 语句（INSERT / SELECT / UPDATE /
DELETE），**不连库、不需要 `RegisterTable`、不执行任何语句**。适合把语句交给已有的 `*sql.DB` /
`*sql.Tx` / sqlx / kratos data 层自己执行、在事务里和手写 SQL 混用、或先打日志再执行。

（`sqlgen.go` 里的 `GenerateCreateTableSQL` / `GenerateMigrationSQL` 负责 DDL，本节负责 DML。）

```go
b := proto2mysql.NewSQLBuilder(&pb.PlayerCurrency{})   // 表配置自动读 proto option
// 或复用已注册表的配置：b, err := pbDB.SQLBuilder(&pb.PlayerCurrency{})

stmt, err := b.UpsertAdd(row, "gold")   // 插入或累加
// stmt.Sql  = INSERT INTO `player_currency` (...) VALUES (?, ?)
//             ON DUPLICATE KEY UPDATE `gold` = `gold` + VALUES(`gold`)
// stmt.Args = [...]
if _, err := tx.ExecContext(ctx, stmt.Sql, stmt.Args...); err != nil { ... }
```

### 接口一览

| 分类 | 方法 | 产出 |
|------|------|------|
| INSERT | `Insert` | 全字段插入 |
| | `InsertSetFields` | 只插已赋值字段，其余交给列默认值（自增 id / `DEFAULT CURRENT_TIMESTAMP`） |
| | `InsertIgnore` / `InsertIgnoreSetFields` | no-op ODKU，只跳过唯一键冲突，不吞类型截断等真实错误 |
| | `Replace` | `REPLACE INTO`（先删后插） |
| | `BatchInsert` / `BatchInsertIgnore` / `BatchReplace` | 多行 VALUES（Ignore 版同样只跳过唯一键冲突） |
| UPSERT | `Upsert(m, cols...)` | `ON DUPLICATE KEY UPDATE c = VALUES(c)`，覆盖 |
| | `UpsertAdd(m, cols...)` | `c = c + VALUES(c)`，累加计数器 |
| | `UpsertKeepOld(m)` | `pk = pk`，插入或只加行锁不改数据 |
| | `UpsertWith(m, assigns...)` | 自定义冲突合并语义 |
| | `BatchUpsert` / `BatchUpsertWith` | 批量版 |
| SELECT | `SelectByPK` / `SelectByPKForUpdate` | 按主键查整行（后者带 `FOR UPDATE` 行锁） |
| | `SelectWhere(where, args, opts)` | 条件查询 + `ORDER BY` / `LIMIT` / `OFFSET` / `FOR UPDATE` |
| | `SelectColumns(cols, ...)` | 只查指定列（列名校验+转义） |
| | `SelectByKVIn` / `SelectByPKIn` | `IN (?, ?, ...)`，占位符按取值个数展开 |
| | `Count` / `Exists` / `ExistsByPKForUpdate` | `COUNT(*)` / `SELECT 1 ... LIMIT 1` |
| UPDATE | `UpdateByPK` / `UpdateWhere` | 更新已赋值字段 |
| | `UpdateByPKIf(m, guard, args)` | CAS：`WHERE pk = ? AND <guard>` |
| | `UpdateFieldsByPK(m, cols...)` | 只更新指定列（零值也照写，用于清零） |
| | `UpdateAssignsByPK` / `UpdateAssignsWhere` | 表达式更新，如 `gold = gold + ?` |
| | `IncrByPK` / `DecrByPKIfEnough` | 原子加 / 够才扣（`AND col >= ?`，防负数） |
| DELETE | `DeleteByPK` / `DeleteByPKIf` / `DeleteWhere` | 按主键 / 带守卫 / 按条件删除 |
| | `DeleteWhereLimit(where, args, orderBy, n)` | 有界批删，保留期清理用 |
| | `DeleteByKVIn` / `DeleteByPKIn` | `IN (?, ?, ...)` 批删 |
| 其它 | `TableName` / `Table` / `CreateTable` / `PrimaryKeyWhere` | 表名 / 底层映射 / 建表语句 / 主键 WHERE 片段 |

### 赋值子句 Assign

`UpsertWith` / `UpdateAssignsByPK` / `UpdateAssignsWhere` 的更新部分由 `Assign` 描述，
构造函数分两类：

通用（UPDATE 和 UPSERT 都可用）：

| 构造 | 产出 |
|------|------|
| `SetCol(col, val)` | `col = ?` |
| `AddCol(col, delta)` | `col = col + ?` |
| `SubCol(col, delta)` | `col = col - ?` |
| `SetColExpr(col, expr, args...)` | `col = <expr>`，如 `"NOW()"` |

仅用于 UPSERT（`VALUES(col)` = 本次本该插入的新值）：

| 构造 | 产出 | 语义 |
|------|------|------|
| `SetNew(col)` | `col = VALUES(col)` | 覆盖 |
| `AddNew(col)` | `col = col + VALUES(col)` | 累加 |
| `MinNew(col)` / `MaxNew(col)` | `LEAST` / `GREATEST` | 取最值（退避时间取更早 / 水位只增不减） |
| `SetNewIfZero(col)` | `col = IF(col = 0, VALUES(col), col)` | 首写生效，已写过不覆盖 |
| `KeepOld(col)` | `col = col` | 不改数据，只拿行锁 |

```go
// 冲突时：代次 +1、jti 换新、首次落的时间戳不被覆盖
stmt, err := b.UpsertWith(row,
    proto2mysql.SetColExpr("generation", "`generation` + 1"),
    proto2mysql.SetNew("sess_jti"),
    proto2mysql.SetNewIfZero("first_seen_ms"))

// 余额够才扣，靠 RowsAffected 判定成败
stmt, err = b.DecrByPKIfEnough(row, "gold", 100)

// 保留期清理：小批量循环删到 RowsAffected < limit
stmt, err = b.DeleteWhereLimit("`created_at` < ?", []interface{}{cutoff}, "", 1000)
```

### 注意事项

- 返回的 SQL **不带结尾分号**，`Args` 与 `?` 一一对应，直接给 `Exec` / `Query` 用；
- 列名一律校验存在于该 message 并加反引号转义，未知列返回 `ErrFieldNotFound`；
- **UPDATE / DELETE 不接受空条件**：`UpdateWhere` / `UpdateAssignsWhere` / `DeleteWhere` /
  `DeleteWhereLimit` 传空串会返回 `ErrEmptyWhereClause`，而不是悄悄退化成整表操作。
  确需操作全表时必须显式传 `"1=1"`，让危险意图在代码评审里看得见。
  （SELECT 侧的 `SelectWhere` / `Count` / `Exists` 没有这个限制，空条件仍按全表查询处理。）
- `whereClause`、`QueryOptions.OrderBy`、`DeleteWhereLimit.orderBy`、
  `UpdateByPKIf` / `DeleteByPKIf` 的 guard、以及 `SetColExpr` 的表达式都是
  **原样拼接的裸 SQL**，只能来自代码常量，取值一律走 `Args`；
- `UpsertAdd` 必须显式指定数值列；不传列或传入 string/BLOB 等非数值列会返回错误，
  防止 MySQL 隐式数值转换写坏数据；
- **proto3 零值 = 未赋值**：`InsertSetFields` / `UpdateByPK` 只写 `Has()` 为真的字段，
  标量字段值为 `0` / `""` / `false` 时会被跳过。要显式写零值，把字段声明为 `optional`，
  或改用 `Insert` / `UpdateFieldsByPK`；
- `FOR UPDATE` 只在事务内有意义（事务外单句自动提交，锁立即释放）；
- CAS 类语句执行后应检查 `RowsAffected`。其中正数扣减/删除返回 0 表示条件未命中；
  `UpdateByPKIf` 在默认 MySQL 配置下返回 0 还可能表示“条件命中但新旧值相同”。必须区分时，
  让语句同时递增版本列，或启用 `clientFoundRows=true` 后按匹配行数判断；
- `VALUES()` 在 MySQL 8.0.20 起被标记 deprecated（官方建议改 `AS new` 行别名），至今仍可用，
  本库沿用它以兼容 5.7。

## 类型映射

| Protobuf 类型 | MySQL 类型 | 说明 |
|--------------|-----------|------|
| int32        | int NOT NULL DEFAULT 0 | - |
| uint32       | int unsigned NOT NULL DEFAULT 0 | - |
| int64        | bigint NOT NULL DEFAULT 0 | - |
| uint64       | bigint unsigned NOT NULL DEFAULT 0 | - |
| float        | float NOT NULL DEFAULT 0 | - |
| double       | double NOT NULL DEFAULT 0 | - |
| bool         | tinyint(1) NOT NULL DEFAULT 0 | - |
| string       | MEDIUMTEXT；在主键/唯一键里为 VARCHAR(N) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' | 键列 N 默认 191，见「字符串键（第三方 ID）」 |
| bytes        | MEDIUMBLOB；在主键/唯一键里为 VARBINARY(N) NOT NULL DEFAULT '' | 原样存储，不做编码 |
| enum         | int NOT NULL DEFAULT 0 | 存储枚举值的数字表示 |
| message      | MEDIUMBLOB | proto wire 格式**裸字节** |
| map          | MEDIUMBLOB | proto wire 格式**裸字节** |
| repeated     | MEDIUMBLOB | proto wire 格式**裸字节** |
| Timestamp    | DATETIME(6) **（恒定可空）** | 保留到微秒；未设置时落 SQL NULL |

### Timestamp 的两条硬约束

**① 列类型是 `DATETIME(6)`，不是 `DATETIME`。**
`DATETIME` 等价 `DATETIME(0)`，写入带毫秒/纳秒的时间会被**静默**截断到整秒——不报错、无警告。
`DATETIME(6)` 保留到微秒；proto `Timestamp` 的纳秒位仍会丢，那是 MySQL 时间类型的硬上限。

存量表（本特性之前建的）停在 `DATETIME(0)`，`UpdateTableField` / `SyncAllTables` 会自动生成
`MODIFY COLUMN ... DATETIME(6)` 把它升上来；也可以手动：

```sql
ALTER TABLE `表名` MODIFY `列名` DATETIME(6) NULL;
```

> 升级前的过渡期请注意：新版本会下发带小数秒的字符串，写进还没迁移的 `DATETIME(0)` 列时
> MySQL 会**四舍五入**（`.9` 进位到下一秒），而不是像旧版本那样在 Go 侧截断。

**② Timestamp 列恒定允许 NULL，不受 `nullable` 选项影响。**
proto 的 message 字段天然是"有/无"两态，而 DATETIME 没有可用的零值——`'0000-00-00'` 在
`NO_ZERO_DATE` 下非法，空串在 `STRICT_TRANS_TABLES` 下直接被拒
（`Error 1292 Incorrect datetime value: ''`）。若声明成 `NOT NULL`，凡是没给该字段赋值的行
**整行都插不进去**，等于把这列变成必填。因此未设置的 Timestamp 一律下发 SQL `NULL`。

### 浮点数：NaN / ±Inf 会被拒绝

MySQL 的 `FLOAT`/`DOUBLE` 没有 NaN/Inf 的表示。写入时本库直接返回 `pbconv.ErrNonFiniteFloat`，
不把问题丢给 MySQL——在 `STRICT` 模式下它只会报 `Error 1265 Data truncated`（完全看不出根因），
非 `STRICT` 模式下更糟：悄悄存成 `0`，成为静默的数据损坏。

### 二进制字段的存储格式（裸字节，不是 Base64）

`bytes` / 嵌套 message / `map` / `repeated` 一律以 **proto wire 格式的裸字节**落库，
与手写 `proto.Marshal(v)` 后直接 `Exec(sql, blob)` 的结果**逐字节相同**。因此这些列可以和
不经本库的手写 SQL 混用：别处写的行本库读得出来，本库写的行别处也读得出来。

不做 Base64 的原因：目标列是 `MEDIUMBLOB`/`VARBINARY`，本身二进制安全，Base64 只会白白多占 33% 体积
（实测 8238 B → 10984 B），并在每次读写上加一次编解码与一次分配。要在 SQL 控制台查看内容，
用 MySQL 自带的 `TO_BASE64()` 即可，不必为此付出存储代价：

```sql
SELECT TO_BASE64(`player`) FROM `golang_test` WHERE `id` = 1;
```

> ⚠️ **列类型必须是二进制类型**。裸字节写进 utf8mb4 的 `TEXT` / `VARCHAR` 列会因非法 UTF-8
> 被拒或损坏。本库建表时这几类字段映射为 `MEDIUMBLOB`（主键/唯一键里的 `bytes` 为 `VARBINARY`），
> 只有手工建的表才可能踩到。

> ⚠️ **从 Base64 版本升级**：早期版本把这些字段编码成 Base64 落库。升级后写入格式变了，
> 存量行必须先就地还原，否则读出来会 `cannot parse invalid wire-format data`：
>
> ```sql
> UPDATE `表名` SET `列名` = FROM_BASE64(`列名`);
> ```
>
> 迁移期间**不要**让新旧两个版本同时读写同一张表（新版本写的裸字节，旧版本会当 Base64 解码
> 而失败）。要灰度并存，得先加新列双写、读侧兼容两种格式，验证后再切、再删旧列。

## 字符串键（第三方 ID）

第三方登录的账号 ID（例如 Google 的 `sub`：最长 255 个区分大小写的 ASCII 字符）、订单号这类字符串，
可以直接放进主键或唯一键。**放进键里**的标量 `string` / `bytes` 不再映射成 `MEDIUMTEXT` / `MEDIUMBLOB`：

| 字段 | 列类型 | 比较语义 |
|---|---|---|
| `string` | `VARCHAR(N) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''` | 按码点比较：区分大小写，尾部空格参与比较（NO PAD） |
| `bytes` | `VARBINARY(N) NOT NULL DEFAULT ''` | 逐字节比较：`'a'`、`'a '`、`'a\0'` 是三个不同的键 |

索引建在**整列**上（这一列出现在普通索引里时也是整列）。原先的 `MEDIUMTEXT` 只能建 191 前缀索引，
唯一性只覆盖前 191 个字符；`utf8mb4_unicode_ci` 会把 `'AbC'` 与 `'abc'` 判为重复，`utf8mb4_bin` 是 PAD SPACE，
会把 `'abc'` 与 `'abc '` 判为重复——三者都会让两个不同的外部账号撞成同一个键。
不在主键/唯一键里的 string/bytes 映射不变。

### 推荐形态：数值代理主键 + 唯一键

```protobuf
message third_party_account {
  option (proto2mysql.table_name)         = "third_party_account";
  option (proto2mysql.primary_key)        = "id";
  option (proto2mysql.auto_increment_key) = "id";
  option (proto2mysql.unique_key)         = "provider,provider_id";

  uint64 id          = 1;
  string provider    = 2 [(proto2mysql.max_length) = 32];
  string provider_id = 3 [(proto2mysql.max_length) = 255];  // Google sub 最长 255 个字符
  uint64 player_id   = 4;
}
```

内部关联（背包、邮件、好友）用 `uint64 id`，外部 ID 只在登录时按 `(provider, provider_id)` 查一次。
也可以直接用 string 主键（`primary_key = "provider_id"`），DDL 与写入校验完全相同。

### max_length、字节预算与写入校验

- N 默认 191；`string` 取 1..768（按字符计），`bytes` 取 1..3072（按字节计）。proto 里写
  `[(proto2mysql.max_length) = 255]`，代码里用 `WithMaxLength("provider_id", 255)`（按字段合并，覆盖 proto 声明）。
- `max_length` 目前只能写在主键/唯一键的 string/bytes 字段上；写在其它字段、显式写 0 或越界都返回 `ErrInvalidTableOption`。
- 单个索引（主键、唯一键、每个普通索引**各自**计算）所有列合计不超过 3072 字节：`VARCHAR(N)` 按 4N 计、
  `VARBINARY(N)` 按 N 计，int32/uint32/enum/float 计 4，int64/uint64/double/Timestamp 计 8，bool 计 1，
  普通索引里不在键中的 string/bytes 走 191 前缀（分别计 764/191）。超出时建表前报错并列出每列占用。
- 写入前按 N 校验：string 按字符数（4 字节的 emoji 算 1 个）且必须是合法 UTF-8，bytes 按字节数。
  不合格返回 `ErrInvalidKeyValue`，**在任何 SQL 发出之前**失败，不会被截断成另一个键；
  批量接口先校验整批再写第一批。错误信息只带表、列、实际长度与上限，不回显值。
- 键列不能声明 `nullable`：写入路径从不给 string/bytes 写 NULL（未赋值写 `''`），可空并不能让未赋值的行不参与唯一性。

### 版本要求

- MySQL ≥ 8.0.17（`utf8mb4_0900_bin` 从这个版本开始提供）。
- TiDB 需要新排序规则框架（`new_collations_enabled_on_first_bootstrap`，新集群默认开启，只能在集群初始化时决定）。
- MySQL 5.7 没有 `utf8mb4_0900_bin`：字符串键请声明成 `bytes` 字段（`VARBINARY` 逐字节比较）。

### 限制

- **不可降级**：新版本建出（或迁移后）的 `VARCHAR/VARBINARY` 键列，旧版本同步时会报 `ErrSchemaDrift`
  （旧版本期望 `MEDIUMTEXT` + 191 前缀），含字符串键的表不能再回滚到旧版本。
- 给已有多行数据的表**新增**一个非空 string 唯一键列时，旧行在新列上全是 `''`，补唯一键会撞 1062——
  与新增数值唯一键列（旧行全是 0）是同一类问题，需要先回填再加键。

### 旧表迁移

旧版本给唯一键里的 string 建的是 `MEDIUMTEXT` 可空列 + `(191)` 前缀索引。新版本同步到这类表（以及 `*_ci`、
`utf8mb4_bin` 排序规则的 VARCHAR 键列）时返回 `ErrSchemaDrift` + `ErrLegacyKeyColumn`，**不执行任何 DDL**；
错误信息里有逐列原因、迁移步骤和按实际表名填好的 SQL。步骤与实测过的 SQL 见
[docs/schema-evolution.md](docs/schema-evolution.md#legacy-key-columns)。

## 配置选项

通过 `TableOption` 函数可以配置表的各种属性：

- `WithPrimaryKey(keys ...string)`: 设置主键字段；仅接受具有稳定等值身份语义、可完整索引且为
  `NOT NULL` 的字段。标量 string/bytes 映射为 `VARCHAR`/`VARBINARY` 整列索引，可以做主键（见「字符串键」）；
  repeated/map/message 只能落 BLOB 前缀索引，float/double 的十进制、二进制与数据库比较语义不适合作为
  稳定身份，都会在 DDL 前被拒绝
- `WithIndexes(indexes ...string)`: 设置普通索引
- `WithUniqueKey(uniqueKey string)`: 设置唯一键；其中的 string/bytes 同样映射为 `VARCHAR`/`VARBINARY` 整列
- `WithAutoIncrementKey(key string)`: 设置自增字段
- `WithNullableFields(fields ...string)`: 设置允许为 NULL 的字段（主键/唯一键里的 string/bytes 不能可空）
- `WithMaxLength(field string, n uint32)`: 主键/唯一键里 string/bytes 字段的列宽，按字段合并，覆盖 proto 的 `max_length`
- `WithTiDBNonclusteredPK()` / `WithTiDBShardRowIDBits(bits)` / `WithTiDBPreSplitRegions(n)` / `WithTiDBAutoIDCacheOne()`: TiDB 方言，见下节

这些选项在 .proto 里都有等价写法（定义见 `proto/proto2mysql_option.proto`）：`table_name` / `primary_key` /
`auto_increment_key` / `index` / `unique_key` / `tidb_*` 是 message option，`nullable` / `max_length` 是 field option。

## TiDB 支持

本库生成的 DML（`INSERT` / `INSERT ... ON DUPLICATE KEY UPDATE` / `REPLACE INTO` / `SELECT ... FOR UPDATE`）与多子句 `ALTER TABLE`（TiDB v6.2+）在 TiDB 上直接可用，无需改动。需要额外处理的是**建表**：

**写热点**。业务自赋值的单调递增主键（如 Snowflake，时间戳在高位）在 TiDB 默认聚簇表下会把写入集中到单个 region（官方点名的热点场景），且 `SHARD_ROW_ID_BITS` 对聚簇表无效。标准解法是非聚簇主键 + 打散 + 预切分：

```proto
message player_data {
  option (proto2mysql.table_name)             = "player_data";
  option (proto2mysql.primary_key)            = "player_id";
  option (proto2mysql.tidb_nonclustered_pk)   = true; // 主键 NONCLUSTERED（代价：点查多一次回表）
  option (proto2mysql.tidb_shard_row_id_bits) = 4;    // 按 _tidb_rowid 打散，建议 log2(TiKV 节点数)
  option (proto2mysql.tidb_pre_split_regions) = 4;    // 建表即预切 region，须 ≤ shard_row_id_bits

  uint64 player_id = 1;
  bytes  data      = 2;
}
```

生成的 DDL 用 TiDB 扩展注释语法（`/*T!...*/`），**MySQL 视为普通注释忽略**，同一份建表语句在 MySQL 与 TiDB 上都能执行：

```sql
CREATE TABLE IF NOT EXISTS `player_data` (
  `player_id` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'pb:1',
  `data` MEDIUMBLOB COMMENT 'pb:2',
  PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci /*T! SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4 */ COMMENT='player_data';
```

自增表如依赖 ID 近似连续（TiDB 默认按批缓存分配，多节点非连续、重启跳号），加 `option (proto2mysql.tidb_auto_id_cache_one) = true;`（v6.4+ 集中分配）。

**集群侧注意**（与本库无关但必须配置）：TiDB 单行 KV 默认上限 6MB（`txn-entry-size-limit`），本库的 MEDIUMBLOB 列可存到 16MB，大 blob 场景必须调大该配置，否则写入报 `entry too large`。

## 注意事项

1. 批量插入的最大条数默认为 1000，可以通过修改 `BatchInsertMaxSize` 常量调整
2. Protobuf 消息中的 `repeated` 字段用于批量查询时，需要定义一个包含该字段的消息（如示例中的 `UserList`）
3. 所有字段名会自动检测是否与 MySQL 关键字冲突，冲突时会自动添加反引号包裹

## 许可证

[MIT](LICENSE)
