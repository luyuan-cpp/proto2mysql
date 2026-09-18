# 表结构怎么安全地演进

> 面向所有人。不需要懂 protobuf 内部，也不需要懂 MySQL 的 DDL 细节。

## 一、先建立一个直觉：为什么这件事很难

你在 `.proto` 里加了一个字段，重启服务，库自动帮你 `ALTER TABLE` 加了一列。
听起来天经地义，对吧？

**问题在于"重启服务"这四个字。** 现代服务几乎都不是一次性全部重启的，
而是**一台一台换**（滚动发布），或者**先换一小部分观察**（金丝雀发布）。
于是有一段时间，新旧两个版本的进程**同时在线**。

而这个库的自动建表逻辑是这样的：

```
每个进程启动时：
    读线上表的实际结构
    对比自己 .proto 里写的结构
    不一样 → 发一条 ALTER，把线上改成自己这份
```

注意最后一句：**把线上改成"自己这份"**。

它没有"我是新版本 / 我是旧版本"的概念，也没有 schema 版本号。
在每个进程眼里，自己的 .proto 就是唯一正确的答案。

于是新旧两个版本同时在线时，就变成了两个人抢方向盘。

## 二、三个真实的事故场景

### 场景 1：改个字段名，两个版本来回改（已修复）

你把 `.proto` 里的 `ip` 改名成 `addr`（字段号还是 2，没变）：

```protobuf
// v1
string ip   = 2;
// v2
string addr = 2;
```

库能靠字段号认出这是改名，生成 `CHANGE COLUMN`，数据保留——这是本库的招牌特性，很好用。

但滚动发布时会变成这样：

```
时刻 1  v2 的实例启动  → CHANGE COLUMN `ip` → `addr`     ✅ 数据保留，符合预期
时刻 2  v1 的某个实例重启（扩容 / 被调度器挪了 / OOM 拉起）
        → 它看到线上有一列注释是 pb:2 但名字不叫 ip
        → CHANGE COLUMN `addr` → `ip`                    ❌ 改回去了
时刻 3  v2 的所有 SQL 立刻报 Error 1054 Unknown column 'addr'
时刻 4  v2 重启 → 又改成 addr → v1 再改回 ip → ...
```

**注意时刻 2 的可怕之处：v1 什么数据都没写，它只是重启了一下。**
这不是"回滚"，是**乒乓球**——每次有 pod 重启就翻一次面。

### 场景 2：把 uint32 拓宽成 uint64，被旧版本改回去（已修复）

```protobuf
// v1
uint32 port = 3;   →  MySQL: int unsigned
// v2
uint64 port = 3;   →  MySQL: bigint unsigned
```

```
v2 启动 → MODIFY COLUMN `port` bigint unsigned    ✅ 拓宽
v1 重启 → MODIFY COLUMN `port` int unsigned       ❌ 收窄回去，超过 42 亿的值全部截断
```

同样只需要 v1 重启一下，不需要写任何数据。

**同类的还有文本列。** 这条在真实项目里发生过：

> 2026-08-19：某个库的 `nickname` 列本来是 `MEDIUMTEXT`，被另一侧的服务按
> `varchar(255)` 重建了。结果同一个玩家的同一次改名，**落到宽列副本就成功、
> 落到窄列副本就报 Error 1406**，而且**不可复现**——因为取决于请求打到了哪台机器。

### 场景 3：字段号复用，数据被隐式类型转换吃掉（已修复）

```protobuf
// v1
string legacy_note = 4;      // 后来这个字段不要了，从 proto 里删掉

// v2 —— 有人把 4 号让给了新字段
int64  score       = 4;      // ← 复用了 4 号
```

库按字段号认列，看到线上有一列注释是 `pb:4`、名字不叫 `score`，
于是认为"这是改名"，生成：

```sql
CHANGE COLUMN `legacy_note` `score` bigint NOT NULL DEFAULT 0
```

MySQL 会做**隐式类型转换**：把一列文本转成 bigint —— 转不动的全变成 `0`。
整列数据没了。

**「本库从不 DROP COLUMN」的保护在这里完全帮不上忙**，因为丢的不是列，是列里的内容。

> **铁律：protobuf 的字段号是身份证，永不复用。**
> 删字段要用 `reserved` 把编号占住：
> ```protobuf
> reserved 4;
> reserved "legacy_note";
> ```

## 三、业界标准做法：Expand → Migrate → Contract

这套方法有个名字，叫 **Expand-Migrate-Contract**（也叫 Parallel Change）。
它的全部道理只有一句话：

> **代码能回滚，schema 不能回滚。**

代码回滚是秒级的、无损的；而 schema 回滚意味着 `DROP COLUMN`，
那会**永久删掉**新版本期间写入的数据。所以正确的姿势不是"schema 也能回滚"，
而是——**让 schema 只前进，并且前进的每一步都对旧版本无害**。

三个阶段：

| 阶段 | 能做什么 | 不能做什么 |
|---|---|---|
| **Expand（扩展）** | 加列（必须可空或有默认值）、加索引、加表 | 任何删除或收紧 |
| **Migrate（迁移）** | 双写、把老数据回填到新列 | — |
| **Contract（收缩）** | 删列、改类型、加 NOT NULL 约束、删表 | — |

**铁律：一次发布只能做 Expand 或 Contract 中的一种，永远不能同时做。**
Contract 必须滞后至少一个完整发布周期——要等到你确定不会再回滚到旧版本。

### 举个完整的例子：把 `ip` 改名成 `addr`

❌ **错误做法**（一步到位）：直接把 proto 里的 `ip` 改成 `addr`。
→ 就是上面的场景 1，乒乓球。

✅ **正确做法**（三次发布）：

```protobuf
// 第 1 次发布（Expand）：加新列，旧列留着
string ip   = 2;      // 老列，还在写
string addr = 7;      // 新列，用一个没用过的字段号
```
代码同时写 `ip` 和 `addr`，读的时候优先 `addr`、回退 `ip`。
此时 v1（只认识 ip）和 v2（两个都认识）**都能正常跑**。

```
// 第 2 次发布（Migrate）：跑一次回填，把历史行的 ip 拷进 addr
// 确认 addr 已经全量有值，且所有实例都是新版本
```

```protobuf
// 第 3 次发布（Contract）：删掉旧列
reserved 2;
reserved "ip";
string addr = 7;
```
（本库不会自动 DROP，`ip` 那列会留成孤儿列，需要人工执行 `ALTER TABLE ... DROP COLUMN ip`。）

麻烦吗？麻烦。但这是**唯一**能在不停机的前提下安全改名的办法。

## 四、这个库现在提供的三道闸

### 闸 1：`ExpandOnly` —— 滚动发布必须打开

```go
db := proto2mysql.NewDB()
db.ExpandOnly = true          // 滚动/金丝雀发布必须打开
if err := db.SyncAllTables(); err != nil {
    log.Fatalf("结构同步失败: %v", err)   // 拒绝启动，别带着半吊子结构跑
}
```

打开之后，任何**不是纯新增**的结构变更都会被拒绝，并把具体语句打印出来：

```
ErrExpandOnlyViolation: 表 player 的本次对齐含非「纯新增」变更，ExpandOnly 下拒绝执行：
  MODIFY COLUMN `port` bigint unsigned NOT NULL DEFAULT 0
  CHANGE COLUMN `ip` `addr` MEDIUMTEXT COMMENT 'pb:2'
  这些语句在滚动发布下会被新旧副本来回执行。请改成 expand→migrate→contract 三步，
  或人工审核后单独执行。
```

**为什么 `ADD COLUMN` 可以放行、`MODIFY`/`CHANGE` 不行？**

因为旧版本的 SQL 里**根本不会出现新列的名字**。库生成的所有 SQL 都是按
自己 .proto 的字段列显式列出来的，从不用 `SELECT *`：

```sql
-- v1 发出的语句，它压根不知道有 foo 这一列
SELECT `id`, `name`, `gold` FROM `player` WHERE `id` = ?
UPDATE `player` SET `name` = ?, `gold` = ? WHERE `id` = ?
```

所以 v2 加的列，对 v1 是**完全不可见**的——既不会读到，也不会被覆盖。
这是"加列绝对安全"的机制保证，不是运气。

**默认是关的**，因为单进程部署时改名/改类型是完全正当的操作，
关掉它会毁掉本库最好用的特性之一。

### 闸 2：不再收窄（无需配置，永远生效）

现在库对四类「同族类型」做了容量排序，**线上比目标宽时一律不动它**：

```
整数    tinyint < smallint < mediumint < int < bigint
文本    char < varchar < tinytext < text < mediumtext < longtext
二进制  binary < varbinary < tinyblob < blob < mediumblob < longblob
浮点    float < double
```

| 线上是 | proto 要 | 行为 |
|---|---|---|
| `bigint` | `int` | 不动（收窄会丢数据） |
| `int` | `bigint` | ALTER 拓宽 |
| `mediumtext` | `varchar(255)` | 不动 |
| `varchar(255)` | `mediumtext` | ALTER 拓宽 |

确实需要收窄（比如为了省空间）？请人工写 `ALTER`，库不替你做这个决定。

### 闸 3：字段号复用直接拒绝（无需配置，无法关闭）

线上那列的类型与新字段**跨族**（如 `mediumtext` vs `bigint`）时，
库判定这不是改名而是字段号被复用了，直接报错：

```
ErrFieldNumberReused: 表 player 的列 legacy_note（mediumtext，pb:4）与新字段
score（bigint NOT NULL DEFAULT 0，pb:4）类型跨族，无法当作改名处理。
  字段号是 protobuf 的身份，**永不复用**：删字段请用 reserved，新字段另取一个没用过的编号。
  若确实要把这一列的数据转成新类型，请人工写 ALTER 并自行确认转换语义。
```

这条**没有开关**，因为字段号复用没有任何正当用途。

## 五、DDL 由谁来执行

大厂的标准做法是：**DDL 不由业务进程在启动时执行**。理由有四条：

1. N 个副本同时启动 = N 条并发 `ALTER`
2. 大表 `ALTER` 会让服务起不来，撞健康检查，把滚动发布卡死
3. 回滚代码不会回滚 schema
4. 每个业务进程都有改表权限，本身是个安全问题

标准形态是：一个**独立的迁移步骤**（k8s Job / CI 流水线的一环 / 人工闸门），
跑在服务发布**之前**，成功了才允许滚动业务进程。

### 如果你不想写迁移脚本

本库提供一条折中路线：**让它生成迁移 SQL，但不执行**。

```go
// 只产出 SQL，不连库执行——交人工/CI 审核后再由迁移步骤执行
sql, err := db.GenerateMigrationSQL(&pb.Player{})
err = db.DumpMigrationSQLFile("migrations/0007_add_addr.sql", &pb.Player{}, &pb.Guild{})
```

你依然不用手写迁移脚本（脚本是**生成**的），但拿回了三样东西：
**唯一执行者、可控时机、出错时能在发布前拦下来**。

### 如果你就是要在启动时自动同步

也可以，但请把 DDL **收敛到一个进程**：

```
进程表：
  migrator   ← 排第一，只有它连的账号有 DDL 权限，跑完 SyncAllTables 就退出
  ↓ 等它成功退出
  服务 A / 服务 B / ...  ← 并行起，用只有 DML 权限的账号
```

库自己也加了一层保护：`SyncAllTables()` 全程持一把 MySQL 咨询锁
（`GET_LOCK`），同一时刻只有一个进程在改结构。拿不到锁**不阻断**，
只降级为无锁执行 + 一条告警——因为 TiDB 等兼容实现不一定支持这个函数，
而"因为拿不到锁就拒绝启动"是把并发问题升级成了可用性事故。

## 六、上线前检查清单

- [ ] 滚动 / 金丝雀发布 → `ExpandOnly = true` 已打开
- [ ] 本次 proto 改动只有**加字段**，没有改名 / 改类型 / 删字段
- [ ] 删掉的字段都用 `reserved` 占住了编号
- [ ] 没有复用过任何历史字段号
- [ ] 代码里没有用 `GetReplaceSQLWithArgs` / `GetBatchReplaceSQLWithArgs`（见 [api-safety.md](api-safety.md)）
- [ ] 开了缓存的话，`ttl` 是有限值，不是永不过期（见 [cache.md](cache.md)）
- [ ] DDL 只由一个进程执行（migrator / 迁移 Job），或至少确认锁生效
- [ ] 主键/唯一键里有 string/bytes 的旧表已按下面第七节迁移，并且确认不会再回滚到旧版本（不可降级）

<a id="legacy-key-columns"></a>

## 七、主键/唯一键里 string/bytes 列的旧形态迁移

主键/唯一键里的 `string` / `bytes` 映射成 `VARCHAR(N) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT ''` /
`VARBINARY(N) NOT NULL DEFAULT ''` 整列索引。线上如果还是下面这些形态，同步（`SyncAllTables` /
`UpdateTableField` / `GenerateMigrationSQL` / GORM 的 `CreateOrUpdateTable`）返回
`ErrSchemaDrift` + `ErrLegacyKeyColumn`（两个都能 `errors.Is`），**本次不执行任何 DDL**：

| 线上形态 | 通常从哪来 | 问题 |
|---|---|---|
| `MEDIUMTEXT` 可空 + `UNIQUE (col(191))` | 旧版本本库给唯一键里的 string 建的 | 唯一性只覆盖前 191 个字符 |
| `MEDIUMBLOB` + 前缀唯一索引 | 旧版本本库给唯一键里的 bytes 建的 | 唯一性只覆盖前 191 个字节 |
| `VARCHAR` + `utf8mb4_unicode_ci` 等 `*_ci` | 手工建表、别的工具 | 不区分大小写：`'AbC'` 与 `'abc'` 撞键 |
| `VARCHAR` + `utf8mb4_bin` | 手工建表 | PAD SPACE：`'abc'` 与 `'abc '` 撞键 |

**为什么不自动迁移**：要先处理 NULL（多行 NULL 改成 `''` 后会互相撞唯一键）、确认没有超长值、
删掉前缀索引再按整列重建，TiDB 聚簇主键还根本不允许原地改——这些都得先看过数据再做，
而且在滚动发布下自动执行会被新旧副本来回翻面。

错误信息里带着逐列原因、步骤和**按实际表名、列名、索引名填好的 SQL**，只有下面两种形态，
都已在 MySQL 与 TiDB v8.5 上实测可以执行（见 `string_key_integration_test.go`，
测试直接执行从错误信息里取出的 SQL，再同步验证零漂移）。

SQL 分成两个块：

- **执行前的人工核对**：只读查询，结果要人看过才能往下走（下面「预检」一节）。
- **可直接执行的 SQL**：必须在**同一个会话**里按顺序逐条执行——块里有 `SET SESSION`、用户变量与
  `PREPARE`，换一条连接（包括用连接池逐条 `Exec`）就丢了。

### 预检（两种形态都要做）

核对查询已按实际表名、列名填好，形如：

```sql
-- 每个旧形态列一条：装不装得下、有多少行是 NULL
SELECT MAX(CHAR_LENGTH(`col`)) AS max_chars, SUM(`col` IS NULL) AS null_rows, COUNT(*) AS rows_total
  FROM `t` /* 目标列 col 上限 191 */;   -- bytes 列用 MAX(LENGTH(`col`))
```

- `max_chars` / `max_bytes` 超过目标长度时先人工处理：迁移块第一条已经把会话切成严格模式
  （`SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'STRICT_ALL_TABLES')`），
  装不下是**报错中断**而不是静默截断——非严格模式下截断后的数据会被 `RENAME` 成正式表，不可回滚。
- `null_rows` **只要不是 0** 就先人工给它们赋唯一值或删除：NULL 改写成 `''` 后不只会互相撞键，
  还会与表里**已有的** `''` 撞——所以一行 NULL 也可能撞。同时查一下已有的空串有几行：
  `SELECT COUNT(*) FROM \`t\` WHERE \`col\` = ''`。这一步不处理，原地 ALTER 路径会在
  `ADD UNIQUE KEY` 撞 Error 1062，影子表路径会在 `INSERT ... SELECT` 撞 Error 1062，
  都是在维护窗口里中途失败（影子表路径此时已经建了影子表，要先 `DROP` 掉再从头来）。

从前缀唯一 / `*_ci` / PAD SPACE 换成整列、区分大小写、NO PAD，唯一性只会变宽松，已有数据不会因此出现新的重复；
会撞键的只有 NULL 改写成 `''` 这一步。

### 形态一：旧形态列只在唯一键/普通索引里——原地 ALTER

必须拆成独立语句：TiDB 不允许在同一条 ALTER 里 DROP 后复用同一个索引名（Error 1061），
也不允许给仍带索引的列改排序规则（Error 8200）；前缀索引也不会随 MODIFY 自动变成整列索引。

```sql
SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'STRICT_ALL_TABLES');
ALTER TABLE `account` DROP INDEX `uk_account`;
UPDATE `account` SET `provider_id` = '' WHERE `provider_id` IS NULL;
ALTER TABLE `account` MODIFY COLUMN `provider_id` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:3';
ALTER TABLE `account` ADD UNIQUE KEY `uk_account` (`provider_id`);
```

- 删索引到重建索引之间唯一性不受约束，请停写或在维护窗口执行。
- 线上若另有包含该列、但不是本库声明的索引，错误信息会带上它的定义（唯一性、列顺序、前缀长度）点名；
  TiDB 上也要先 `DROP INDEX` 掉它们才能 MODIFY，迁移后按那份定义人工重建（本库不会替你动它们）。
- 线上列名与 proto 字段名不同（按 `COMMENT 'pb:N'` 的字段号认出来）时，`MODIFY COLUMN` 会换成
  `CHANGE COLUMN` 一步改名兼改类型，`UPDATE ... IS NULL` 用的也是线上那个名字。
- 本次没有读取线上二级索引时（proto 一条索引都没声明、主键里也没有 string/bytes 键列），错误信息会明确要求
  先 `SHOW INDEX` 核对，不会假装线上没有索引。

### 形态二：旧形态列在主键里——影子表重建

TiDB 聚簇主键既不能 `DROP PRIMARY KEY`，也不能改带索引列的排序规则（先 `CREATE TABLE ... LIKE`
建空表再 MODIFY 也一样是 Error 8200），所以按影子表重建，MySQL 与 TiDB 通用：

```sql
SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@SESSION.sql_mode, ''), 'STRICT_ALL_TABLES');
CREATE TABLE `account__p2m_new` (`provider_id` VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL DEFAULT '' COMMENT 'pb:1', `note` longtext COMMENT 'pb:2', `id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT 'pb:3', PRIMARY KEY (`provider_id`), INDEX `idx_account_0` (`id`), UNIQUE KEY `uk_manual_note` (`note`(191))) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='account';
INSERT INTO `account__p2m_new` (`provider_id`, `note`, `id`) SELECT `provider_id`, `note`, `id` FROM `account`;
/*!80000 SET SESSION information_schema_stats_expiry = 0 */;
SET @p2m_auto_increment = COALESCE((SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'account'), 1);
SET @p2m_rebase_sql = CONCAT('ALTER TABLE `account__p2m_new` AUTO_INCREMENT = ', @p2m_auto_increment);
PREPARE p2m_rebase_auto_increment FROM @p2m_rebase_sql;
EXECUTE p2m_rebase_auto_increment;
DEALLOCATE PREPARE p2m_rebase_auto_increment;
RENAME TABLE `account` TO `account__p2m_old`, `account__p2m_new` TO `account`;
-- 核对数据无误后再手工 DROP TABLE `account__p2m_old`（本库不会替你删）
```

- 建表语句与本库的建表走同一份生成逻辑（索引名、TiDB 方言块、表 COMMENT、字符集都一致），只换了表名；
  索引名保持原表的 `idx_<表名>_N` / `uk_<表名>`，RENAME 回来之后同步才认得出这些索引。
- **本库未声明的二级索引会被带进影子表**：线上存在、proto 没声明的索引（上例的 `uk_manual_note`）按线上定义
  重建，保留原名、唯一性与列顺序；旧形态键列上的前缀长度会去掉（它在影子表里已是 `VARCHAR/VARBINARY` 整列，
  前缀长度 ≥ 列宽会报 Error 1089），其它 TEXT/BLOB 列保留线上的 `SUB_PART`。引用了影子表没有的列、或去掉前缀
  长度后超过单个索引 3072 字节的，无法重建，错误信息会逐个点名。`FULLTEXT`/`SPATIAL`、降序、不可见等属性不还原，
  请按 `SHOW CREATE TABLE` 的输出人工核对。
- **线上比 proto 更宽的列不收窄**：同步路径本来就不会把线上 `bigint`（proto `int32`）、`LONGTEXT`（proto
  `MEDIUMTEXT`）改窄，影子表沿用同一条规则（上例的 `note` 保持 `longtext`），否则严格模式下迁移会中途失败、
  非严格模式下截断后的数据会被 RENAME 成正式表。键列 `max_length` 调小时同理，保留线上宽度。被保留宽度的列
  会在迁移步骤里点名。
- **自增计数器要继承**：影子表的计数器只跟着拷进去的最大值走。旧表 `AUTO_INCREMENT=100001` 而 `MAX(id)=95000`
  （删过尾部行）时，不抬计数器就会把 95001..100000 重新发一遍，撞上按旧 id 存下的外部引用。上面那几条
  （`information_schema_stats_expiry` 只是绕开 MySQL 8 对 `information_schema.TABLES.AUTO_INCREMENT` 的
  24 小时缓存；DDL 不接受表达式，所以用 `PREPARE`）在 MySQL 与 TiDB 上都实测可直接执行。
- **RENAME 只搬表本身**：引用本表的外键会跟着指向 `account__p2m_old`，本表上的触发器也留在那边；`CHECK` 约束、
  `FULLTEXT`/`SPATIAL` 索引、分区同样不进影子表。核对块里的 `information_schema.REFERENTIAL_CONSTRAINTS` 与
  `information_schema.TRIGGERS` 查询就是用来发现它们的，RENAME 之后人工重建。
- 影子表只含 proto 当前声明的列；线上多出来的列（错误信息会点名）要在执行前手工补进建表语句和 INSERT 列表，
  否则它们的数据只留在旧表里。
- 线上列名与 proto 字段名不同时，`INSERT ... SELECT` 的目标列用 proto 名字、源列用线上名字，改名与迁移一步完成。
- 可空的旧形态列在 SELECT 里会写成 `COALESCE(col, '')`。
- 中途任何一步失败：先 `DROP TABLE account__p2m_new` 再从头执行（影子表还没接客，丢弃是安全的）。
- TiDB 单个事务有大小上限，大表请分批拷贝。
- 只在 MySQL 上、且只是排序规则不对（例如 `varchar(191) utf8mb4_unicode_ci` 主键）时，
  直接 `ALTER TABLE ... MODIFY COLUMN ... COLLATE utf8mb4_0900_bin` 也能原地完成（实测 MySQL 通过、TiDB 报 8200）。

迁移完成后再同步应当零漂移、零 ALTER。

### 不可降级

迁移后（以及新版本新建的表）键列是 `VARCHAR/VARBINARY`；旧版本进程按它自己的映射
（`MEDIUMTEXT`/`MEDIUMBLOB` + 191 前缀）处理，拦截点按键的位置不同：

- string/bytes 在**主键**里：旧版本在注册/建表阶段就以 `ErrInvalidTableOption` 拒绝（`MEDIUMTEXT` 只能建
  前缀索引，不能保证完整主键唯一性），`GetCreateTableSQL` 返回空串，根本到不了同步那一步。
- string/bytes 只在**唯一键**里：旧版本能建表，同步时才按 `MEDIUMTEXT` + 191 前缀的期望报 `ErrSchemaDrift`。

含字符串键的表一旦迁移，就不能再回滚到旧版本的库。
另外，给已有多行数据的表**新增**非空 string 唯一键列时，旧行在新列上全是 `''`，补唯一键会撞 1062，
需要先回填再加键（与新增数值唯一键列同类）。
