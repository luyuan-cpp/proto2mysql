# v0.2.0

本版本汇总 v0.1.1 之后的表结构同步、缓存、CRUD 与字符串键列修复，并将开发分支与发布分支统一合入 main。由于包含兼容性变化，版本号提升为 v0.2.0。

## 主要变化

- string/bytes 主键与唯一键使用 VARCHAR/VARBINARY 整列索引，默认长度 191；支持 proto `max_length`、`WithMaxLength` 与 `WithMaxLengths`。
- 字符串键使用 `utf8mb4_0900_bin`，区分大小写和尾部空格；写入、主键查询与缓存键路径统一校验长度和 UTF-8，超限返回 `ErrInvalidKeyValue`。
- 识别旧键列并返回 `ErrLegacyKeyColumn`，提供可检查的迁移 SQL；完善影子表迁移对已有索引、自增属性、宽列、NOT NULL 列、FULLTEXT 与索引键长预算的处理。
- 加固字段号迁移、结构漂移检测、ExpandOnly、并发同步、事务缓存失效与 GORM 路径。
- `Save` 按完整主键保存，备用唯一键冲突返回 `ErrDuplicateKey`，防止更新到其他主键对应的行。
- 保留原本本地主键修复分支和发布分支中的三项回归测试，使用 main 已有的完整键列实现。

## 升级注意事项

- string/bytes 键列默认最多容纳 191 个字符/字节；需要更长键时显式配置长度，并遵守索引总长度限制。
- 字符串键列需要数据库支持 `utf8mb4_0900_bin`。本次发布已验证 MySQL 8.0.46 和 TiDB 8.5.2。
- 旧前缀键、大小写不敏感或 PAD SPACE 排序规则的键列不会被自动改写；请检查现有数据，再按错误信息与[迁移说明](schema-evolution.md)执行迁移。
- 滚动发布期间启用 `ExpandOnly`；涉及已有键列的迁移需要单独安排。
- Go module 路径保持 `github.com/luyuancpp/proto2mysql`，GitHub 仓库位于 `luyuan-cpp/proto2mysql`。

```sh
go get github.com/luyuancpp/proto2mysql@v0.2.0
```

## 本次发布验证

使用 Go 1.26.5，在合并后的 main 上执行：

- 根模块及 `tools/proto2sql` 独立模块：`go test -short -count=1 ./...` 与 `go vet ./...` 均通过。
- 根模块 MySQL 8.0.46 集成测试：685 个测试及子测试通过，0 失败，3 跳过。
- 根模块 TiDB 8.5.2 集成测试：683 个测试及子测试通过，0 失败，5 跳过。
- string 主键、bytes 主键、旧唯一键迁移、旧主键迁移四项核心真实数据库测试在两个后端均通过。

两个后端均跳过未配置的跨语言语料输出及两项 TiDB 多节点集群测试。TiDB 另跳过已有列补自增主键与 FULLTEXT 索引测试，属于后端能力限制。本次未运行多节点 TiDB、Python 跨语言对拍及 race 检测。
