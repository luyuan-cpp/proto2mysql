# v0.3.0（草稿，未发布）

> 状态：代码在 `feat/conditional-save` 分支，**未编译、未测试**。下方「验证」一节的命令全部通过后才合入 main 并打 tag。

本版本只有新增，没有破坏性变化：已有 API 的签名、生成的 SQL 与行为都不变（`Save` 的语句序列一字未改）。

## 新增：`SaveConditional`（带栅栏的整行落库）

```go
func (p *DB) SaveConditional(message proto.Message, opts SaveOptions) (ConditionalSaveResult, error)
func (p *GormDB) SaveConditional(message proto.Message, opts SaveOptions) (ConditionalSaveResult, error)
```

- 用于同一行会被多个进程写入不同版本的场景：只有库内栅栏列（`FenceFields`，uint32/uint64）组成的元组按字典序
  **小于**消息里的值时才写（`AllowEqual` 时为小于等于）。判断与写入是同一条 UPDATE，不依赖外部锁的租约。
- 结局：`SaveUpdated` / `SaveInserted` / `SaveUnchanged` / `SaveRejected`；被拒时 `Stored` 是库内栅栏值，
  同版本重放也是 `SaveRejected` 且 `Stored` 等于消息栅栏，调用方据此判定重复。
- `PreserveFields`：更新已有行时保留这些列的库内值，给不认识栅栏的旧写入方用。
- 语义与 `Save` 同构：完整主键「有则更新、无则插入」，备用唯一键冲突返回 `ErrDuplicateKey` 且不改另一行。
- 常见路径的语句数：Updated 1 条；Rejected 2 条；Inserted 3 条；Unchanged 3 条（撞 1062 时多一次重读）。
  先读状态再 INSERT，拒绝路径只多一条只取栅栏列的小查询，也避开了 RC 显式事务里「两个被拒的写各持 1062 留下的
  S 锁再互相请求 X 锁」的死锁。显式事务里的**并发首插**仍可能 1213（RR 下两个写者落在同一主键间隙即可触发，
  与 `Save` 相同），须整体重试事务。
- 状态查询与整行比较一律 `FOR UPDATE`：强制 current read，并让读写分离层留在主库（发到从库会把旧属主的重放
  误判成已生效）。autocommit 下锁只持续这一条语句。
- 新增错误：`ErrInvalidSaveOptions`（发 SQL 之前的参数校验）、`ErrConditionalSaveRace`（两轮内都撞上并发写者，可重试，
  不包装 `ErrDuplicateKey`）、`ErrSaveMatchMismatch`（两轮都做了整行比较、读到同一状态却改不动，多半是表结构与比较
  规则不一致）。库内栅栏为 NULL 时返回 `ErrSchemaDrift`。1062 后行又消失（被物理删除）时返回包装原 1062 的
  `ErrDuplicateKey`，信息里写明 row disappeared（core 后端可 `errors.As` 出 `*mysql.MySQLError`；gorm 开了
  `TranslateError` 时原始错误已被 gorm 翻译掉，只保证 `errors.Is(err, ErrDuplicateKey)`）。
- 超时与取消：core 用 `WithContext(ctx)`，gorm 用 gorm 自己的 `WithContext`。语句发出后才报错时结局未知。
- 缓存（core）：除 Rejected 外一律失效（事务内推迟到提交后，回滚不删）。出错时也失效，因为语句可能已生效，
  删缓存只多一次未命中。

约束（详见 [api-safety.md](api-safety.md)）：启用栅栏的表只能经 `SaveConditional` 写入，不做物理 DELETE。

## 不在本版本

- Python 侧没有对应实现；本 API 的 SQL 不进跨语言对拍语料（语料仍是 v2）。
- 没有批量版本（BatchSaveConditional），也没有任意上界（FenceBelow）——后者会让栅栏倒退，设计评审中已否决。
- fixed32/fixed64 栅栏列：本库整体不支持这两种类型。

## 测试改动

- `gorm_audit_fixes_test.go` 的审计假驱动新增：记录每条语句的参数（`argsAt`）、按顺序排队查询结果（`queueQuery`）、
  `CheckNamedValue` 让 uint64 原样到达驱动。既有用例不读参数，行为不变。
- 新增 `conditional_save_test.go`（离线，core 与 gorm 各跑一遍）与 `conditional_save_integration_test.go`（真库）。

## 验证（待执行，执行后把结果写回本节）

```sh
go vet ./...
go test -count=1 ./...
cd tools/proto2sql && go test -count=1 ./... && cd ../..
PROTO2MYSQL_INTEGRATION=1 go test -count=1 -run 'SaveConditional' -v .      # MySQL 8.0/8.4
PROTO2MYSQL_INTEGRATION=1 go test -count=1 -run 'SaveConditional' -v .      # TiDB 8.5（换 testdata/db.json 指向）
go test -race -count=1 -run 'SaveConditional' .
```

通过标准：全绿；真库用例**零 SKIP**（`-v` 输出里不得出现 `SKIP: TestSaveConditional`）；既有用例无新增失败。
