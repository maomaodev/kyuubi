# PR #7718 状态备忘：custom session catalog 的 GetSchemas/GetTables 修复

> 个人分析笔记，请勿提交到仓库（AGENTS.md 约定：私人文件不入库，避免 RAT 扫描）。
> 更新于 2026-11-16，基于分支 `kyuubi-7673`（commit `5b7ec6af4`）、
> TBDS Spark 3.5.9 源码（`/Users/maomao/study/idea/531/spark`）与 GitHub 最新状态核对。

## 一、背景：issue #7673

https://github.com/apache/kyuubi/issues/7673

- **环境**：Spark 3.5.6 + Kyuubi 1.9.4 + Iceberg 1.5.2 升级到 Spark 3.5.9 + Kyuubi 1.11.1
  + Iceberg 1.10.0，classpath 去掉了 `iceberg-hive-runtime`（正确做法，PMC 确认）
- **现象**：JDBC `GetTables`（beeline `!tables`）失败，报
  `ClassNotFoundException: org.apache.iceberg.mr.hive.HiveIcebergSerDe / HiveIcebergStorageHandler`。
  只要 catalog 中存在以旧 storage_handler 注册的 Iceberg 表，整个请求即被阻断
- **根因**（PMC pan3793 定性为 Kyuubi 侧 bug）：配置
  `spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog` 后，
  Kyuubi 的 GetTables 仍走内置 v1 `sessionState.catalog`（`HiveExternalCatalog`）路径，
  Iceberg 表被当作普通 Hive SerDe 表批量转换 → 加载 storage handler / serde 类 → ClassNotFound
- **Spark 侧触发点**（TBDS 3.5.9 已核实，`HiveClientImpl.scala:456` `convertHiveTableToCatalogTable`）：
  - `h.getCols` → Hive `MetaStoreUtils.getDeserializer`（serde 类加载）
  - `h.getStorageHandler != null` → Hive `HiveUtils.getStorageHandler`（handler 类加载）
  - 批量 `getTablesByName`（:436-440）与单表 `getTableOption`（:442-447）都经过该转换

## 二、解决方案（PR #7718）

https://github.com/apache/kyuubi/pull/7718

核心：在 `SparkCatalogUtils.getCatalogTablesOrViews` 中区分内置与自定义 session catalog，
自定义时改走 DSv2 路径。三分支结构：

```
分支1  builtin.name() == SESSION_CATALOG && !hasCustomSessionCatalog
       → 仅 Spark 内置 session catalog，v1 路径（master 原行为）
分支2  tc: TableCatalog if tc.name() == SESSION_CATALOG
       → 自定义 session catalog（Iceberg SparkSessionCatalog、DeltaCatalog 等）
       → DSv2 listTables + v1 listViews 探测视图 + Try(loadTable) 兜底
分支3  tc: TableCatalog
       → 普通 v2 catalog（Iceberg SparkCatalog / hadoop_prod 等）
       → 全 TABLE、不应用 tableTypes 过滤（master 原行为，PMC 点 8 要求恢复）
```

关键实现点（`SparkCatalogUtils.scala`，行号基于 `5b7ec6af4`）：

|            机制             |    位置    |                                                                              说明                                                                              |
|---------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hasCustomSessionCatalog` | :109-111 | 检查 `spark.sql.catalog.spark_catalog` 配置，`equalsIgnoreCase("builtin")` 兼容 Spark 4.x 的 `builtin` 魔法值；3.5 中该 conf 为 optional，缺省 `"builtin"` 不会误判                |
| 空 namespace 过滤            | :255-257 | `V2SessionCatalog.listTables` 会带出空 namespace 的本地临时视图（`SessionCatalog.listTables(db)` 默认 `includeLocalTempViews=true`），需过滤 `t.namespace().nonEmpty`           |
| 视图探测                      | :260-274 | 用 v1 `sessionCatalog.listViews(db, pattern)` 探出视图集合（`databaseExists` 守卫 + `filter(_.database.isDefined)`），与 `tc.listTables` 结果按 `(db, name)` 匹配区分 TABLE/VIEW |
| loadTable 兜底              | :295-296 | `Try(tc.loadTable(ident)...).getOrElse("")`，委托回 v1 的表加载失败时 REMARKS 降级为 `""`，不再让整个请求失败                                                                        |

## 三、PMC Review 处理情况

pan3793 于 2026-09-11 给出 17 点意见（3 Blocker + 9 Major + 测试/规范小项），
作者 2026-09-15 force-push（`5b7ec6af4`）逐条修复并回复。要点：

- Blocker：空 namespace 临时视图泄漏（过滤）、loadTable 无守卫（Try 兜底）、
  视图探查误标（只留带 database 的 TableIdentifier）——均已修
- Major：getSchemas 引号（`quote=false`）、`listAllNamespaces` 兜底（`Array.empty`）、
  `requireDbExists` 守卫（`databaseExists`）、大小写（`equalsIgnoreCase`）、
  普通 v2 目录恢复不过滤（文档化限制）、TABLE_CAT 统一 `catalog.name()`——均已修
- 测试：Delta 无区分度用例删除、GetSchemas 补 `SparkCatalogUtilsSuite#getSchemas lists
  DSv2-only namespace`、namespace finally 清理、`ignoreTableProperties` 三行为测试、
  HMS 容器加固——均已补

**当前状态（2026-11-16 核实）**：Open + requested changes，9 月 15 日后无 PMC 复审、无新评论。

## 四、遗留问题

### 1.【中】GetColumns 未覆盖（2026-11-16 实测后严重性下调）

`getColumnsByCatalog`（`SparkCatalogUtils.scala:331-359`）的三个代码问题不变（master 既有
+ 本 PR 未动），但**实测表明 issue #7673 的典型场景下 GetColumns 是正常的**：v2 分支的
`tc.loadTable` 会先经自定义 session catalog（Iceberg SparkSessionCatalog）按
metadata_location 加载真实 Iceberg 表，不触碰 v1 storage handler 转换。

实际失败条件（收窄）：
1. **loadTable 委托回 v1 的表**：非 Iceberg 的 HMS 表且 storage handler 类缺失，或
metadata_location 缺失/损坏 → `V1Table(getTableMetadata)` →
`convertHiveTableToCatalogTable` → ClassNotFound → GetColumns 整体失败（无 Try 守卫）
2. **本地临时视图**（:335-337 无空 namespace 过滤）：存在匹配 pattern 的本地临时视图时
`loadTable(空namespace)` 抛 `requiresSinglePartNamespaceError`（master 存量 bug）
3. **分支顺序**：`case tc: TableCatalog` 在前使 v1 分支对 session catalog 不可达（死代码，
PMC 点 10 在此分支的 `builtin.name()` 修复无效）

**处理**：对齐分支 2 做法（过滤空 namespace + Try 包裹）。可本 PR 补上，或按
"One concern per PR" 另开 PR 并在 re-review 回复中向 PMC 说明。

### 2.【中】GetTables 与 GetSchemas 引号行为不一致

PMC 点 4 修了 `getSchemas`（`quote=false`，:127），但 GetTables 路径未对齐：

- `listNamespacesWithPattern`（:172-179）按 `quoteIfNeeded` 引号形式匹配 schemaPattern，
  分支 2 输出 TABLE_SCHEM 也带引号（:298）→ 库名 `2024_db` 时 `!schemas` 能列出但
  `!tables` 匹配不到，自相矛盾
- `tablePattern` 同理按 `quoteIfNeeded` 匹配（:256）→ 数字开头表名在分支 2 匹配不上
- 与分支 1（`listDatabases(schemaPattern)` 原样匹配）也不一致

**处理**：re-review 前对齐（分支 2 匹配与输出改为不引号），或回复中说明理由。

### 3.【低】rebase

基点 `13d1bd6e6`（9 月），master 已前进至 `823478048`（11 月），合并前需 rebase 重跑测试。

### 4. 已文档化限制（保持口径即可，非 bug）

- 委托回 v1 的表（Iceberg 目录下的非 Iceberg 表等）：REMARKS 降级 `""`、恒为 TABLE
  （Try 兜底，PMC 点 9 已知残余）
- 分支 3 全 TABLE、无视图检测、不应用 tableTypes 过滤（PMC 点 8 要求，见第五节 #7793）
- `ignoreTableProperties=true` 全部按 TABLE、不做视图检测（设计如此）
- DSv2-only namespace 视图探测跳过（v1 视图只存在于 HMS 库中，合理）

## 五、与 #7772 / #7793 的关系

- Issue: https://github.com/apache/kyuubi/issues/7772 （Paimon 持久化视图无法通过
  GetTables 列出，明确引用 #7718 记录的限制；open，无维护者回应）
- PR: https://github.com/apache/kyuubi/pull/7793 （[SPARK] Support standard ViewCatalog
  in GetTables，作者 wayne-x-wang；PMC 定调走 Spark 4.2 标准 `ViewCatalog` 而非 Paimon
  反射适配器；CI 全绿，2026-11-16 核实仍 Open 等待复审）

### 代码重叠（rebase 必然冲突）

|                                                            #7793 改动                                                            |                            与 #7718 的关系                            |
|--------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| 重写非 session `case tc: TableCatalog` 分支：TABLE 枚举加 tableTypes 门槛 + 反射枚举标准 ViewCatalog 视图（含根命名空间视图、`SparkViewCatalogUtils` 反射工具类） | 即 #7718 的分支 3。PMC 点 8 让 #7718 恢复 master 行为并文档化限制，#7793 正是该限制的后续解法 |
| `listAllNamespaces` 兜底 `case _ => Array(catalog.defaultNamespace())`                                                           | 与 #7718 PMC 点 5 的 `case _ => Array.empty` 直接冲突，rebase 时需统一        |
| 不触碰 `hasCustomSessionCatalog` / session catalog 分支                                                                             | 分支 1/2 无语义冲突，但整个 match 块被重写，文本冲突不可避免                              |
| 不触碰 GetColumns                                                                                                                 | 第四节问题 1 依旧独立存在，#7793 也无视图列元数据支持                                   |

### 语义空白（两个 PR 均未覆盖，建议 follow-up）

**自定义 session catalog + 标准 ViewCatalog**：Iceberg 1.12+ 的 `SparkSessionCatalog`
（Spark 4.2 连接器，pan3793 确认已实现 ViewCatalog）若配置为 `spark_catalog`——正是
issue #7673 的场景——#7718 分支 2 走 v1 `listViews` 探测，枚举不到 ViewCatalog 中的
Iceberg 持久化视图（它们不是 HMS 的 v1 view）；#7793 又明确不改 session catalog 分支。
该组合场景无人覆盖，建议 re-review 回复中主动向 PMC 提出，另开 follow-up issue，
避免日后被当作 #7718 的回归报出来。

另：Paimon on Spark 3.5 的原始诉求（#7772）仍未解决——PMC 拒绝反射适配 Paimon 自有
`SupportView` 接口，主张等连接器迁移标准 ViewCatalog。

## 六、复现记录（2026-11-16，TBDS 测试环境）

环境：`119.45.208.69`（tbds-10-0-0-155），Kerberized TBDS，Kyuubi 1.9.0-TBDS-5.3.2_2026p3
（Spark 3.5.8-TBDS，HMS thrift://*:7004），`spark.sql.catalog.spark_catalog` 已配置为
`org.apache.iceberg.spark.SparkSessionCatalog`（type=hive）。

### 复现步骤（可直接执行）

环境前置：`119.45.208.69`（tbds-10-0-0-155），root 密码登录，`klist` 已有
`hadoop/tbds-10-0-0-155@TBDS-755DVU29` 票据；`/usr/local/service/spark/conf/spark-defaults.conf`
已含 `spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog`（type=hive）。

**Step 1. 经 Kyuubi 建真实 Iceberg 表**（HiveCatalog 注册，metadata 落 HDFS）：

```bash
BEELINE="/usr/local/service/kyuubi/bin/beeline -u jdbc:hive2://10.0.0.155:10009/\\;principal=hadoop/tbds-10-0-0-155@TBDS-755DVU29\\;#kyuubi.engine.share.level=CONNECTION;kyuubi.engine.type=SPARK_SQL;"
$BEELINE -e "CREATE TABLE IF NOT EXISTS spark_catalog.default.repro_real_iceberg (id bigint) USING iceberg;"
```

**Step 2. spark-shell + HiveMetaStoreClient 仿造 legacy 表**（与旧版
`iceberg-hive-runtime` Hive 引擎集成注册方式同构）。脚本 `/tmp/repro_legacy.scala`：

```scala
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.hive.metastore.api.{FieldSchema, SerDeInfo, StorageDescriptor, Table}
import scala.collection.JavaConverters._

val hiveConf = new HiveConf()
hiveConf.addResource(new org.apache.hadoop.fs.Path(
  "/usr/local/service/spark/conf/hive-site.xml"))   // HMS: thrift://tbds-10-0-0-{155,156}:7004
val client = new HiveMetaStoreClient(hiveConf)
try { client.dropTable("default", "repro_legacy_iceberg") } catch { case _: Exception => () }
val real = client.getTable("default", "repro_real_iceberg")
val ml = real.getParameters.get("metadata_location")

val t = new Table()
t.setDbName("default"); t.setTableName("repro_legacy_iceberg")
t.setOwner("hadoop"); t.setTableType("EXTERNAL_TABLE")
val sd = new StorageDescriptor()
sd.setCols(List(new FieldSchema("id", "bigint", "")).asJava)
sd.setInputFormat("org.apache.hadoop.hive.ql.io.HiveInputFormat")
sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveOutputFormat")
val serde = new SerDeInfo()
serde.setSerializationLib("org.apache.iceberg.mr.hive.HiveIcebergSerDe")  // 触发点1
sd.setSerdeInfo(serde); sd.setLocation(real.getSd.getLocation)
t.setSd(sd)
t.setParameters(Map(
  "table_type" -> "iceberg",
  "metadata_location" -> ml,
  "storage_handler" -> "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler",  // 触发点2
  "EXTERNAL" -> "TRUE").asJava)
client.createTable(t); client.close(); System.exit(0)
```

运行：`/usr/local/service/spark/bin/spark-shell --master 'local[1]' -i /tmp/repro_legacy.scala`

**Step 3. 基线验证**（jars = iceberg 1.6.1 含 `iceberg-hive-runtime`，issue 的"升级前"）：

```bash
printf '!tables\n!quit\n' > /tmp/tables.bcmd
$BEELINE -f /tmp/tables.bcmd        # 期望：正常列出 repro_real_iceberg / repro_legacy_iceberg
```

**Step 4. 切换 iceberg 1.10.1（去掉 hive-runtime，issue 的"升级后"）**：

```bash
mkdir -p /usr/local/service/spark/backup/iceberg-1.6.1
cd /usr/local/service/spark/jars
mv iceberg-*1.6.1-TBDS-5.3.1.5.jar /usr/local/service/spark/backup/iceberg-1.6.1/
cp /usr/local/service/spark/backup/iceberg-*-1.10.1-*.jar .
```

**Step 5. 复现**（CONNECTION share level，新连接自动用新 classpath 拉 engine）：

```bash
$BEELINE -f /tmp/tables.bcmd        # 期望：ClassNotFound（issue #7673）
$BEELINE -e "SELECT * FROM spark_catalog.default.repro_legacy_iceberg;"   # 期望：正常
printf '!columns repro_legacy_iceberg\n!quit\n' > /tmp/columns.bcmd
$BEELINE -f /tmp/columns.bcmd       # 期望：正常（见第四节修正说明）
```

### 结果

|              操作               | 1.6.1（含 hive-runtime） |        1.10.1（无 hive-runtime）        |
|-------------------------------|-----------------------|--------------------------------------|
| `!tables`                     | 正常                    | **ClassNotFound，与 issue #7673 完全一致** |
| `SELECT repro_legacy_iceberg` | 正常                    | 正常（Iceberg catalog 加载成功）             |
| `!columns`                    | 正常                    | 正常（v2 分支经 Iceberg loadTable，见第四节修正）  |

失败堆栈 Kyuubi 侧：`GetTables.runInternal(:81)` →
`SparkCatalogUtils.getCatalogTablesOrViews(:174)` → v1 `getTablesByName` →
`HiveClientImpl.convertHiveTableToCatalogTable` → `Table.getStorageHandler` →
`HiveUtils.getStorageHandler` → `ClassNotFoundException: HiveIcebergStorageHandler`。

**SELECT 正常 + GetTables 失败** 实证 pan3793 的诊断：表经 DSv2 catalog 完全可用，
坏的只是 Kyuubi 走 v1 的元数据 API（PR #7718 的修复目标）。

### 环境当前状态与恢复

- jars 目录现为 iceberg 1.10.1；1.6.1 三个 jar 保留在
  `/usr/local/service/spark/backup/iceberg-1.6.1/`，恢复：移回 jars 目录并删除
  1.10.1 两个 jar
- 测试表：`default.repro_real_iceberg`、`default.repro_legacy_iceberg`（脚本
  `/tmp/repro_legacy.scala` 可重造 legacy 表；beeline 命令文件 `/tmp/tables.bcmd`、
  `/tmp/columns.bcmd`）
- 下一步可用此环境验证 PR #7718 修复：替换 `/usr/local/service/kyuubi/jars/` 中的
  `kyuubi-spark-sql-engine` jar 为修复版构建，重跑 `!tables`

## 七、修复充分性评估（2026-11-16，对照 Spark 3.5 / Spark master / iceberg 双版本）

代码库：Spark 3.5.9-TBDS（`/Users/maomao/study/idea/531/spark`）、Spark master 4.x
（`/Users/maomao/study/idea/open/spark`，`d781c67c7c0`）、iceberg TBDS 1.10.1
（`/Users/maomao/study/idea/531/iceberg`）、iceberg master
（`/Users/maomao/study/idea/open/iceberg`）。

### 分支 2 依赖的 Spark/iceberg 行为，跨版本全部成立

|                                                       修复依赖的行为                                                        |             Spark 3.5             |                                  Spark master (4.x)                                  |                                           对应修复                                           |
|----------------------------------------------------------------------------------------------------------------------|-----------------------------------|--------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| `V2SessionCatalog.listTables` 委托 `SessionCatalog.listTables(db)`，本地临时视图以空 namespace 返回                               | ✓（`V2SessionCatalog.scala:61-71`） | ✓（`V2SessionCatalog.scala:64-74`，实现不变）                                               | 空 namespace 过滤（:255-257）                                                                 |
| `SessionCatalog.listTables(db)` 默认 `includeLocalTempViews=true`；`listViews` 走 `requireDbExists` 且附带本地临时视图            | ✓（`:1048, :1083-1097`）            | ✓（`:1347, :1382+`，结构不变）                                                              | `databaseExists` 守卫 + `database.isDefined` 过滤                                            |
| `V2_SESSION_CATALOG_IMPLEMENTATION` 配置语义                                                                             | optional 无默认                      | **默认 `"builtin"` + transform 将任意大小写 builtin 归一化**（`SQLConf.scala:7154+`，SPARK-50700） | `equalsIgnoreCase("builtin")` 与 Spark 自身 `V2SessionCatalog.hasCustomSessionCatalog` 语义一致 |
| CatalogManager 形态                                                                                                    | class                             | interface（SPARK-46050, 4.2）                                                          | PR 已用反射访问 `sessionState.catalogManager`                                                  |
| iceberg `SparkSessionCatalog.listTables` 全量委托 v1 session catalog；`loadTable` Iceberg 优先、`NoSuchTableException` 回退 v1 | ✓（1.6.1/1.10.1 均如此，实测验证）          | ✓（master `SparkSessionCatalog.java:138-150` 语义不变）                                    | 分支 2 整体设计与 `Try(loadTable)` 兜底                                                           |

### 结论

1. **对 issue #7673 的目标场景，当前修复足够**：bug 已复现实证；三分支设计依赖的全部
   Spark/iceberg 行为在 3.5 与 master 均核实成立；PMC 17 点全部处理；跨 Spark 版本
   （3.5/4.x）与跨 iceberg 版本（1.6.x/1.10.x/master）无假设破绽。
2. **已知但可接受**：委托回 v1 的坏表 REMARKS 降级（PMC 点 9 认可的方案）、分支 3
   全 TABLE（PMC 点 8 要求，#7793 后续解决）、逐表 loadTable 的 I/O 代价
   （PMC 点 2 给的可选方案，SPARK-52109 可后续优化）。
3. **两个遗留问题的处置决策见第八节**。

## 八、遗留问题处置决策

### 【中·建议本 PR 修】GetTables 引号不一致（分支 2）

- `listNamespacesWithPattern`（:176）按 `quoteIfNeeded` 引号形式匹配 schemaPattern、
  TABLE_SCHEM 输出带引号（:298）、tablePattern 亦然（:256）→ 库名 `2024_db` 时
  `!tables` 匹配不到，与 `!schemas`（quote=false）自相矛盾
- **修在本 PR 的理由**：这是 PR 新引入的行为差异（master 上 spark_catalog 走 v1 原样
  匹配），与 PMC 点 4 同类——性质上是回归，只是 PMC 只点了 getSchemas；re-review 被
  追问概率高，届时再改要再等一轮复审
- 改法：分支 2 的 namespace/table 匹配与输出对齐 `quote=false`；补一个 `2024_db`
  式测试（现有 `get tables` 测试恰好绕开：spark_catalog 只用了不带点的 `db4`）

### 【中·建议另开 follow-up PR】GetColumns（`getColumnsByCatalog`）

- 三个问题（分支顺序死代码、loadTable 无 Try、空 namespace 未过滤）**全部是 master
  存量**，非本 PR 引入；实测（第六节）issue 典型场景下 GetColumns 正常，失败条件
  收窄为"委托回 v1 的坏表"与"本地临时视图"两种
- **不塞进本 PR 的理由**：PR 已等复审两个月，扩 scope 拖长周期且违反
  "One concern per PR"；在 re-review 回复中主动说明 + 开 follow-up 更符合社区惯例
- 修法明确：对齐分支 2（空 ns 过滤 + Try 包裹），顺手调整死代码分支顺序

## 九、行动项

1. **本 PR 内**：修复 GetTables 分支 2 引号不一致（第八节），补 `2024_db` 测试，然后
   ping pan3793 复审
2. **follow-up PR**：GetColumns 加固（空 ns 过滤 + Try + 分支顺序），re-review 回复中
   向 PMC 主动说明
3. re-review 回复中主动说明与 #7793 的边界与互补关系（回应 PMC 点 8 后续追问）
4. 跟踪 #7793 合并进度：若先合并，rebase 时分支 3 采纳其实现，并统一
   `listAllNamespaces` 兜底语义（`defaultNamespace()` vs `Array.empty`）
5. 向 PMC 提出 session catalog + ViewCatalog 空白作为 follow-up
6. 用第六节环境验证修复版 engine jar（替换 `/usr/local/service/kyuubi/jars/` 中的
   `kyuubi-spark-sql-engine`，重跑 `!tables`），形成"复现→修复→验证"证据链
7. **已完成（2026-11-16）**：修复已同步至 TBDS 仓库
   `/Users/maomao/study/idea/531/kyuubi`（分支 `dev/5.3.2_2026p3`，commit `171fa6074`，
   cherry-pick 自 `5b7ec6af4`，10 文件 +409/-62）。冲突解决：`IcebergMetadataTests`
   保留 TBDS 的 `SPARK_COMPILE_VERSION` 导入与 SPARK-37931 版本条件（TBDS 支持
   spark-3.2~4.0 profile），dbs 列表对齐 PR 的版本条件写法；其余为 TBDS 基线差异
   （直接 `catalogManager` 调用、旧版 `quoteIfNeeded`）原样保留。**注意**：TBDS 的
   旧版 `quoteIfNeeded`（`[a-zA-Z0-9_]+ && !全数字`）不会给 `2024_db` 加引号，第十节
   问题 1 在 TBDS 上的触发面不同（仅含 `-`/`.` 等特殊字符的单段名）。待 TBDS 流水线
   构建后在第六节环境复测

## 十、遗留问题详细记录（2026-11-16）

### 问题 1：GetTables 分支 2 引号不一致（本 PR 引入的回归，建议本 PR 修）

**现象**（触发条件：库名/表名不匹配 `^[a-zA-Z_][a-zA-Z0-9_]*`，即数字开头、
含 `-`/`$`/中文等特殊字符）：

自定义 session catalog（分支 2）下，库名 `2024_db`：
- `!schemas` 正常返回 `2024_db`（PMC 点 4 修复，quote=false）
- `!tables`（schemaPattern=`2024%`）返回**空**——库被 pattern 过滤掉了
- 即使 pattern 恰好写成带反引号的形式匹配上，TABLE_SCHEM 输出的也是
`` `2024_db` ``，与 `!schemas` 的输出自相矛盾
- 表名同理：表 `2024_sales` 在 tablePattern=`2024_sales` 下匹配不到

**机理**（代码位置基于 `5b7ec6af4`）：

1. `listNamespacesWithPattern`（`SparkCatalogUtils.scala:172-179`）把每个 namespace
   用 `quoteIfNeeded` 逐段加引号后拼串，再与 schemaPattern 匹配：
   `quoteIfNeeded("2024_db")` → 首字符不匹配 `^[a-zA-Z_][a-zA-Z0-9_]*` →
   返回 `` `2024_db` ``（`quoteIdentifier` 加反引号）→ pattern `2024.*` 匹配失败
2. 分支 2 输出 TABLE_SCHEM（:298）同样 `namespace().map(quoteIfNeeded)` → 带引号
3. tablePattern 匹配（:256）`tp.matcher(quoteIfNeeded(t.name()))` → 同理

**为什么是回归**：master 上 `spark_catalog`（无论内置还是自定义）走 v1 分支，
`listDatabases(schemaPattern)` / `listTables(db, tablePattern)` 全程原始名匹配、
原始名输出，从无反引号。PR 把自定义 session catalog 挪到分支 2 后引入了引号行为。
PMC 点 4 已确认这是错误（"a DB like 2024_db is returned as `2024_db` and no longer
matchable by its own pattern. Master returned raw v1 names"），但只修了 getSchemas
一侧（`getSchemasWithPattern(..., quote = false)`，:127），GetTables 一侧漏了。

**为什么分支 3（普通 v2 catalog）的引号是有意的、不能动**：v2 namespace 是多段的，
引号用于消歧——`["a","b"]` → `a.b`，而名字本身就含点的单段 `["a.b"]` → `` `a.b` ``。
这是 master 既有设计，`IcebergMetadataTests` 的 `` `a.b` ``/`a.`b.c``/`` `a.b.c` ``
系列测试专门钉住（对 hadoop_prod 路径）。而分支 2 的 session catalog namespace
恒为单段（HMS 库名，`V2SessionCatalog.listTables` 对多段 namespace 直接抛
`requiresSinglePartNamespace`），无需消歧，对齐原始名既正确又与 v1/getSchemas 一致。

**测试盲区**：`get tables` 测试（`IcebergMetadataTests:103-139`）对 spark_catalog
只用了 `dbs.filterNot(_.contains("."))` 即 `db4`——恰好绕开所有需要引号的名字。

**修法**：分支 2 中 namespace 匹配（改 `ns.mkString(".")`）、TABLE_SCHEM 输出、
tablePattern 匹配（改 `t.name()`）均用原始名；分支 3 保持不动；补 `2024_db`
库 + `2024_sales` 表的测试（spark_catalog + 自定义 session catalog 路径）。

### 问题 2：GetColumns 未加固（master 存量，建议 follow-up PR）

`getColumnsByCatalog`（`SparkCatalogUtils.scala:331-359`）三个子问题，均为 master
存量、非本 PR 引入：

1. **分支顺序死代码**：`case tc: TableCatalog`（:332）在前，内置 `V2SessionCatalog`
   与自定义 session catalog 都实现 `TableCatalog` 必然先命中 v2 分支；后面的
   `case builtin if builtin.name() == SESSION_CATALOG`（:347，v1 路径）对它们不可达
   ——PMC 点 10 在该分支改的 `builtin.name()`（:355）实际不生效
2. **`tc.loadTable` 无 Try 守卫**（:339）：委托回 v1 的表（非 Iceberg 的 HMS 表且
   storage handler 类缺失，或 metadata_location 损坏）→ `V1Table(getTableMetadata)` →
   `convertHiveTableToCatalogTable` → ClassNotFound → GetColumns 整体失败
3. **空 namespace 未过滤**（:335-337）：`tc.listTables(ns)` 返回的本地临时视图
   （空 namespace）进入 `loadTable` → `requiresSinglePartNamespaceError`

**实测边界**（第六节）：issue 典型场景（SparkSessionCatalog + 真实 Iceberg legacy 表）
下 GetColumns **正常**——`tc.loadTable` 先经 Iceberg 按 metadata_location 加载成功，
不触碰 v1 storage handler。失败仅限上述两种收窄条件。

**修法**：对齐分支 2 的做法——`i.namespace().nonEmpty` 过滤 + `Try` 包裹 loadTable
（REMARKS 降级），并把分支顺序调整为 v1 分支在前或直接删除死代码分支。






### Why are the changes needed?
<!--
Please clarify why the changes are needed. For instance,
  1. If you propose a new feature, clarify the use cases.
  2. If you fix a bug, you can clarify why it is a bug, and what versions are affected.
-->
The thrift `GetSchemas`/`GetTables` operations assumed `spark_catalog` is always the builtin V1 session catalog and listed databases/tables via `spark.sessionState.catalog`. When a custom session catalog is configured, e.g.

```
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
```

these operations bypass the DSv2 catalog APIs (`SupportsNamespaces`/`TableCatalog`) and the V1 session catalog path cannot even parse the V2 table metadata the custom catalog persists in HMS - it may return wrong results, or throw and fail the whole request. #7673 is a real-world example: `GetTables` fails with `ClassNotFoundException: org.apache.iceberg.mr.hive.HiveIcebergSerDe` when the V1 `HiveExternalCatalog` tries to load tables registered by Iceberg with `storage_handler = org.apache.iceberg.mr.hive.HiveIcebergStorageHandler`.

Additionally, for identifiers that need quoting in SQL (e.g. `2026`), the previous code matched `schemaPattern`/`tablePattern` against and returned `TABLE_SCHEM`/`TABLE_NAME` in the backtick-quoted form (`` `2026` ``), while JDBC clients always pass and expect raw names, so such names could never be matched.


### How was this patch tested?
<!--
If tests were added, say they were added here. Please make sure to add some test cases that check the changes thoroughly including negative and positive cases if possible.
If it was tested in a way different from regular unit tests, please clarify how you tested step by step, ideally copy and paste-able, so that other reviewers can test and check, and descendants can verify in the future.
If tests were not added, please describe why they were not added and/or why it was difficult to add.
-->
1. Added test coverage:
    * `IcebergMetadataTests#get tables` — exercises both `spark_catalog` (hive-backed session catalog) and `hadoop_prod` (hadoop-backed v2 catalog), now asserting `TABLE_SCHEM`/`TABLE_CAT` and dropping namespaces in `finally`.
    * `IcebergMetadataTests#get tables and views` — custom session catalog view detection, verifying `tableTypes` filtering (`TABLE`/`VIEW`).
    * `IcebergMetadataTests#get tables with ignoreTableProperties` — covers all three behaviors of `ignoreTableProperties` (skip view probe, skip `tableTypes` filter, skip per-table property loading).
    * `IcebergMetadataTests#get tables with names needing quotes` — verifies `schemaPattern`/`tablePattern` match the raw (unquoted) names for the session catalog, guarding the regression where patterns were matched and results were returned in the backtick-quoted form.
    * `SparkCatalogUtilsSuite#getSchemas lists DSv2-only namespace` — covers the custom session catalog GetSchemas path via a test-only `DummySessionCatalog` exposing a namespace the v1 catalog cannot see.
    * A testcontainers-based `WithSimpleHMSContainer` is wired into `SparkIcebergOperationSuite` and `IcebergOperationSuite` so these run against a real Hive metastore (`catalogImplementation=hive`).

2. Manually verified against Iceberg 1.10.1 (which removed `iceberg-hive-runtime`): `beeline -u "jdbc:hive2://<kyuubi-host>:<port>/" -n <user> -e "!tables"`. This previously threw `ClassNotFoundException: org.apache.iceberg.mr.hive.HiveIcebergStorageHandler`, and now returns the table as `TABLE` and v1/v2 views as `VIEW`.

### Limitations
- Non-session v2 catalogs report tables only (all rows typed `TABLE`); the `tableTypes` filter is not applied to them, so a `VIEW`-only request still returns those tables typed `TABLE` (no `VIEW` rows are produced by the catalog itself). DSv2 `ViewCatalog` is still immature, so covering v2 tables only is intentional for this PR.
- The fix guarantees listing for custom-catalog tables it can load; tables the custom session catalog delegates back to v1 (e.g. non-Iceberg tables under Iceberg's session catalog) are still listed as `TABLE`, and when `loadTable` fails for them, `REMARKS` degrades to "" (logged at warn).
- `GetColumns` still matches and returns backtick-quoted names for the custom session catalog (pre-existing behavior, out of the scope of this PR); it will be addressed in a follow-up.


### Was this patch assisted by generative AI tooling?
<!--
If generative AI tooling has assisted in authoring or reviewing this patch, please include
an 'Assisted-by: ' trailer that identifies the agent and/or model.
For example: 'Assisted-by: Claude Opus 4.7' or 'Assisted-by: Claude:claude-opus-4-7'.
If no, write 'No'.
Please refer to the [ASF Generative Tooling Guidance](https://www.apache.org/legal/generative-tooling.html) for details.
-->
Assisted-by: DeepSeek-V4-Pro



---

Review of #7718 (head 8ffa541). Verdict: request changes. The #7673 diagnosis and the Iceberg fix mechanism are verified real; the listing refactor introduces the regressions below in the same configurations the PR targets.

[Blocker, regression] Local temp views leak into the new v2 listing. catalog.listTables(ns) (SparkCatalogUtils.scala:202-204) reaches V2SessionCatalog.listTables -> SessionCatalog.listTables(db) with includeLocalTempViews = true, so temp views come back as Identifier.of(Array(), name); they are typed TABLE with TABLE_SCHEM="" (:217-232) and getTempViews (:350-359) emits the same view a second time as VIEW. Master used includeLocalTempViews = false here. Fix: drop identifiers with an empty namespace before typing/loading.

[Blocker, regression] catalog.loadTable(ident) at :229 is unguarded and runs per row when ignoreTableProperties=false (the default), so the namespace-less identifier from (1) throws (requiresSinglePartNamespace/NoSuchTable) and fails the whole GetTables; any unloadable, dropped, or corrupt table does the same. It also forces per-table format I/O (Delta _delta_log, Iceberg metadata JSON) where master did one bulk HMS call. Fix: wrap in Try and degrade REMARKS to "" (or keep the session-catalog listing load-free).

[Blocker, regression] The view probe keys listViews results by the outer db (:211-215) while SessionCatalog.listViews also returns local temp views, so a temp view v marks permanent table db.v as VIEW and removes it under tableTypes={TABLE}. Fix: only keep TableIdentifiers with a database.

[Major, regression] getSchemas("spark_catalog", ...) with a custom session catalog now returns backtick-quoted namespaces and matches the pattern against the quoted form (:126, :170-173); a DB like 2024_db is returned as `2024_db` and no longer matchable by its own pattern. Master returned raw v1 names.

[Major, regression] listAllNamespaces is a non-exhaustive match (:153-163) and is now reachable for the session catalog, so a TableCatalog-only session catalog throws MatchError in GetSchemas where master returned v1 rows.

[Major, new failure mode] The per-namespace spark.sessionState.catalog.listViews(db, ...) probe calls requireDbExists, so a custom session catalog whose namespaces are not v1-backed now fails the whole GetTables with NoSuchDatabaseException. Guard with databaseExists or catch and skip the probe for that namespace.

[Major] hasCustomSessionCatalog (:106-109) compares case-sensitively; Spark 4.x normalizes BUILTIN to builtin, so such a value is treated as custom. Use equalsIgnoreCase (mirrors V2SessionCatalog.hasCustomSessionCatalog).

[Major, behavior change] tableTypes is now applied to ordinary v2 catalogs (:225) while view detection exists only for the session catalog, so getTables(<v2 catalog>, ..., Array("VIEW")) goes from all-rows-typed-TABLE to empty. DSv2 ViewCatalog is still immature, so covering v2 tables only is fine for this PR - please just document the limitation (non-session v2 catalogs report tables only, VIEW-only filters return empty) and drop the filter or add a test pinning the empty-result behavior.

[Major, scope/claim] The fix guarantees only "the custom catalog can load the table". Tables delegated back to v1 (e.g. DeltaCatalog + catalogImplementation=hive, or non-Iceberg tables under Iceberg's session catalog) still hit HiveExternalCatalog/HiveClientImpl.getStorageHandler and the same ClassNotFoundException. Narrow the claim, or make the listing independent of per-catalog loading.

[Major] TABLE_CAT is inconsistent within one result: catalog.name() at :231 vs the raw request string in getTempViews (:357) and getColumnsByCatalog, so getTables(null, ...) mixes "spark_catalog" with null. Align all row producers and tighten the assertions.

[Test] DeltaMetadataTests#get tables and views exercises the new path (DeltaCatalog is a custom session catalog) but passes on master, so it does not discriminate the fix. Add a case that fails without the change, or drop it.

[Test] Nothing covers GetSchemas: reverting only the getSchemas change keeps the suites green, because the test session catalogs delegate namespaces to the v1 catalog. Add a test-only CatalogExtension exposing a namespace the v1 catalog cannot see.

[Test] IcebergMetadataTests creates spark_catalog.<db> namespaces and never drops them (withDatabases only drops unqualified names), making the exact-count get schemas assertion order-dependent. Drop them in the finally blocks.

[Test] kyuubi.operation.getTables.ignoreTableProperties has no coverage repo-wide; the new path has three distinct behaviors for it (:208, :225, :228). Add a test.

[Minor] The new tests never assert TABLE_SCHEM/TABLE_CAT; the docstring at :186-191 overstates ("returns both v1 and v2 views", "never loads the Hive storage handler") - it only covers views registered in the v1 catalog; catalog.name() and catalog.name are mixed in one file.

[Minor] The new WithSimpleHMSContainer duplicates WithSecuredHMSContainer (drop the unused env, consider sharing the container def), hmsThriftUris has no null guard, and HostPortWaitStrategy has no startup-timeout margin.

[Hygiene] Commit 80255d65e is tagged [WIP][KYUUBI #7064] Fix thrift GetSchemas/GetTables with custom session catalog #7064 while the PR head/title use [Bug] Legacy Iceberg tables fail with Kyuubi 1.11.1 and Iceberg 1.10.0 #7673, and the description does not describe the final diff or list the commands run. Please update the description for the final state.

---






Thanks for the thorough review. All blockers and majors are addressed, code, tests, and PR metadata are now aligned. Point-by-point:

[Blocker] Local temp views leak — Fixed. Identifiers are filtered by t.namespace().nonEmpty before typing/loading, matching master's includeLocalTempViews = false.

[Blocker] Unguarded loadTable — Fixed. Wrapped in `Try` and degrade REMARKS to "" on failure, logging at `warn` so the degradation is traceable.

[Blocker] View probe keys by outer db — Fixed. listViews results are filtered by .filter(_.database.isDefined).

[Major] getSchemas backtick-quoted — Fixed. For a custom session catalog, getSchemas now lists namespaces through DSv2 (getSchemasWithPattern(catalog, schemaPattern, quote = false)) with quoting disabled, so a DB like 2026_db is returned raw and remains matchable by its own pattern, matching master's raw-name behavior.

[Major] Non-exhaustive listAllNamespaces — Fixed. Added a case _ => Array.empty fallback, so a TableCatalog-only custom session catalog no longer throws MatchError in getSchemas.

[Major] listViews requireDbExists — Fixed. The per-namespace probe is guarded with sessionCatalog.databaseExists(db).

[Major] hasCustomSessionCatalog case-sensitivity — Fixed. Uses equalsIgnoreCase.

[Major] tableTypes on ordinary v2 catalogs — Fixed. Dropped the filter, restoring master behavior (non-session v2 catalogs report tables only, typed TABLE). Limitation documented in the PR description.

[Major] Scope/claim — Addressed. The listing is now independent of per-catalog loading (Try-guarded loadTable + databaseExists probe), and the claim is narrowed in the PR description.

[Major] TABLE_CAT inconsistency — Fixed. All row producers use catalog.name().

[Test] Delta get tables and views — Removed the non-discriminating Delta case; the Iceberg suite exercises the custom-session-catalog path and fails without the fix.

[Test] GetSchemas coverage — Added SparkCatalogUtilsSuite#getSchemas lists DSv2-only namespace. It wires a test-only DummySessionCatalog (a DelegatingCatalogExtension exposing a namespace the v1 catalog cannot see) and asserts getSchemas lists it through DSv2 with raw (unquoted) names — the test fails without the hasCustomSessionCatalog detection and quote = false.

[Test] Un-dropped namespaces — Fixed. All namespace-creating tests now drop namespaces in finally.

[Test] ignoreTableProperties coverage — Added get tables with ignoreTableProperties, covering all three behaviors (view probe, tableTypes filter, loadTable).

[Minor] TABLE_SCHEM/TABLE_CAT + docstring + name()/name — Fixed. Assertions added, overstating docstring removed, unified to catalog.name().

[Minor] HMS container duplication — Fixed. Shared DOCKER_IMAGE_NAME/EXPOSED_HMS_PORT, added hmsThriftUris null guard, and added startup timeout.

[Hygiene] Commit tag / description — Commit retagged to #7673; PR description rewritten to reflect the final diff, and the beeline command is now written out in full instead of beeline ....



---

Review of #7718 (head 3cdd2d7). Verdict: request changes. The previous round is addressed; 1-3 need to be addressed in this PR, and 1 and 2 are regressions in the configuration this PR targets. "Previous item N" refers to my last review; line numbers refer to SparkCatalogUtils.scala unless noted.

[Major, regression] With a custom session catalog, schema and table patterns are matched as case-sensitive Java regex against DSv2 names (:176-179 via :207, :187-190 via :127, :259). Master listed through the v1 catalog, which matches case-insensitively, so getTables("spark_catalog", "DEFAULT", "MY_TABLE", null) and getSchemas("spark_catalog", "DEFAULT") return rows on master and nothing here. The view probe at :270 still uses v1 matching, so the two filters disagree within one request. Session catalog identifiers are case-insensitive; compile these patterns with Pattern.CASE_INSENSITIVE for spark_catalog.

[Major, regression] Previous item 2 is fixed for failures, but the per-table format I/O noted there remains. With ignoreTableProperties=false (default), every custom session catalog (Iceberg, Delta, Hudi, Paimon) calls tc.loadTable per listed table just for REMARKS (:298), where master made one getTablesByName HMS call per database; e.g. DeltaCatalog with 5k tables turns one RPC into 5k _delta_log reads on every GetTables. Per namespace:

Try the batched v1 sessionCatalog.getTablesByName first. It returns both the VIEW type and the comment (Iceberg pushes table properties, including comment, into HMS params), so REMARKS is unchanged and the databaseExists + listViews probe (:263-277) is only needed on the fallback path.
Fall back to the current per-table path for namespaces where the batch throws (the [Bug] Legacy Iceberg tables fail with Kyuubi 1.11.1 and Iceberg 1.10.0 #7673 case) and for identifiers it does not return (DSv2-only tables).
[Major, perf] SPARK-52109 (4.1.0) added TableCatalog.listTableSummaries, which returns table types without loading tables when the catalog implements it. Since this PR reworks both TableCatalog branches (:254, :311), use it there for ignoreTableProperties=true, so tableTypes is honored without per-table loads:

Detect the method by reflection, not by Spark version, since vendors backport.
Call it only when the catalog has its own implementation. Calling it unconditionally turns today's single listTables into N loads and brings [Bug] Legacy Iceberg tables fail with Kyuubi 1.11.1 and Iceberg 1.10.0 #7673 back for catalogs that don't implement it (e.g. DeltaCatalog via DelegatingCatalogExtension). A simple check is enough as long as the code comment explains it, e.g.
// SPARK-52109 (4.1.0): added TableCatalog.listTableSummaries. Detect it by reflection
// instead of by Spark version, since vendors may backport it. Use it only when the
// catalog provides its own implementation, because the TableCatalog default calls
// loadTable for every table, which is what this path avoids. A declaring class that
// directly implements TableCatalog counts as not implemented: scalac emits a forwarder
// to the interface default in such Scala classes (e.g. V2SessionCatalog). Real
// implementations in such classes (e.g. JDBCTableCatalog) are skipped, which only
// loses the optimization.
private def implementsListTableSummaries(tc: TableCatalog): Boolean =
try {
val owner = tc.getClass.getMethod("listTableSummaries", classOf[Array[String]])
.getDeclaringClass
owner != classOf[TableCatalog] && !owner.getInterfaces.contains(classOf[TableCatalog])
} catch {
case _: NoSuchMethodException => false
}
Map VIEW and METRIC_VIEW to VIEW and the rest to TABLE, filter by tableTypes, keep REMARKS empty, and fall back to listTables per namespace on failure.
Today this covers Iceberg 1.12.0 SparkCatalog and gives real VIEW types for non-session v2 catalogs, resolving the TODO at :317; new implementations are picked up with no code change. Iceberg's SparkSessionCatalog (Spark 4.2) is detected but forwards to V2SessionCatalog's default, so it stays per-table until Spark implements it there.
Update the kyuubi.operation.getTables.ignoreTableProperties doc (it says tableTypes is ignored) and regenerate settings.md.
[Minor] Try rethrows LinkageError (:298), so a NoClassDefFoundError (SerDe or handler class present, a dependency missing, the same upgrade pattern as [Bug] Legacy Iceberg tables fail with Kyuubi 1.11.1 and Iceberg 1.10.0 #7673) still fails the whole GetTables. Catch it explicitly, also in the fallback from 2.

[Minor, refines previous item 10] getTempViews labels temp views with the requested or current catalog (:379), but temp views live in the session catalog and getColumns labels them spark_catalog (:435, :454). With spark.sql.defaultCatalog=hadoop_prod, getTables(null, "global_temp", "%", ["VIEW"]) returns TABLE_CAT=hadoop_prod, and hadoop_prod.global_temp.v does not resolve. Use SESSION_CATALOG for these rows.

[Test] IcebergOperationSuite.scala:30 and SparkIcebergOperationSuite.scala:35 repeat the same 14-entry extraConfigs, mostly re-listing IcebergSuiteMixin's entries, and IcebergMetadataTests (:28) now needs an HMS-backed spark_catalog without declaring it. Move the HMS overrides into one mixin built on super.extraConfigs, and require it from IcebergMetadataTests with a self-type.

[Minor] STARTUP_TIMEOUT = 60s (WithSimpleHMSContainer.scala:50) equals testcontainers' default wait timeout, so the margin from previous item 16 has no effect. Use a larger value, e.g. 3 minutes.

[Nit] Unrelated cosmetic changes: three section banners are re-padded while the Columns one (:405) is not, and the getCatalogs/getSchemas scaladocs are reworded. Revert, or make the banners consistent.























