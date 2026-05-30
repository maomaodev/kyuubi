-- ============================================================================
-- Kyuubi Hive Connector - Bucketed Table Write Smoke Tests
--
-- 用途：验证 KSHC 在 V2 路径下对 Hive 分桶表的支持。
-- 使用方式：
--   1. 把以下 SQL 通过 beeline / spark-sql 提交到挂载了 KSHC 的 Spark/Kyuubi 集群。
--   2. 每个用例完成后，手动通过 `hdfs dfs -ls <table_location>` 检查文件名是否带
--      `<bucketId:05d>_0_*` 前缀。
--   3. 关键断言：
--        * 单桶表只产出 `00000_0_*` 文件。
--        * N 桶表的 bucket id 全部落在 [0, N) 区间内。
--        * 分区桶表的 bucket 文件落在各自 `dt=...` 子目录下。
--        * 数据回查结果与 INSERT 内容一致。
--
-- 本脚本默认使用 `test_catalog` 作为 KSHC 注册名，请按你的环境替换。
-- ============================================================================

-- 公共准备：清理可能残留的测试库表
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_basic;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_one;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_large;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_sorted;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_multi_col;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_part_bucket;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_part_bucket_static;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_append;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_empty;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_null;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_ctas_src;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_dyn_src;
DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_parquet;

-- ============================================================================
-- Case 1: 基础非分区分桶表（4 桶）
--   预期：HDFS 上文件名形如 `00000_0_*`、`00001_0_*` 等，bucket id 范围 [0,4)。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_basic (
  id INT,
  name STRING
) CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_basic
VALUES (1, 'a'), (2, 'b'), (1, 'c'), (2, 'd'), (3, 'e'), (4, 'f');

SELECT id, name FROM test_catalog.default.kyuubi_bucket_basic ORDER BY id, name;

-- ============================================================================
-- Case 2: 边界 - 单桶表（1 BUCKETS）
--   预期：只有 `00000_0_*` 一类文件。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_one (
  id INT,
  name STRING
) CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_one
VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT id, name FROM test_catalog.default.kyuubi_bucket_one ORDER BY id;

-- ============================================================================
-- Case 3: 边界 - 大桶数（256 BUCKETS）
--   预期：写入数据较少时不一定铺满 256 个文件，但所有产出 bucket id 应在 [0,256)。
--         手动验证：`hdfs dfs -ls .../kyuubi_bucket_large | wc -l` 应为非零数。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_large (
  id BIGINT,
  payload STRING
) CLUSTERED BY (id) INTO 256 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_large
SELECT cast(id as BIGINT) as id, concat('row-', cast(id as STRING)) as payload
FROM range(0, 1000);

SELECT count(*) AS cnt, count(distinct id) AS distinct_ids
FROM test_catalog.default.kyuubi_bucket_large;
-- 预期：cnt = 1000, distinct_ids = 1000

-- ============================================================================
-- Case 4: SORTED BY 分桶表（带 sort 列）
--   预期：bucket id 范围 [0,3)，且每个 bucket 文件内部 ts 列单调递增（可在 ORC 元
--         数据或读取时验证）。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_sorted (
  id INT,
  name STRING,
  ts BIGINT
) CLUSTERED BY (id) SORTED BY (ts) INTO 3 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_sorted
VALUES (1, 'a', 100), (2, 'b', 50), (1, 'c', 75), (3, 'd', 25), (2, 'e', 200);

SELECT id, name, ts FROM test_catalog.default.kyuubi_bucket_sorted ORDER BY id, ts;

-- ============================================================================
-- Case 5: 多列复合分桶 key
--   预期：bucket id 范围 [0,4)。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_multi_col (
  id INT,
  region STRING,
  value DOUBLE
) CLUSTERED BY (id, region) INTO 4 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_multi_col VALUES
  (1, 'CN', 1.0), (2, 'US', 2.0), (1, 'US', 3.0), (2, 'CN', 4.0);

SELECT id, region, value FROM test_catalog.default.kyuubi_bucket_multi_col
ORDER BY id, region;

-- ============================================================================
-- Case 6: 分区 + 分桶（动态分区）
--   预期：每个分区目录 `dt=2024-01-01`、`dt=2024-01-02` 下分别有自己的
--         `00000_0_*`、`00001_0_*` 文件。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_part_bucket (
  id INT,
  name STRING
) PARTITIONED BY (dt STRING)
  CLUSTERED BY (id) INTO 2 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_part_bucket PARTITION (dt) VALUES
  (1, 'a', '2024-01-01'),
  (2, 'b', '2024-01-01'),
  (3, 'c', '2024-01-02'),
  (4, 'd', '2024-01-02'),
  (5, 'e', '2024-01-02');

SELECT dt, id, name FROM test_catalog.default.kyuubi_part_bucket
ORDER BY dt, id;

-- ============================================================================
-- Case 7: 分区 + 分桶（静态分区写入）
--   验证两点：
--     1. `INSERT OVERWRITE PARTITION (dt = '2024-02-01')` 仅写入该静态分区，
--        bucket 文件落在 `dt=2024-02-01/00000_0_*`、`00001_0_*`。
--     2. 紧接着的 `INSERT INTO PARTITION (dt = '2024-02-02')` 只新增第二个
--        分区目录，不影响第一个分区已写入的数据；两个分区下都应有完整的
--        bucket 文件（bucket id ∈ [0, 2)）。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_part_bucket_static (
  id INT,
  name STRING
) PARTITIONED BY (dt STRING)
  CLUSTERED BY (id) INTO 2 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_part_bucket_static
PARTITION (dt = '2024-02-01')
VALUES (10, 'x'), (11, 'y'), (12, 'z');

INSERT INTO TABLE test_catalog.default.kyuubi_part_bucket_static
PARTITION (dt = '2024-02-02')
VALUES (20, 'p'), (21, 'q');

SELECT dt, id, name FROM test_catalog.default.kyuubi_part_bucket_static
ORDER BY dt, id;

-- ============================================================================
-- Case 8: 多次 INSERT INTO 追加（验证追加不破坏 bucket layout）
--   预期：两次 INSERT 后目录中仍能看到 `00000_0_*`、`00001_0_*` 形式的文件，
--         可能每个 bucket id 对应多个文件（每次写入产生一组）。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_append (
  id INT,
  name STRING
) CLUSTERED BY (id) INTO 2 BUCKETS
STORED AS ORC;

INSERT INTO TABLE test_catalog.default.kyuubi_bucket_append
VALUES (1, 'a'), (2, 'b');

INSERT INTO TABLE test_catalog.default.kyuubi_bucket_append
VALUES (3, 'c'), (4, 'd');

SELECT id, name FROM test_catalog.default.kyuubi_bucket_append ORDER BY id;

-- ============================================================================
-- Case 9: 边界 - 空写入
--   预期：表保持空，且不会因为 0 行数据导致 commit 失败。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_empty (
  id INT,
  name STRING
) CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_empty
SELECT * FROM (VALUES (1, 'never')) t(id, name) WHERE 1 = 0;

SELECT count(*) AS cnt FROM test_catalog.default.kyuubi_bucket_empty;
-- 预期：cnt = 0

-- ============================================================================
-- Case 10: 边界 - bucket 列含 NULL
--   预期：NULL 也会被 HiveHash 计算到某个 bucket id（不抛异常），数据回查无丢失。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_null (
  id INT,
  name STRING
) CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_null VALUES
  (1, 'a'), (CAST(NULL AS INT), 'null-row-1'), (2, 'b'), (CAST(NULL AS INT), 'null-row-2');

SELECT count(*) AS cnt,
       sum(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_cnt
FROM test_catalog.default.kyuubi_bucket_null;
-- 预期：cnt = 4, null_cnt = 2

-- ============================================================================
-- Case 11: CTAS 风格 - 从普通表读出再写入分桶表
--   预期：bucket layout 与直接 VALUES 写入一致。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_ctas_src (
  id INT,
  name STRING
) STORED AS ORC;

INSERT INTO test_catalog.default.kyuubi_bucket_ctas_src VALUES
  (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (6, 'f');

CREATE TABLE test_catalog.default.kyuubi_bucket_basic_ctas (
  id INT,
  name STRING
) CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_basic_ctas
SELECT id, name FROM test_catalog.default.kyuubi_bucket_ctas_src;

SELECT id, name FROM test_catalog.default.kyuubi_bucket_basic_ctas ORDER BY id;

-- ============================================================================
-- Case 12: 动态分区 + 分桶 + 大数据量（验证多 task 写入仍保持 bucket layout）
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_dyn_src (
  id BIGINT,
  dt STRING
) STORED AS ORC;

INSERT INTO test_catalog.default.kyuubi_bucket_dyn_src
SELECT id,
       CASE WHEN id % 3 = 0 THEN '2024-03-01'
            WHEN id % 3 = 1 THEN '2024-03-02'
            ELSE '2024-03-03' END AS dt
FROM range(0, 600);

CREATE TABLE test_catalog.default.kyuubi_bucket_dyn_tgt (
  id BIGINT
) PARTITIONED BY (dt STRING)
  CLUSTERED BY (id) INTO 8 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_dyn_tgt PARTITION (dt)
SELECT id, dt FROM test_catalog.default.kyuubi_bucket_dyn_src;

SELECT dt, count(*) AS cnt
FROM test_catalog.default.kyuubi_bucket_dyn_tgt
GROUP BY dt
ORDER BY dt;
-- 预期：每个 dt 200 行

-- ============================================================================
-- Case 13: 不同存储格式下的分桶（PARQUET）
--   预期：PARQUET 表的 bucket 文件命名同样满足 `<bucketId:05d>_0_*` 前缀。
-- ============================================================================
CREATE TABLE test_catalog.default.kyuubi_bucket_parquet (
  id INT,
  name STRING
) CLUSTERED BY (id) INTO 4 BUCKETS
STORED AS PARQUET;

INSERT OVERWRITE TABLE test_catalog.default.kyuubi_bucket_parquet VALUES
  (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

SELECT id, name FROM test_catalog.default.kyuubi_bucket_parquet ORDER BY id;

-- ============================================================================
-- 清理
-- ============================================================================
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_basic;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_one;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_large;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_sorted;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_multi_col;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_part_bucket;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_part_bucket_static;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_append;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_empty;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_null;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_ctas_src;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_basic_ctas;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_dyn_src;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_dyn_tgt;
-- DROP TABLE IF EXISTS test_catalog.default.kyuubi_bucket_parquet;
