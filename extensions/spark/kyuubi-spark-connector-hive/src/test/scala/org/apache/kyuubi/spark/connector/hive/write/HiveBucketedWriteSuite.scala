/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.spark.connector.hive.write

import java.io.File

import org.apache.spark.sql.{Row, SparkSession}

import org.apache.kyuubi.spark.connector.hive.{HiveTableCatalog, KyuubiHiveTest}

/**
 * End-to-end tests covering Kyuubi Hive Connector's support for Hive-bucketed tables created and
 * written via the V2 path. The intent is to verify that:
 *   - The bucket spec survives `CREATE TABLE ... CLUSTERED BY ... INTO N BUCKETS`.
 *   - `INSERT` produces files whose names follow the Hive bucket convention
 *     (`<bucketId:05d>_0_*`), so they can be read back by Hive, Trino and Presto.
 *   - The data round-trips correctly across plain bucketed, sorted-bucketed and
 *     partitioned-bucketed tables.
 */
class HiveBucketedWriteSuite extends KyuubiHiveTest {

  /**
   * Resolves the on-disk table location through the V2 catalog plugin so that the test does not
   * need to hard-code warehouse paths.
   */
  private def tableLocation(spark: SparkSession, fullName: String): File = {
    // fullName is `hive.<db>.<table>`; split off the catalog prefix.
    val parts = fullName.split('.')
    require(parts.length == 3, s"Expected fully-qualified <catalog>.<db>.<table>, got: $fullName")
    val catalogPlugin = spark.sessionState.catalogManager.catalog(parts(0))
    val tableCatalog = catalogPlugin.asInstanceOf[HiveTableCatalog]
    val ident = org.apache.spark.sql.connector.catalog.Identifier.of(Array(parts(1)), parts(2))
    val location = tableCatalog.catalog.getTableMetadata(ident.asTableIdentifier).location
    new File(location)
  }

  private implicit class IdentifierOps(ident: org.apache.spark.sql.connector.catalog.Identifier) {
    def asTableIdentifier: org.apache.spark.sql.catalyst.TableIdentifier = {
      val ns = ident.namespace()
      require(ns.length == 1, s"Expected single-part namespace, got: ${ns.mkString(",")}")
      org.apache.spark.sql.catalyst.TableIdentifier(ident.name(), Some(ns(0)))
    }
  }

  /**
   * Recursively collects regular data files (skipping staging dirs and Hadoop markers).
   */
  private def collectDataFiles(root: File): Seq[File] = {
    if (!root.exists()) {
      Seq.empty
    } else if (root.isFile) {
      val name = root.getName
      if (name.startsWith(".") || name.startsWith("_")) Seq.empty else Seq(root)
    } else {
      Option(root.listFiles())
        .map(_.toSeq)
        .getOrElse(Seq.empty)
        .filterNot(f => f.getName.startsWith(".hive-staging"))
        .flatMap(collectDataFiles)
    }
  }

  /**
   * Returns the Hive bucket id encoded in a file name, if any. Files written via the V2 Hive
   * connector follow the prefix convention `<bucketId:05d>_0_*`.
   */
  private def hiveBucketIdOf(fileName: String): Option[Int] = {
    val pattern = "^(\\d{5})_0_.*".r
    fileName match {
      case pattern(id) => Some(id.toInt)
      case _ => None
    }
  }

  test("non-partitioned bucketed table: file names follow Hive bucket convention") {
    withSparkSession() { spark =>
      val table = "hive.default.kyuubi_bucket_basic"
      withTable(table) {
        spark.sql(
          s"""
             | CREATE TABLE $table (id INT, name STRING)
             | CLUSTERED BY (id) INTO 4 BUCKETS
             | STORED AS ORC
             |""".stripMargin)

        spark.sql(
          s"""
             | INSERT OVERWRITE TABLE $table
             | VALUES (1, 'a'), (2, 'b'), (1, 'c'), (2, 'd'), (3, 'e'), (4, 'f')
             |""".stripMargin)

        val files = collectDataFiles(tableLocation(spark, table))
        assert(files.nonEmpty, s"No data files were written under $table")
        val bucketIds = files.flatMap(f => hiveBucketIdOf(f.getName)).toSet
        assert(
          bucketIds.nonEmpty,
          s"No Hive-bucketed file found, got: ${files.map(_.getName).mkString(", ")}")
        bucketIds.foreach { id =>
          assert(id >= 0 && id < 4, s"Bucket id $id out of range [0, 4)")
        }

        checkAnswer(
          spark.sql(s"SELECT id, name FROM $table ORDER BY id, name"),
          Seq(
            Row(1, "a"),
            Row(1, "c"),
            Row(2, "b"),
            Row(2, "d"),
            Row(3, "e"),
            Row(4, "f")))
      }
    }
  }

  test("non-partitioned bucketed table: empty insert produces no Hive bucket data") {
    withSparkSession() { spark =>
      val table = "hive.default.kyuubi_bucket_empty"
      withTable(table) {
        spark.sql(
          s"""
             | CREATE TABLE $table (id INT, name STRING)
             | CLUSTERED BY (id) INTO 4 BUCKETS
             | STORED AS ORC
             |""".stripMargin)

        spark.sql(s"INSERT OVERWRITE TABLE $table SELECT * FROM VALUES (1, 'a') WHERE 1 = 0")

        // It's acceptable for the writer to produce zero files or only empty files; the contract
        // we care about is that no spurious row leaks into the table.
        checkAnswer(spark.sql(s"SELECT * FROM $table"), Seq.empty)
      }
    }
  }

  test("non-partitioned bucketed table: 1 bucket boundary case") {
    withSparkSession() { spark =>
      val table = "hive.default.kyuubi_bucket_one"
      withTable(table) {
        spark.sql(
          s"""
             | CREATE TABLE $table (id INT, name STRING)
             | CLUSTERED BY (id) INTO 1 BUCKETS
             | STORED AS ORC
             |""".stripMargin)

        spark.sql(s"INSERT OVERWRITE TABLE $table VALUES (1, 'a'), (2, 'b'), (3, 'c')")

        val files = collectDataFiles(tableLocation(spark, table))
        val bucketIds = files.flatMap(f => hiveBucketIdOf(f.getName)).toSet
        assert(
          bucketIds == Set(0),
          s"Single-bucket table must only contain bucket id 0, got: $bucketIds")
        checkAnswer(
          spark.sql(s"SELECT id FROM $table ORDER BY id"),
          Seq(Row(1), Row(2), Row(3)))
      }
    }
  }

  test("sorted-bucketed table: data round-trips and uses Hive bucket files") {
    withSparkSession() { spark =>
      val table = "hive.default.kyuubi_bucket_sorted"
      withTable(table) {
        spark.sql(
          s"""
             | CREATE TABLE $table (id INT, name STRING, ts BIGINT)
             | CLUSTERED BY (id) SORTED BY (ts) INTO 3 BUCKETS
             | STORED AS ORC
             |""".stripMargin)

        spark.sql(
          s"""
             | INSERT OVERWRITE TABLE $table
             | VALUES (1, 'a', 100), (2, 'b', 50), (1, 'c', 75), (3, 'd', 25)
             |""".stripMargin)

        val files = collectDataFiles(tableLocation(spark, table))
        val bucketIds = files.flatMap(f => hiveBucketIdOf(f.getName)).toSet
        assert(bucketIds.nonEmpty, "Sorted-bucketed table must produce Hive-bucketed files")
        bucketIds.foreach(id => assert(id >= 0 && id < 3, s"Bucket id $id out of range [0, 3)"))

        checkAnswer(
          spark.sql(s"SELECT id, name, ts FROM $table ORDER BY id, ts"),
          Seq(
            Row(1, "c", 75L),
            Row(1, "a", 100L),
            Row(2, "b", 50L),
            Row(3, "d", 25L)))
      }
    }
  }

  test("multi-column bucketed table: composite bucket key is honored") {
    withSparkSession() { spark =>
      val table = "hive.default.kyuubi_bucket_multi_col"
      withTable(table) {
        spark.sql(
          s"""
             | CREATE TABLE $table (id INT, region STRING, value DOUBLE)
             | CLUSTERED BY (id, region) INTO 4 BUCKETS
             | STORED AS ORC
             |""".stripMargin)

        spark.sql(
          s"""
             | INSERT OVERWRITE TABLE $table VALUES
             |   (1, 'CN', 1.0), (2, 'US', 2.0), (1, 'US', 3.0), (2, 'CN', 4.0)
             |""".stripMargin)

        val files = collectDataFiles(tableLocation(spark, table))
        val bucketIds = files.flatMap(f => hiveBucketIdOf(f.getName)).toSet
        assert(bucketIds.nonEmpty, "Multi-column bucketed table must produce Hive-bucketed files")
        bucketIds.foreach(id => assert(id >= 0 && id < 4, s"Bucket id $id out of range [0, 4)"))

        checkAnswer(
          spark.sql(s"SELECT id, region, value FROM $table ORDER BY id, region"),
          Seq(
            Row(1, "CN", 1.0),
            Row(1, "US", 3.0),
            Row(2, "CN", 4.0),
            Row(2, "US", 2.0)))
      }
    }
  }

  test("partitioned + bucketed table: each partition contains its own bucket files") {
    withSparkSession() { spark =>
      val table = "hive.default.kyuubi_part_bucket"
      withTable(table) {
        spark.sql(
          s"""
             | CREATE TABLE $table (id INT, name STRING)
             | PARTITIONED BY (dt STRING)
             | CLUSTERED BY (id) INTO 2 BUCKETS
             | STORED AS ORC
             |""".stripMargin)

        spark.sql(
          s"""
             | INSERT OVERWRITE TABLE $table PARTITION (dt)
             | VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01'),
             |        (3, 'c', '2024-01-02'), (4, 'd', '2024-01-02'),
             |        (5, 'e', '2024-01-02')
             |""".stripMargin)

        val files = collectDataFiles(tableLocation(spark, table))
        val bucketIds = files.flatMap(f => hiveBucketIdOf(f.getName)).toSet
        assert(bucketIds.nonEmpty, "Partitioned bucketed table must emit Hive-bucketed files")
        bucketIds.foreach(id => assert(id >= 0 && id < 2, s"Bucket id $id out of range [0, 2)"))

        // Each Hive-bucket file should live under a `dt=...` partition directory.
        files.flatMap(f => Option(f.getParentFile)).map(_.getName).distinct.foreach { dirName =>
          assert(
            dirName.startsWith("dt="),
            s"Bucket files must reside under partition directories, got: $dirName")
        }

        checkAnswer(
          spark.sql(s"SELECT dt, id, name FROM $table ORDER BY dt, id"),
          Seq(
            Row("2024-01-01", 1, "a"),
            Row("2024-01-01", 2, "b"),
            Row("2024-01-02", 3, "c"),
            Row("2024-01-02", 4, "d"),
            Row("2024-01-02", 5, "e")))
      }
    }
  }

  test("INSERT INTO (append) preserves Hive bucket layout across multiple writes") {
    withSparkSession() { spark =>
      val table = "hive.default.kyuubi_bucket_append"
      withTable(table) {
        spark.sql(
          s"""
             | CREATE TABLE $table (id INT, name STRING)
             | CLUSTERED BY (id) INTO 2 BUCKETS
             | STORED AS ORC
             |""".stripMargin)

        spark.sql(s"INSERT INTO TABLE $table VALUES (1, 'a'), (2, 'b')")
        spark.sql(s"INSERT INTO TABLE $table VALUES (3, 'c'), (4, 'd')")

        val files = collectDataFiles(tableLocation(spark, table))
        val bucketIds = files.flatMap(f => hiveBucketIdOf(f.getName)).toSet
        assert(bucketIds.nonEmpty, "Appending to a bucketed table must keep Hive bucket file names")
        bucketIds.foreach(id => assert(id >= 0 && id < 2, s"Bucket id $id out of range [0, 2)"))

        checkAnswer(
          spark.sql(s"SELECT id FROM $table ORDER BY id"),
          Seq(Row(1), Row(2), Row(3), Row(4)))
      }
    }
  }
}
