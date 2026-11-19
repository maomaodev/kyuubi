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

package org.apache.kyuubi.engine.spark.util

import java.sql.{Connection, DriverManager}
import java.util.Locale

import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, TableCatalog}

import org.apache.kyuubi.engine.spark.KyuubiSparkUtil.SPARK_ENGINE_RUNTIME_VERSION
import org.apache.kyuubi.engine.spark.WithSparkSQLEngine

object DummySessionCatalog {
  val extraNamespace: Array[String] = Array("2026_db")
}

class DummySessionCatalog extends DelegatingCatalogExtension {

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    val base = super.listNamespaces(namespace)
    if (namespace.isEmpty) base :+ DummySessionCatalog.extraNamespace else base
  }
}

object SummariesJdbcCatalog {
  val catalogImpl = "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"
  // Derby, brought in by the spark-hive test dependency, keeps an in-memory database only
  // while at least one connection is open.
  val url = "jdbc:derby:memory:kyuubi_summaries_catalog;create=true"
  val driver = "org.apache.derby.jdbc.EmbeddedDriver"
  val catalogName = "summaries_catalog"
  val schema = "summaries_db"
  val table = "summaries_table"
}

class SparkCatalogUtilsSuite extends WithSparkSQLEngine {

  override def withKyuubiConf: Map[String, String] = Map(
    "spark.sql.catalog.spark_catalog" ->
      "org.apache.kyuubi.engine.spark.util.DummySessionCatalog")

  test("getSchemas lists DSv2-only namespace") {
    val rows = SparkCatalogUtils.getSchemas(spark, SparkCatalogUtils.SESSION_CATALOG, ".*")
    val schemaNames = rows.map(_.getString(0)).toSet

    assert(schemaNames.contains(DummySessionCatalog.extraNamespace.head))
    assert(!schemaNames.contains(s"`${DummySessionCatalog.extraNamespace.head}`"))
    assert(rows.forall(_.getString(1) == SparkCatalogUtils.SESSION_CATALOG))
  }

  test("getSchemas matches session catalog patterns case-insensitively") {
    val pattern = DummySessionCatalog.extraNamespace.head.toUpperCase(Locale.ROOT)
    val rows = SparkCatalogUtils.getSchemas(spark, SparkCatalogUtils.SESSION_CATALOG, pattern)
    val schemaNames = rows.map(_.getString(0)).toSet

    assert(schemaNames.contains(DummySessionCatalog.extraNamespace.head))
  }

  test("implementsListTableSummaries is false without an own implementation") {
    // DummySessionCatalog relies on the TableCatalog default, whose forwarder must not be
    // mistaken for a real implementation of SPARK-52109 listTableSummaries.
    val catalog = SparkCatalogUtils
      .getCatalog(spark, SparkCatalogUtils.SESSION_CATALOG)
      .asInstanceOf[TableCatalog]
    assert(!SparkCatalogUtils.implementsListTableSummaries(catalog))
  }
}

class SummariesCatalogUtilsSuite extends WithSparkSQLEngine {
  import SummariesJdbcCatalog._

  override def withKyuubiConf: Map[String, String] = Map(
    "spark.sql.catalog.spark_catalog" -> catalogImpl,
    "spark.sql.catalog.spark_catalog.url" -> url,
    "spark.sql.catalog.spark_catalog.driver" -> driver,
    s"spark.sql.catalog.$catalogName" -> catalogImpl,
    s"spark.sql.catalog.$catalogName.url" -> url,
    s"spark.sql.catalog.$catalogName.driver" -> driver)

  // Hold the Derby connection that backs the in-memory database open for the suite
  // lifetime, and create the schema and the table over it.
  private var derbyConnection: Connection = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    Class.forName(driver)
    derbyConnection = DriverManager.getConnection(url)
    val statement = derbyConnection.createStatement()
    try {
      statement.execute(s"""CREATE SCHEMA "$schema"""")
      statement.execute(s"""CREATE TABLE "$schema"."$table" ("id" INT)""")
    } finally {
      statement.close()
    }
  }

  override def afterAll(): Unit = {
    if (derbyConnection != null) {
      derbyConnection.close()
      derbyConnection = null
    }
    super.afterAll()
  }

  test("implementsListTableSummaries detects only real implementations") {
    val catalog = SparkCatalogUtils.getCatalog(spark, catalogName).asInstanceOf[TableCatalog]
    if (SPARK_ENGINE_RUNTIME_VERSION >= "4.1") {
      // SPARK-52109 (4.1.0): JDBCTableCatalog implements listTableSummaries itself.
      assert(SparkCatalogUtils.implementsListTableSummaries(catalog))
    } else {
      assert(!SparkCatalogUtils.implementsListTableSummaries(catalog))
    }
  }

  test("getTables honors tableTypes on the session catalog with ignoreTableProperties") {
    // Row fields: 0=TABLE_CAT, 1=TABLE_SCHEM, 2=TABLE_NAME, 3=TABLE_TYPE, 4=REMARKS
    val tableRows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      SparkCatalogUtils.SESSION_CATALOG,
      schema,
      ".*",
      Set("TABLE"),
      ignoreTableProperties = true)
    assert(tableRows.map(_.getString(2)).toSet == Set(table))
    assert(tableRows.forall(_.getString(0) == SparkCatalogUtils.SESSION_CATALOG))
    assert(tableRows.forall(_.getString(1) == schema))
    // On 4.1+ the summaries report FOREIGN, mapped to TABLE; before that the fallback
    // lists names as TABLE.
    assert(tableRows.forall(_.getString(3) == "TABLE"))
    assert(tableRows.forall(_.getString(4) == ""))

    // JDBCTableCatalog lists only tables and SPARK-52109 marks them FOREIGN, mapped to
    // TABLE. With listTableSummaries (4.1+) the tableTypes criteria is honored, so the
    // VIEW criteria filters everything out; without it the criteria cannot be honored
    // without loading tables, so identities return typed TABLE.
    val viewRows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      SparkCatalogUtils.SESSION_CATALOG,
      schema,
      ".*",
      Set("VIEW"),
      ignoreTableProperties = true)
    if (SPARK_ENGINE_RUNTIME_VERSION >= "4.1") {
      assert(viewRows.isEmpty)
    } else {
      assert(viewRows.map(_.getString(2)).toSet == Set(table))
      assert(viewRows.forall(_.getString(3) == "TABLE"))
    }
  }

  test("getTables falls back to per-table loading when the batched v1 lookup fails") {
    // The JDBC tables live outside the v1 session catalog, so the batched v1 lookup
    // fails (the KYUUBI #7673 resilience path): the table must still be returned via
    // the per-table fallback instead of failing.
    val rows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      SparkCatalogUtils.SESSION_CATALOG,
      schema,
      ".*",
      Set("TABLE"),
      ignoreTableProperties = false)
    assert(rows.map(_.getString(2)).toSet == Set(table))
    assert(rows.forall(_.getString(1) == schema))
    assert(rows.forall(_.getString(3) == "TABLE"))
    assert(rows.forall(_.getString(4) == ""))
  }

  test("getTables honors tableTypes on a non-session catalog with ignoreTableProperties") {
    val tableRows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      catalogName,
      schema,
      ".*",
      Set("TABLE"),
      ignoreTableProperties = true)
    assert(tableRows.map(_.getString(2)).toSet == Set(table))
    assert(tableRows.forall(_.getString(0) == catalogName))
    assert(tableRows.forall(_.getString(1) == schema))
    assert(tableRows.forall(_.getString(3) == "TABLE"))
    assert(tableRows.forall(_.getString(4) == ""))

    val viewRows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      catalogName,
      schema,
      ".*",
      Set("VIEW"),
      ignoreTableProperties = true)
    if (SPARK_ENGINE_RUNTIME_VERSION >= "4.1") {
      assert(viewRows.isEmpty)
    } else {
      assert(viewRows.map(_.getString(2)).toSet == Set(table))
      assert(viewRows.forall(_.getString(3) == "TABLE"))
    }
  }
}
