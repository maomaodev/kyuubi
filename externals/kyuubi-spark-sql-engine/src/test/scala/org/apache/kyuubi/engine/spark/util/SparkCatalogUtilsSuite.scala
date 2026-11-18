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

import java.util.Locale

import org.apache.spark.sql.connector.catalog.{DelegatingCatalogExtension, Identifier}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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

object SummariesSessionCatalog {
  val schema = "summaries_db"
  val table = "summaries_table"
  val view = "summaries_view"
  val metricView = "summaries_metric_view"
}

// A duck-typed SPARK-52109 TableSummary, so the reflection-based access also works on Spark
// versions without the API.
class SimpleTableSummary(ident: Identifier, typ: String) {
  def identifier(): Identifier = ident
  def tableType(): String = typ
}

class SummariesSessionCatalog extends DelegatingCatalogExtension {
  import SummariesSessionCatalog._

  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }

  // name(), defaultNamespace(), and listNamespaces() delegate to the session catalog, which
  // exists only for the spark_catalog registration; a standalone registration under another
  // name has no delegate to forward to.
  override def name(): String = catalogName

  override def defaultNamespace(): Array[String] = Array(schema)

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    val base =
      if (delegate == null) Array.empty[Array[String]] else super.listNamespaces(namespace)
    if (namespace.isEmpty) base :+ Array(schema) else base
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    if (namespace.sameElements(Array(schema))) {
      Array(table, view, metricView).map(Identifier.of(Array(schema), _))
    } else {
      super.listTables(namespace)
    }
  }

  def listTableSummaries(namespace: Array[String]): Array[SimpleTableSummary] = {
    if (namespace.sameElements(Array(schema))) {
      Array(
        new SimpleTableSummary(Identifier.of(Array(schema), table), "MANAGED"),
        new SimpleTableSummary(Identifier.of(Array(schema), view), "VIEW"),
        new SimpleTableSummary(Identifier.of(Array(schema), metricView), "METRIC_VIEW"))
    } else {
      Array.empty
    }
  }
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
}

class SummariesCatalogUtilsSuite extends WithSparkSQLEngine {

  override def withKyuubiConf: Map[String, String] = Map(
    "spark.sql.catalog.spark_catalog" ->
      "org.apache.kyuubi.engine.spark.util.SummariesSessionCatalog",
    "spark.sql.catalog.summaries_catalog" ->
      "org.apache.kyuubi.engine.spark.util.SummariesSessionCatalog")

  test("getTables honors tableTypes via listTableSummaries with ignoreTableProperties") {
    import SummariesSessionCatalog._

    val viewRows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      SparkCatalogUtils.SESSION_CATALOG,
      schema,
      ".*",
      Set("VIEW"),
      ignoreTableProperties = true)
    // VIEW and METRIC_VIEW map to VIEW; the MANAGED table is filtered out.
    // Row fields: 1=TABLE_SCHEM, 2=TABLE_NAME, 3=TABLE_TYPE, 4=REMARKS
    assert(viewRows.map(_.getString(2)).toSet == Set(view, metricView))
    assert(viewRows.forall(_.getString(3) == "VIEW"))
    assert(viewRows.forall(_.getString(4) == ""))

    val tableRows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      SparkCatalogUtils.SESSION_CATALOG,
      schema,
      ".*",
      Set("TABLE"),
      ignoreTableProperties = true)
    assert(tableRows.map(_.getString(2)).toSet == Set(table))
    assert(tableRows.forall(_.getString(3) == "TABLE"))
  }

  test("getTables falls back to per-table loading when the batched v1 lookup fails") {
    import SummariesSessionCatalog._
    // The summaries_db tables are DSv2-only, so the v1 session catalog has no such database
    // and the batched getTablesByName call fails (the KYUUBI #7673 resilience path): every
    // table must still be returned via the per-table fallback instead of failing.
    val rows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      SparkCatalogUtils.SESSION_CATALOG,
      schema,
      ".*",
      Set("TABLE"),
      ignoreTableProperties = false)
    assert(rows.map(_.getString(2)).toSet == Set(table, view, metricView))
    assert(rows.forall(_.getString(1) == schema))
    assert(rows.forall(_.getString(3) == "TABLE"))
    assert(rows.forall(_.getString(4) == ""))
  }

  test("getTables honors tableTypes via listTableSummaries on a non-session catalog") {
    import SummariesSessionCatalog._
    val catalog = "summaries_catalog"

    val viewRows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      catalog,
      schema,
      ".*",
      Set("VIEW"),
      ignoreTableProperties = true)
    assert(viewRows.map(_.getString(2)).toSet == Set(view, metricView))
    assert(viewRows.forall(_.getString(0) == catalog))
    assert(viewRows.forall(_.getString(1) == schema))
    assert(viewRows.forall(_.getString(3) == "VIEW"))

    val tableRows = SparkCatalogUtils.getCatalogTablesOrViews(
      spark,
      catalog,
      schema,
      ".*",
      Set("TABLE"),
      ignoreTableProperties = true)
    assert(tableRows.map(_.getString(2)).toSet == Set(table))
    assert(tableRows.forall(_.getString(3) == "TABLE"))
  }
}
