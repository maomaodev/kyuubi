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

package org.apache.kyuubi.spark.connector.hive

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import com.google.common.collect.Maps
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchViewException, ViewAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, ViewCatalog, ViewChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class HiveCatalogViewSuite extends KyuubiHiveTest {

  private val emptyProps: util.Map[String, String] = Collections.emptyMap[String, String]
  private val schema: StructType = new StructType()
    .add("id", IntegerType)
    .add("data", StringType)

  private val testNs: Array[String] = Array("view_db")
  private val testIdent: Identifier = Identifier.of(testNs, "test_view")

  private var catalog: HiveTableCatalog = _

  private def newCatalog(): HiveTableCatalog = {
    val c = new HiveTableCatalog
    val properties = Maps.newHashMap[String, String]()
    properties.put("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:memorydb;create=true")
    properties.put("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    c.initialize(catalogName, new CaseInsensitiveStringMap(properties))
    c
  }

  /** 构造一个最简的 createView 调用包装。 */
  private def doCreateView(
      ident: Identifier,
      sql: String = "SELECT 1 AS id, 'a' AS data",
      currentCatalog: String = "hive",
      currentNamespace: Array[String] = Array("default"),
      schema: StructType = schema,
      queryColumnNames: Array[String] = Array("id", "data"),
      columnAliases: Array[String] = Array.empty,
      columnComments: Array[String] = Array.empty,
      properties: util.Map[String, String] = emptyProps) = {
    catalog.createView(
      ident,
      sql,
      currentCatalog,
      currentNamespace,
      schema,
      queryColumnNames,
      columnAliases,
      columnComments,
      properties)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    catalog = newCatalog()
    catalog.createNamespace(testNs, emptyProps)
  }

  override def afterEach(): Unit = {
    try {
      // 清理 view 与 namespace
      Option(catalog).foreach { c =>
        Option(c.listViews(testNs: _*)).getOrElse(Array.empty).foreach(c.dropView)
        if (c.listNamespaces().exists(_.sameElements(testNs))) {
          c.dropNamespace(testNs, cascade = true)
        }
      }
    } finally {
      catalog = null
      super.afterEach()
    }
  }

  test("createView and loadView basic flow") {
    withSparkSession() { _ =>
      val view = doCreateView(testIdent)
      assert(view != null)
      assert(view.name() == s"${testNs(0)}.${testIdent.name}")

      val loaded = catalog.loadView(testIdent)
      assert(loaded.query() == "SELECT 1 AS id, 'a' AS data")
      assert(loaded.schema().fieldNames.toSeq == Seq("id", "data"))
      assert(loaded.queryColumnNames().toSeq == Seq("id", "data"))
      assert(loaded.currentCatalog() == "hive")
      assert(loaded.currentNamespace().toSeq == Seq("default"))
    }
  }

  test("createView with column aliases and comments") {
    withSparkSession() { _ =>
      doCreateView(
        testIdent,
        columnAliases = Array("user_id", "user_data"),
        columnComments = Array("primary id", null))
      val loaded = catalog.loadView(testIdent)
      assert(loaded.schema().fieldNames.toSeq == Seq("user_id", "user_data"))
      assert(loaded.columnAliases().toSeq == Seq("user_id", "user_data"))
      assert(loaded.columnComments()(0) == "primary id")
      assert(loaded.columnComments()(1) == null)
    }
  }

  test("createView throws ViewAlreadyExistsException when view already exists") {
    withSparkSession() { _ =>
      doCreateView(testIdent)
      intercept[ViewAlreadyExistsException] {
        doCreateView(testIdent)
      }
    }
  }

  test("createView throws ViewAlreadyExistsException when a table with same name exists") {
    withSparkSession() { _ =>
      // 先以 table 身份建出来
      catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)
      val ex = intercept[ViewAlreadyExistsException] {
        doCreateView(testIdent)
      }
      assert(ex != null)
    }
  }

  test("createView throws NoSuchNamespaceException when namespace does not exist") {
    withSparkSession() { _ =>
      val nonexistent = Identifier.of(Array("not_exist_db"), "v")
      intercept[NoSuchNamespaceException] {
        doCreateView(nonexistent)
      }
    }
  }

  test("loadView throws NoSuchViewException when target is a table") {
    withSparkSession() { _ =>
      catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)
      intercept[NoSuchViewException] {
        catalog.loadView(testIdent)
      }
    }
  }

  test("loadView throws NoSuchViewException when view does not exist") {
    withSparkSession() { _ =>
      intercept[NoSuchViewException] {
        catalog.loadView(testIdent)
      }
    }
  }

  test("viewExists returns false for table or missing view") {
    withSparkSession() { _ =>
      assert(!catalog.viewExists(testIdent))
      catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)
      assert(!catalog.viewExists(testIdent))
    }
  }

  test("viewExists returns true after createView") {
    withSparkSession() { _ =>
      doCreateView(testIdent)
      assert(catalog.viewExists(testIdent))
    }
  }

  test("listViews returns only views, not tables") {
    withSparkSession() { _ =>
      // 一个 table、两个 view
      catalog.createTable(Identifier.of(testNs, "t1"), schema, Array.empty[Transform], emptyProps)
      doCreateView(Identifier.of(testNs, "v1"))
      doCreateView(Identifier.of(testNs, "v2"))

      val views = catalog.listViews(testNs: _*).map(_.name()).toSet
      assert(views == Set("v1", "v2"))
    }
  }

  test("listViews throws NoSuchNamespaceException for missing namespace") {
    withSparkSession() { _ =>
      intercept[NoSuchNamespaceException] {
        catalog.listViews("missing_db")
      }
    }
  }

  test("alterView SetProperty / RemoveProperty works on user properties") {
    withSparkSession() { _ =>
      val props = new util.HashMap[String, String]()
      props.put("k1", "v1")
      doCreateView(testIdent, properties = props)

      catalog.alterView(testIdent, ViewChange.setProperty("k2", "v2"))
      var loaded = catalog.loadView(testIdent)
      assert(loaded.properties().asScala.get("k1").contains("v1"))
      assert(loaded.properties().asScala.get("k2").contains("v2"))

      catalog.alterView(testIdent, ViewChange.removeProperty("k1"))
      loaded = catalog.loadView(testIdent)
      assert(!loaded.properties().asScala.contains("k1"))
      assert(loaded.properties().asScala.get("k2").contains("v2"))
    }
  }

  test("alterView rejects modifying internal view.* properties") {
    withSparkSession() { _ =>
      doCreateView(testIdent)
      intercept[UnsupportedOperationException] {
        catalog.alterView(
          testIdent,
          ViewChange.setProperty(s"${CatalogTable.VIEW_PREFIX}query.out.numCols", "99"))
      }
      intercept[UnsupportedOperationException] {
        catalog.alterView(
          testIdent,
          ViewChange.removeProperty(CatalogTable.VIEW_CATALOG_AND_NAMESPACE))
      }
    }
  }

  test("alterView rejects modifying ViewCatalog reserved properties") {
    withSparkSession() { _ =>
      doCreateView(testIdent)
      intercept[UnsupportedOperationException] {
        catalog.alterView(testIdent, ViewChange.setProperty(ViewCatalog.PROP_COMMENT, "bad"))
      }
    }
  }

  test("alterView throws NoSuchViewException when target is a table") {
    withSparkSession() { _ =>
      catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)
      intercept[NoSuchViewException] {
        catalog.alterView(testIdent, ViewChange.setProperty("k", "v"))
      }
    }
  }

  test("dropView returns true after drop and false otherwise") {
    withSparkSession() { _ =>
      assert(!catalog.dropView(testIdent))
      doCreateView(testIdent)
      assert(catalog.dropView(testIdent))
      assert(!catalog.viewExists(testIdent))
      // 重复 drop 返回 false
      assert(!catalog.dropView(testIdent))
    }
  }

  test("dropView returns false when target is a table (does not drop the table)") {
    withSparkSession() { _ =>
      catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)
      assert(!catalog.dropView(testIdent))
      // 表仍然存在
      assert(catalog.tableExists(testIdent))
    }
  }

  test("renameView renames an existing view") {
    withSparkSession() { _ =>
      doCreateView(testIdent)
      val newIdent = Identifier.of(testNs, "test_view_renamed")
      catalog.renameView(testIdent, newIdent)
      assert(!catalog.viewExists(testIdent))
      assert(catalog.viewExists(newIdent))
    }
  }

  test("renameView throws NoSuchViewException when source is a table or missing") {
    withSparkSession() { _ =>
      // 缺失
      intercept[NoSuchViewException] {
        catalog.renameView(testIdent, Identifier.of(testNs, "any"))
      }
      // 是表
      catalog.createTable(testIdent, schema, Array.empty[Transform], emptyProps)
      intercept[NoSuchViewException] {
        catalog.renameView(testIdent, Identifier.of(testNs, "any"))
      }
    }
  }

  test("renameView throws when target already exists") {
    withSparkSession() { _ =>
      doCreateView(testIdent)
      // 按 ViewCatalog 接口约定，target 是 table 时也应抛 ViewAlreadyExistsException
      val targetTable = Identifier.of(testNs, "target_table")
      catalog.createTable(targetTable, schema, Array.empty[Transform], emptyProps)
      intercept[ViewAlreadyExistsException] {
        catalog.renameView(testIdent, targetTable)
      }

      val targetView = Identifier.of(testNs, "target_view")
      doCreateView(targetView)
      intercept[ViewAlreadyExistsException] {
        catalog.renameView(testIdent, targetView)
      }
    }
  }
}
