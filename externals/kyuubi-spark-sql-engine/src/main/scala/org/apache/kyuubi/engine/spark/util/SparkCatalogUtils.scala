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

import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{CatalogExtension, CatalogPlugin, Identifier, SupportsNamespaces, TableCatalog, View, ViewCatalog}
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.kyuubi.Logging
import org.apache.kyuubi.engine.spark.schema.SchemaHelper
import org.apache.kyuubi.util.reflect.ReflectUtils._

/**
 * A shim that defines the interface interact with Spark's catalogs
 */
object SparkCatalogUtils extends Logging {

  private val VIEW = "VIEW"
  private val TABLE = "TABLE"

  val SESSION_CATALOG: String = "spark_catalog"
  val sparkTableTypes: Set[String] = Set(VIEW, TABLE)

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                          Catalog                                             //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  // SPARK-46050 (4.2.0) changed CatalogManager from a class to an interface, breaking
  // binary compatibility when compiled against Spark 3.5 and run against Spark 4.2.
  // Access catalogManager reflectively to avoid IncompatibleClassChangeError.
  def catalogManager(spark: SparkSession): AnyRef =
    invokeAs[AnyRef](spark.sessionState, "catalogManager")

  def setCurrentNamespace(spark: SparkSession, namespace: Array[String]): Unit = {
    val mgr = catalogManager(spark)
    val method = mgr.getClass.getMethod("setCurrentNamespace", classOf[Array[String]])
    try {
      method.invoke(mgr, namespace)
    } catch {
      case e: java.lang.reflect.InvocationTargetException => throw e.getCause
    }
  }

  def currentCatalog(spark: SparkSession): CatalogPlugin =
    invokeAs[CatalogPlugin](catalogManager(spark), "currentCatalog")

  def currentNamespace(spark: SparkSession): Array[String] =
    invokeAs[Array[String]](catalogManager(spark), "currentNamespace")

  def catalog(spark: SparkSession, name: String): CatalogPlugin =
    invokeAs[CatalogPlugin](catalogManager(spark), "catalog", (classOf[String], name))

  def isCatalogRegistered(spark: SparkSession, name: String): Boolean =
    invokeAs[Boolean](catalogManager(spark), "isCatalogRegistered", (classOf[String], name))

  def listViews(vc: ViewCatalog, ns: Array[String]): Array[Identifier] =
    invokeAs[Array[Identifier]](vc, "listViews", (classOf[Array[String]], ns))

  def loadView(vc: ViewCatalog, ident: Identifier): View =
    invokeAs[View](vc, "loadView", (classOf[Identifier], ident))

  /**
   * Note that the result only contains loaded catalogs because catalogs are lazily loaded in Spark.
   */
  def getCatalogs(spark: SparkSession): Seq[Row] = {
    val catalogMgr = catalogManager(spark)
    // get the custom v2 session catalog or default spark_catalog
    val sessionCatalog = invokeAs[AnyRef](catalogMgr, "v2SessionCatalog")
    val defaultCatalog = invokeAs[CatalogPlugin](catalogMgr, "currentCatalog")

    val defaults = Seq(sessionCatalog, defaultCatalog).distinct.map(invokeAs[String](_, "name"))
    val catalogs = getField[scala.collection.Map[String, _]](catalogMgr, "catalogs")
    (catalogs.keys ++: defaults).distinct.map(Row(_))
  }

  def getCatalog(spark: SparkSession, catalogName: String): CatalogPlugin = {
    if (StringUtils.isBlank(catalogName)) {
      currentCatalog(spark)
    } else {
      catalog(spark, catalogName)
    }
  }

  def setCurrentCatalog(spark: SparkSession, catalog: String): Unit = {
    // SPARK-36841 (3.3.0) Ensure setCurrentCatalog method catalog must exist
    if (isCatalogRegistered(spark, catalog)) {
      invokeAs[Unit](catalogManager(spark), "setCurrentCatalog", (classOf[String], catalog))
    } else {
      throw new IllegalArgumentException(s"Cannot find catalog plugin class for catalog '$catalog'")
    }
  }

  // SPARK-50700 (4.0.0) adds the `builtin` magic value
  private def hasCustomSessionCatalog(spark: SparkSession): Boolean = {
    spark.conf.get(s"spark.sql.catalog.$SESSION_CATALOG", "builtin") != "builtin"
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                           Schema                                             //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Return a list of [[Row]]s, with 2 fields `schemaName: String, catalogName: String`
   */
  def getSchemas(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String): Seq[Row] = {
    val catalog = getCatalog(spark, catalogName)
    val dbs = if (catalog.name() == SESSION_CATALOG && !hasCustomSessionCatalog(spark)) {
      spark.sessionState.catalog.listDatabases(schemaPattern)
    } else {
      getSchemasWithPattern(catalog, schemaPattern)
    }
    lazy val globalTempDb = getGlobalTempViewManager(spark, schemaPattern)
    val schemaNames = if (catalog.name() == SESSION_CATALOG) dbs ++ globalTempDb else dbs
    schemaNames.map(Row(_, catalog.name()))
  }

  private def getGlobalTempViewManager(
      spark: SparkSession,
      schemaPattern: String): Seq[String] = {
    val database = spark.conf.get("spark.sql.globalTempDatabase")
    Option(database).filter(_.matches(schemaPattern)).toSeq
  }

  private def listAllNamespaces(
      catalog: SupportsNamespaces,
      namespaces: Array[Array[String]]): Array[Array[String]] = {
    val children = namespaces.flatMap { ns =>
      catalog.listNamespaces(ns)
    }
    if (children.isEmpty) {
      namespaces
    } else {
      namespaces ++: listAllNamespaces(catalog, children)
    }
  }

  private def listAllNamespaces(catalog: CatalogPlugin): Array[Array[String]] = {
    catalog match {
      case catalog: CatalogExtension =>
        // DSv2 does not support pass schemaPattern transparently
        catalog.defaultNamespace() +: catalog.listNamespaces(Array())
      case catalog: SupportsNamespaces =>
        val rootSchema = catalog.listNamespaces()
        val allSchemas = listAllNamespaces(catalog, rootSchema)
        allSchemas
    }
  }

  private def listNamespacesWithPattern(
      catalog: CatalogPlugin,
      schemaPattern: String): Array[Array[String]] = {
    listAllNamespaces(catalog).filter { ns =>
      val quoted = ns.map(quoteIfNeeded).mkString(".")
      schemaPattern.r.pattern.matcher(quoted).matches()
    }.map(_.toList).toList.distinct.map(_.toArray).toArray
  }

  private def getSchemasWithPattern(catalog: CatalogPlugin, schemaPattern: String): Seq[String] = {
    val p = schemaPattern.r.pattern
    listAllNamespaces(catalog).flatMap { ns =>
      val quoted = ns.map(quoteIfNeeded).mkString(".")
      if (p.matcher(quoted).matches()) Some(quoted) else None
    }.distinct
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                        Table & View                                          //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  private def toTableOrViewRow(
      catalogName: String,
      ident: Identifier,
      tableType: String,
      comment: String): Row = {
    val schema = ident.namespace().map(quoteIfNeeded).mkString(".")
    val name = quoteIfNeeded(ident.name())
    Row(catalogName, schema, name, tableType, comment, null, null, null, null, null)
  }

  private def toColumns(
      schema: StructType,
      catalogName: String,
      ident: Identifier,
      columnPattern: Pattern): Seq[Row] = {
    val namespace = ident.namespace().map(quoteIfNeeded).mkString(".")
    val name = quoteIfNeeded(ident.name())
    schema.zipWithIndex.filter(f => columnPattern.matcher(f._1.name).matches())
      .map { case (f, i) => toColumnResult(catalogName, namespace, name, f, i) }
  }

  /**
   * List matched (identifier, tableType) pairs from a v2 catalog. `tableType` is either
   * [[TABLE]] or [[VIEW]].
   *
   * Handles the tricky "custom session catalog" case (e.g. Iceberg's `SparkSessionCatalog`),
   * where `listViews` returns empty (because the underlying `V2SessionCatalog` does not
   * implement `ViewCatalog`) while `listTables` still returns v1 HMS views mixed with tables.
   * In that case the v1 `SessionCatalog` is used to probe `tableType` for each identifier.
   */
  private def listTablesAndViews(
      spark: SparkSession,
      catalog: CatalogPlugin,
      namespaces: Array[Array[String]],
      tablePattern: String): Seq[(Identifier, String)] = {
    val tp = tablePattern.r.pattern
    def matches(i: Identifier): Boolean = tp.matcher(quoteIfNeeded(i.name())).matches()
    def key(i: Identifier): (Seq[String], String) = (i.namespace().toSeq, i.name())

    if (catalog.name() == SESSION_CATALOG && hasCustomSessionCatalog(spark)) {
      // Probe every identifier via v1 SessionCatalog to determine TABLE vs VIEW.
      val sessionCatalog = spark.sessionState.catalog
      catalog match {
        case tc: TableCatalog =>
          namespaces.toSeq.flatMap(ns => tc.listTables(ns).filter(matches)).map { ident =>
            val tpe =
              try {
                val ti = TableIdentifier(ident.name(), ident.namespace().headOption)
                if (sessionCatalog.getTableMetadata(ti).tableType.name == VIEW) VIEW else TABLE
              } catch {
                case _: Exception => TABLE
              }
            ident -> tpe
          }
        case _ => Seq.empty
      }
    } else {
      val views = catalog match {
        case vc: ViewCatalog => namespaces.toSeq.flatMap(ns => listViews(vc, ns).filter(matches))
        case _ => Seq.empty
      }
      val viewKeys = views.map(key).toSet
      val tables = catalog match {
        case tc: TableCatalog =>
          namespaces.toSeq
            .flatMap(ns => tc.listTables(ns).filter(matches))
            .filterNot(i => viewKeys.contains(key(i)))
        case _ => Seq.empty
      }
      tables.map(_ -> TABLE) ++ views.map(_ -> VIEW)
    }
  }

  def getCatalogTablesOrViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      tableTypes: Set[String],
      ignoreTableProperties: Boolean = false): Seq[Row] = {
    val catalog = getCatalog(spark, catalogName)
    catalog match {
      case _ if catalog.name() == SESSION_CATALOG && !hasCustomSessionCatalog(spark) =>
        val sessionCatalog = spark.sessionState.catalog
        val databases = sessionCatalog.listDatabases(schemaPattern)

        def isMatchedTableType(tableTypes: Set[String], tableType: String): Boolean = {
          val typ = if (tableType.equalsIgnoreCase(VIEW)) VIEW else TABLE
          tableTypes.exists(typ.equalsIgnoreCase)
        }

        databases.flatMap { db =>
          val identifiers =
            sessionCatalog.listTables(db, tablePattern, includeLocalTempViews = false)
          if (ignoreTableProperties) {
            identifiers.map { ti: TableIdentifier =>
              Row(
                catalogName,
                ti.database.getOrElse("default"),
                ti.table,
                TABLE, // ignore tableTypes criteria and simply treat all table type as TABLE
                "",
                null,
                null,
                null,
                null,
                null)
            }
          } else {
            sessionCatalog.getTablesByName(identifiers)
              .filter(t => isMatchedTableType(tableTypes, t.tableType.name)).map { t =>
                val typ = if (t.tableType.name == VIEW) VIEW else TABLE
                Row(
                  catalogName,
                  t.database,
                  t.identifier.table,
                  typ,
                  t.comment.getOrElse(""),
                  null,
                  null,
                  null,
                  null,
                  null)
              }
          }
        }

      case _: TableCatalog | _: ViewCatalog =>
        val namespaces = listNamespacesWithPattern(catalog, schemaPattern)
        val identifiers = listTablesAndViews(spark, catalog, namespaces, tablePattern)
        val wantTable = tableTypes.exists(_.equalsIgnoreCase(TABLE))
        val wantView = tableTypes.exists(_.equalsIgnoreCase(VIEW))

        identifiers.flatMap {
          case (ident, TABLE) if wantTable =>
            val comment =
              if (ignoreTableProperties) ""
              else catalog match {
                case tc: TableCatalog =>
                  // loadTable is a time-consuming operation
                  tc.loadTable(ident).properties().getOrDefault(TableCatalog.PROP_COMMENT, "")
                case _ => ""
              }
            Some(toTableOrViewRow(catalog.name(), ident, TABLE, comment))

          case (ident, VIEW) if wantView =>
            val comment =
              if (ignoreTableProperties) ""
              else {
                // Prefer v2 ViewCatalog.loadView; fall back to v1 SessionCatalog for v1
                // HMS views exposed by a custom session catalog (e.g. Iceberg).
                // SPARK-52729 (Spark 4.2) dropped ViewCatalog.PROP_COMMENT, so use
                // TableCatalog.PROP_COMMENT for compatibility.
                val v2Comment = catalog match {
                  case vc: ViewCatalog =>
                    try {
                      Some(loadView(vc, ident).properties()
                        .getOrDefault(TableCatalog.PROP_COMMENT, ""))
                    } catch { case _: Exception => None }
                  case _ => None
                }
                v2Comment.getOrElse {
                  try {
                    val ti = TableIdentifier(ident.name(), ident.namespace().headOption)
                    spark.sessionState.catalog.getTableMetadata(ti).comment.getOrElse("")
                  } catch { case _: Exception => "" }
                }
              }
            Some(toTableOrViewRow(catalog.name(), ident, VIEW, comment))

          case _ => None
        }

      case _ => Seq.empty
    }
  }

  private def getColumnsByCatalog(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: Pattern): Seq[Row] = {
    val catalog = getCatalog(spark, catalogName)

    catalog match {
      case _ if catalog.name() == SESSION_CATALOG && !hasCustomSessionCatalog(spark) =>
        val sessionCatalog = spark.sessionState.catalog
        val databases = sessionCatalog.listDatabases(schemaPattern)
        databases.flatMap { db =>
          val identifiers =
            sessionCatalog.listTables(db, tablePattern, includeLocalTempViews = true)
          sessionCatalog.getTablesByName(identifiers).flatMap { t =>
            t.schema.zipWithIndex.filter(f => columnPattern.matcher(f._1.name).matches())
              .map { case (f, i) =>
                toColumnResult(catalog.name(), t.database, t.identifier.table, f, i)
              }
          }
        }

      case _: TableCatalog | _: ViewCatalog =>
        val namespaces = listNamespacesWithPattern(catalog, schemaPattern)
        val identifiers = listTablesAndViews(spark, catalog, namespaces, tablePattern)

        identifiers.flatMap {
          case (ident, TABLE) => catalog match {
              case tc: TableCatalog =>
                toColumns(tc.loadTable(ident).schema(), catalog.name(), ident, columnPattern)
              case _ => Seq.empty
            }
          case (ident, VIEW) =>
            // Prefer v2 ViewCatalog.loadView; fall back to v1 SessionCatalog for v1
            // HMS views exposed by a custom session catalog (e.g. Iceberg).
            val v2Schema = catalog match {
              case vc: ViewCatalog =>
                try Some(loadView(vc, ident).schema())
                catch { case _: Exception => None }
              case _ => None
            }
            val schema = v2Schema.getOrElse {
              try {
                val ti = TableIdentifier(ident.name(), ident.namespace().headOption)
                spark.sessionState.catalog.getTableMetadata(ti).schema
              } catch { case _: Exception => new StructType() }
            }
            toColumns(schema, catalog.name(), ident, columnPattern)
        }

      case _ => Seq.empty
    }
  }

  def getTempViews(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String): Seq[Row] = {
    val views = getViews(spark, schemaPattern, tablePattern)
    views.map { ident =>
      Row(catalogName, ident.database.orNull, ident.table, VIEW, "", null, null, null, null, null)
    }
  }

  private def getViews(
      spark: SparkSession,
      schemaPattern: String,
      tablePattern: String): Seq[TableIdentifier] = {
    val db = getGlobalTempViewManager(spark, schemaPattern)
    if (db.nonEmpty) {
      spark.sessionState.catalog.listTables(db.head, tablePattern)
    } else {
      spark.sessionState.catalog.listLocalTempViews(tablePattern)
    }
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                          Columns                                            //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  def getColumns(
      spark: SparkSession,
      catalogName: String,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: String): Seq[Row] = {

    val cp = columnPattern.r.pattern
    val byCatalog = getColumnsByCatalog(spark, catalogName, schemaPattern, tablePattern, cp)
    val byGlobalTmpDB = getColumnsByGlobalTempViewManager(spark, schemaPattern, tablePattern, cp)
    val byLocalTmp = getColumnsByLocalTempViews(spark, tablePattern, cp)

    byCatalog ++ byGlobalTmpDB ++ byLocalTmp
  }

  private def getColumnsByGlobalTempViewManager(
      spark: SparkSession,
      schemaPattern: String,
      tablePattern: String,
      columnPattern: Pattern): Seq[Row] = {
    val catalog = spark.sessionState.catalog

    getGlobalTempViewManager(spark, schemaPattern).flatMap { globalTmpDb =>
      catalog.globalTempViewManager.listViewNames(tablePattern).flatMap { v =>
        catalog.globalTempViewManager.get(v).map { plan =>
          plan.schema.zipWithIndex.filter(f => columnPattern.matcher(f._1.name).matches())
            .map { case (f, i) =>
              toColumnResult(SparkCatalogUtils.SESSION_CATALOG, globalTmpDb, v, f, i)
            }
        }
      }.flatten
    }
  }

  private def getColumnsByLocalTempViews(
      spark: SparkSession,
      tablePattern: String,
      columnPattern: Pattern): Seq[Row] = {
    val catalog = spark.sessionState.catalog

    catalog.listLocalTempViews(tablePattern)
      .map(v => (v, catalog.getTempView(v.table).get))
      .flatMap { case (v, plan) =>
        plan.schema.zipWithIndex
          .filter(f => columnPattern.matcher(f._1.name).matches())
          .map { case (f, i) =>
            toColumnResult(SparkCatalogUtils.SESSION_CATALOG, null, v.table, f, i)
          }
      }
  }

  private def toColumnResult(
      catalog: String,
      db: String,
      table: String,
      col: StructField,
      pos: Int): Row = {
    // format: off
    Row(
      catalog,                                              // TABLE_CAT
      db,                                                   // TABLE_SCHEM
      table,                                                // TABLE_NAME
      col.name,                                             // COLUMN_NAME
      SchemaHelper.toJavaSQLType(col.dataType),             // DATA_TYPE
      col.dataType.sql,                                     // TYPE_NAME
      SchemaHelper.getColumnSize(col.dataType).orNull,      // COLUMN_SIZE
      null,                                                 // BUFFER_LENGTH
      SchemaHelper.getDecimalDigits(col.dataType).orNull,   // DECIMAL_DIGITS
      SchemaHelper.getNumPrecRadix(col.dataType).orNull,    // NUM_PREC_RADIX
      if (col.nullable) 1 else 0,                           // NULLABLE
      col.getComment().getOrElse(""),                       // REMARKS
      null,                                                 // COLUMN_DEF
      null,                                                 // SQL_DATA_TYPE
      null,                                                 // SQL_DATETIME_SUB
      null,                                                 // CHAR_OCTET_LENGTH
      pos,                                                  // ORDINAL_POSITION
      "YES",                                                // IS_NULLABLE
      null,                                                 // SCOPE_CATALOG
      null,                                                 // SCOPE_SCHEMA
      null,                                                 // SCOPE_TABLE
      null,                                                 // SOURCE_DATA_TYPE
      "NO"                                                  // IS_AUTO_INCREMENT
    )
    // format: on
  }

  // SPARK-47300 (4.0.0): quoteIfNeeded should quote identifier starts with digits
  private val validIdentPattern = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*")

  // Forked from Apache Spark's [[org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIfNeeded]]
  def quoteIfNeeded(part: String): String = {
    if (validIdentPattern.matcher(part).matches()) {
      part
    } else {
      quoteIdentifier(part)
    }
  }

  // Forked from Apache Spark's [[org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIdentifier]]
  def quoteIdentifier(name: String): String = {
    // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
    // identifier with back-ticks.
    "`" + name.replace("`", "``") + "`"
  }
}
