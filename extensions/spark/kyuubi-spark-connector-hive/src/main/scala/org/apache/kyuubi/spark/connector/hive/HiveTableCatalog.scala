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

import java.lang.{Boolean => JBoolean, Long => JLong}
import java.net.URI
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{CurrentUserContext, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, NoSuchViewException, TableAlreadyExistsException, ViewAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange, View, ViewCatalog, ViewChange}
import org.apache.spark.sql.connector.catalog.NamespaceChange.RemoveProperty
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.HiveUDFExpressionBuilder
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper._
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, GLOBAL_TEMP_DATABASE}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.spark.connector.hive.HiveConnectorUtils.withSparkSQLConf
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog.{getStorageFormatAndProvider, toCatalogDatabase, CatalogDatabaseHelper, HIVE_TABLE_RESERVED_SERDE_PROPERTIES, IdentifierHelper, NamespaceHelper}
import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorConf.DROP_TABLE_AS_PURGE_TABLE
import org.apache.kyuubi.spark.connector.hive.KyuubiHiveConnectorDelegationTokenProvider.metastoreTokenSignature
import org.apache.kyuubi.util.reflect.{DynClasses, DynConstructors}

/**
 * A [[TableCatalog]] that wrap HiveExternalCatalog to as V2 CatalogPlugin instance to access Hive.
 */
class HiveTableCatalog(sparkSession: SparkSession)
  extends TableCatalog with SQLConfHelper with SupportsNamespaces with ViewCatalog with Logging {

  def this() = this(SparkSession.active)

  private val externalCatalogManager = ExternalCatalogManager.getOrCreate(sparkSession)

  private val LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME = "spark.sql.legacy.v1IdentifierNoCatalog"

  private val sc = sparkSession.sparkContext

  private val sessionState = sparkSession.sessionState

  private var catalogName: String = _

  private var catalogOptions: CaseInsensitiveStringMap = _

  var catalog: HiveSessionCatalog = _

  val NAMESPACE_RESERVED_PROPERTIES =
    Seq(
      SupportsNamespaces.PROP_COMMENT,
      SupportsNamespaces.PROP_LOCATION,
      SupportsNamespaces.PROP_OWNER)

  private lazy val hadoopConf: Configuration = {
    val conf = sparkSession.sessionState.newHadoopConf()
    catalogOptions.asScala.foreach { case (k, v) => conf.set(k, v) }
    if (catalogOptions.containsKey("hive.metastore.uris")) {
      conf.set("hive.metastore.token.signature", metastoreTokenSignature(catalogOptions))
    }
    conf
  }

  private lazy val sparkConf: SparkConf = {
    val conf = sparkSession.sparkContext.getConf
    catalogOptions.asScala.foreach {
      case (k, v) => conf.set(k, v)
    }
    conf
  }

  def hadoopConfiguration(): Configuration = hadoopConf

  override def name(): String = {
    require(catalogName != null, "The Hive table catalog is not initialed")
    catalogName
  }

  private def newHiveMetastoreCatalog(sparkSession: SparkSession): HiveMetastoreCatalog = {
    val sparkSessionClz = DynClasses.builder()
      .impl("org.apache.spark.sql.classic.SparkSession") // SPARK-49700 (4.0.0)
      .impl("org.apache.spark.sql.SparkSession")
      .buildChecked()

    val hiveMetastoreCatalogCtor =
      DynConstructors.builder()
        .impl("org.apache.spark.sql.hive.HiveMetastoreCatalog", sparkSessionClz)
        .buildChecked[HiveMetastoreCatalog]()

    hiveMetastoreCatalogCtor.newInstanceChecked(sparkSession)
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    assert(catalogName == null, "The Hive table catalog is already initialed.")
    assert(
      conf.getConf(CATALOG_IMPLEMENTATION) == "hive",
      s"Require setting ${CATALOG_IMPLEMENTATION.key} to `hive` to enable hive support.")
    catalogName = name
    catalogOptions = options
    catalog = new HiveSessionCatalog(
      externalCatalogBuilder = () => externalCatalog,
      globalTempViewManagerBuilder = () => globalTempViewManager,
      metastoreCatalog = newHiveMetastoreCatalog(sparkSession),
      functionRegistry = sessionState.functionRegistry,
      tableFunctionRegistry = sessionState.tableFunctionRegistry,
      hadoopConf = hadoopConf,
      parser = sessionState.sqlParser,
      functionResourceLoader = sessionState.resourceLoader,
      HiveUDFExpressionBuilder)
  }

  private lazy val globalTempViewManager: GlobalTempViewManager = {
    val globalTempDB = conf.getConf(GLOBAL_TEMP_DATABASE)
    if (externalCatalog.databaseExists(globalTempDB)) {
      throw KyuubiHiveConnectorException(
        s"$globalTempDB is a system preserved database, please rename your existing database to " +
          s"resolve the name conflict, or set a different value for ${GLOBAL_TEMP_DATABASE.key}, " +
          "and launch your Spark application again.")
    }
    new GlobalTempViewManager(globalTempDB)
  }

  /**
   * A catalog that interacts with external systems.
   */
  lazy val externalCatalog: ExternalCatalogWithListener = {
    val externalCatalog = externalCatalogManager.take(Ticket(catalogName, sparkConf, hadoopConf))

    // Wrap to provide catalog events
    val wrapped = new ExternalCatalogWithListener(externalCatalog)

    // Make sure we propagate external catalog events to the spark listener bus
    wrapped.addListener((event: ExternalCatalogEvent) => postExternalCatalogEvent(sc, event))

    wrapped
  }

  override val defaultNamespace: Array[String] = Array("default")

  override def listTables(namespace: Array[String]): Array[Identifier] =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array(db) =>
          catalog
            .listTables(db)
            .map(ident =>
              Identifier.of(ident.database.map(Array(_)).getOrElse(Array()), ident.table))
            .toArray
        case _ =>
          throw new NoSuchNamespaceException(namespace)
      }
    }

  override def loadTable(ident: Identifier): Table =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      HiveTable(sparkSession, catalog.getTableMetadata(ident.asTableIdentifier), this)
    }

  // scalastyle:off
  private def newCatalogTable(
      identifier: TableIdentifier,
      tableType: CatalogTableType,
      storage: CatalogStorageFormat,
      schema: StructType,
      provider: Option[String] = None,
      partitionColumnNames: Seq[String] = Seq.empty,
      bucketSpec: Option[BucketSpec] = None,
      owner: String = Option(CurrentUserContext.CURRENT_USER.get()).getOrElse(""),
      createTime: JLong = System.currentTimeMillis,
      lastAccessTime: JLong = -1,
      createVersion: String = "",
      properties: Map[String, String] = Map.empty,
      stats: Option[CatalogStatistics] = None,
      viewText: Option[String] = None,
      comment: Option[String] = None,
      collation: Option[String] = None,
      unsupportedFeatures: Seq[String] = Seq.empty,
      tracksPartitionsInCatalog: JBoolean = false,
      schemaPreservesCase: JBoolean = true,
      ignoredProperties: Map[String, String] = Map.empty,
      viewOriginalText: Option[String] = None): CatalogTable = {
    // scalastyle:on
    Try { // SPARK-50675 (4.0.0)
      DynConstructors.builder()
        .impl(
          classOf[CatalogTable],
          classOf[TableIdentifier],
          classOf[CatalogTableType],
          classOf[CatalogStorageFormat],
          classOf[StructType],
          classOf[Option[String]],
          classOf[Seq[String]],
          classOf[Option[BucketSpec]],
          classOf[String],
          classOf[Long],
          classOf[Long],
          classOf[String],
          classOf[Map[String, String]],
          classOf[Option[CatalogStatistics]],
          classOf[Option[String]],
          classOf[Option[String]],
          classOf[Option[String]],
          classOf[Seq[String]],
          classOf[Boolean],
          classOf[Boolean],
          classOf[Map[String, String]],
          classOf[Option[String]])
        .buildChecked()
        .invokeChecked[CatalogTable](
          null,
          identifier,
          tableType,
          storage,
          schema,
          provider,
          partitionColumnNames,
          bucketSpec,
          owner,
          createTime,
          lastAccessTime,
          createVersion,
          properties,
          stats,
          viewText,
          comment,
          collation,
          unsupportedFeatures,
          tracksPartitionsInCatalog,
          schemaPreservesCase,
          ignoredProperties,
          viewOriginalText)
    }.recover { case _: Exception => // Spark 3.5 and previous
      DynConstructors.builder()
        .impl(
          classOf[CatalogTable],
          classOf[TableIdentifier],
          classOf[CatalogTableType],
          classOf[CatalogStorageFormat],
          classOf[StructType],
          classOf[Option[String]],
          classOf[Seq[String]],
          classOf[Option[BucketSpec]],
          classOf[String],
          classOf[Long],
          classOf[Long],
          classOf[String],
          classOf[Map[String, String]],
          classOf[Option[CatalogStatistics]],
          classOf[Option[String]],
          classOf[Option[String]],
          classOf[Seq[String]],
          classOf[Boolean],
          classOf[Boolean],
          classOf[Map[String, String]],
          classOf[Option[String]])
        .buildChecked()
        .invokeChecked[CatalogTable](
          null,
          identifier,
          tableType,
          storage,
          schema,
          provider,
          partitionColumnNames,
          bucketSpec,
          owner,
          createTime,
          lastAccessTime,
          createVersion,
          properties,
          stats,
          viewText,
          comment,
          unsupportedFeatures,
          tracksPartitionsInCatalog,
          schemaPreservesCase,
          ignoredProperties,
          viewOriginalText)
    }.get
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.TransformHelper
      val (partitionColumns, maybeBucketSpec) = partitions.toSeq.convertTransforms
      val location = Option(properties.get(TableCatalog.PROP_LOCATION))
      val maybeProvider = Option(properties.get(TableCatalog.PROP_PROVIDER))
      val allProps = properties.asScala.toMap
      val (optionsProps, serdeProps) = toOptionsAndSerdeProps(allProps)
      val (storage, provider) =
        getStorageFormatAndProvider(
          maybeProvider,
          location,
          allProps,
          optionsProps,
          serdeProps)
      val isExternal = properties.containsKey(TableCatalog.PROP_EXTERNAL)
      val tableType =
        if (isExternal || location.isDefined) {
          CatalogTableType.EXTERNAL
        } else {
          CatalogTableType.MANAGED
        }

      val tableDesc = newCatalogTable(
        identifier = ident.asTableIdentifier,
        tableType = tableType,
        storage = storage,
        schema = schema,
        provider = Some(provider),
        partitionColumnNames = partitionColumns,
        bucketSpec = maybeBucketSpec,
        properties = toTableProps(allProps, optionsProps ++ serdeProps),
        tracksPartitionsInCatalog = conf.manageFilesourcePartitions,
        comment = Option(properties.get(TableCatalog.PROP_COMMENT)))

      try {
        catalog.createTable(tableDesc, ignoreIfExists = false)
      } catch {
        case _: TableAlreadyExistsException =>
          throw new TableAlreadyExistsException(ident)
      }

      loadTable(ident)
    }

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      val catalogTable =
        try {
          catalog.getTableMetadata(ident.asTableIdentifier)
        } catch {
          case _: NoSuchTableException =>
            throw new NoSuchTableException(ident)
        }

      val properties = CatalogV2Util.applyPropertiesChanges(catalogTable.properties, changes)
      val schema = HiveConnectorUtils.applySchemaChanges(
        catalogTable.schema,
        changes)
      val comment = properties.get(TableCatalog.PROP_COMMENT)
      val owner = properties.getOrElse(TableCatalog.PROP_OWNER, catalogTable.owner)
      val location = properties.get(TableCatalog.PROP_LOCATION).map(CatalogUtils.stringToURI)
      val storage =
        if (location.isDefined) {
          catalogTable.storage.copy(locationUri = location)
        } else {
          catalogTable.storage
        }

      try {
        catalog.alterTable(
          catalogTable.copy(
            properties = properties,
            schema = schema,
            owner = owner,
            comment = comment,
            storage = storage))
      } catch {
        case _: NoSuchTableException =>
          throw new NoSuchTableException(ident)
      }

      loadTable(ident)
    }

  override def purgeTable(ident: Identifier): Boolean = {
    dropTableInternal(ident, purge = true)
  }

  override def dropTable(ident: Identifier): Boolean = {
    val purge = sessionState.conf.getConf(DROP_TABLE_AS_PURGE_TABLE)
    dropTableInternal(ident, purge)
  }

  private def dropTableInternal(ident: Identifier, purge: Boolean): Boolean =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      try {
        if (loadTable(ident) != null) {
          catalog.dropTable(
            ident.asTableIdentifier,
            ignoreIfNotExists = true,
            purge /* whether to skip HDFS trash */ )
          true
        } else {
          false
        }
      } catch {
        case _: NoSuchTableException =>
          false
      }
    }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      if (tableExists(newIdent)) {
        throw new TableAlreadyExistsException(newIdent)
      }

      // Load table to make sure the table exists
      loadTable(oldIdent)
      catalog.renameTable(oldIdent.asTableIdentifier, newIdent.asTableIdentifier)
    }

  /**
   * Splits properties into optionsProps and serdeProps based on the `options.` prefix.
   *
   * - optionsProps: keys with "options." prefix whose stripped key ALREADY exist in properties,
   *   indicating they were originally specified via OPTIONS clause.
   * - serdeProps: keys with "options." prefix whose stripped key does NOT exists in properties,
   *   indicating they were originally specified via SERDEPROPERTIES clause.
   *
   * @param properties the full properties map
   * @return a tuple of (optionsProps, serdeProps), both with the "options." prefix stripped
   */
  private[hive] def toOptionsAndSerdeProps(
      properties: Map[String, String]): (Map[String, String], Map[String, String]) = {
    val (optionsProps, serdeProps) = properties
      .filterKeys(_.startsWith(TableCatalog.OPTION_PREFIX))
      .map { case (key, value) => key.drop(TableCatalog.OPTION_PREFIX.length) -> value }
      .toMap
      .partition { case (strippedKey, _) => properties.contains(strippedKey) }
    (optionsProps, serdeProps)
  }

  /**
   * Return table properties to be stored in the Hive metastore after excluding the following:
   *
   * - Excludes `CatalogV2Util.TABLE_RESERVED_PROPERTIES`.
   * - Excludes keys with the `options.` prefix.
   * - Excludes stripped keys already extracted from OPTIONS or SERDEPROPERTIES.
   * - Excludes Hive SerDe/storage keys such as `hive.serde` and `hive.stored-as`.
   *
   * @param properties the full properties map
   * @param optionsAndSerdeProps stripped keys extracted from OPTIONS and SERDEPROPERTIES
   * @return table properties to be stored in the Hive metastore
   */
  private[hive] def toTableProps(
      properties: Map[String, String],
      optionsAndSerdeProps: Map[String, String]): Map[String, String] = {
    properties.filterKeys(key =>
      !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(key)
        && !key.startsWith(TableCatalog.OPTION_PREFIX)
        && !optionsAndSerdeProps.contains(key)
        && !HIVE_TABLE_RESERVED_SERDE_PROPERTIES.contains(key)).toMap
  }

  override def listNamespaces(): Array[Array[String]] =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      catalog.listDatabases().map(Array(_)).toArray
    }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array() =>
          listNamespaces()
        case Array(db) if catalog.databaseExists(db) =>
          Array()
        case _ =>
          throw new NoSuchNamespaceException(namespace)
      }
    }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array(db) =>
          try {
            catalog.getDatabaseMetadata(db).toMetadata
          } catch {
            case _: NoSuchDatabaseException =>
              throw new NoSuchNamespaceException(namespace)
          }

        case _ =>
          throw new NoSuchNamespaceException(namespace)
      }
    }

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array(db) if !catalog.databaseExists(db) =>
          catalog.createDatabase(
            toCatalogDatabase(db, metadata, defaultLocation = Some(getCatalogDefaultDBPath(db))),
            ignoreIfExists = false)

        case Array(_) =>
          throw new NamespaceAlreadyExistsException(namespace)

        case _ =>
          throw new IllegalArgumentException(s"Invalid namespace name: ${namespace.quoted}")
      }
    }

  /**
   * Returns the default database path with catalog-level warehouse configuration precedence.
   *
   * This method resolves the database path using the following priority order:
   *   1. Catalog-level `spark.sql.catalog.<catalog>.hive.metastore.warehouse.dir`
   *   2. Global-level `spark.sql.warehouse.dir` (Underlying)
   *
   * @param db database name
   * @return qualified URI path for the database
   */
  private def getCatalogDefaultDBPath(db: String): URI = {
    Option(catalogOptions.get("hive.metastore.warehouse.dir")).filter(_.nonEmpty) match {
      case Some(dir) =>
        CatalogUtils.makeQualifiedDBObjectPath(catalog.getDefaultDBPath(db), dir, hadoopConf)
      case None =>
        catalog.getDefaultDBPath(db)
    }
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array(db) =>
          // validate that this catalog's reserved properties are not removed
          changes.foreach {
            case remove: RemoveProperty
                if NAMESPACE_RESERVED_PROPERTIES.contains(remove.property) =>
              throw new UnsupportedOperationException(
                s"Cannot remove reserved property: ${remove.property}")
            case _ =>
          }

          val metadata = catalog.getDatabaseMetadata(db).toMetadata
          catalog.alterDatabase(
            toCatalogDatabase(db, CatalogV2Util.applyNamespaceChanges(metadata, changes)))

        case _ =>
          throw new NoSuchNamespaceException(namespace)
      }
    }

  /**
   * List the metadata of partitions that belong to the specified table, assuming it exists, that
   * satisfy the given partition-pruning predicate expressions.
   */
  def listPartitionsByFilter(
      tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    catalog.listPartitionsByFilter(tableName, predicates)
  }

  def listPartitions(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    catalog.listPartitions(tableName, partialSpec)
  }

  override def dropNamespace(
      namespace: Array[String],
      cascade: Boolean): Boolean =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Array(db) if catalog.databaseExists(db) =>
          catalog.dropDatabase(db, ignoreIfNotExists = false, cascade)
          true

        case Array(_) =>
          // exists returned false
          false

        case _ =>
          throw new NoSuchNamespaceException(namespace)
      }
    }

  // ///////////////////////////////////////////////////////////////////////////////////////////////
  //                                          ViewCatalog                                         //
  // ///////////////////////////////////////////////////////////////////////////////////////////////

  override def listViews(namespace: String*): Array[Identifier] =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      namespace match {
        case Seq(db) if catalog.databaseExists(db) =>
          catalog.listViews(db, "*")
            .map(ident =>
              Identifier.of(ident.database.map(Array(_)).getOrElse(Array()), ident.table))
            .toArray
        case _ =>
          throw new NoSuchNamespaceException(namespace.toArray)
      }
    }

  override def loadView(ident: Identifier): View =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      val catalogTable =
        try {
          catalog.getTableMetadata(ident.asTableIdentifier)
        } catch {
          case _: NoSuchTableException =>
            throw new NoSuchViewException(ident)
          case _: NoSuchDatabaseException =>
            throw new NoSuchViewException(ident)
        }
      if (catalogTable.tableType != CatalogTableType.VIEW) {
        throw new NoSuchViewException(ident)
      }
      HiveView(catalogTable)
    }

  override def viewExists(ident: Identifier): Boolean =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      try {
        loadView(ident) != null
      } catch {
        case _: NoSuchViewException => false
      }
    }

  override def createView(
      ident: Identifier,
      sql: String,
      currentCatalog: String,
      currentNamespace: Array[String],
      schema: StructType,
      queryColumnNames: Array[String],
      columnAliases: Array[String],
      columnComments: Array[String],
      properties: util.Map[String, String]): View =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      // 命名空间存在性检查
      ident.namespace match {
        case Array(db) if !catalog.databaseExists(db) =>
          throw new NoSuchNamespaceException(ident.namespace)
        case _ =>
      }
      // 按 ViewCatalog 接口约定，ident 已是表或视图都抛 ViewAlreadyExistsException
      if (tableExists(ident) || viewExists(ident)) {
        throw new ViewAlreadyExistsException(ident)
      }

      // 列别名 + 列注释，叠加到 schema 上
      val viewSchema = applyColumnAliasesAndComments(schema, columnAliases, columnComments)

      // viewCatalogAndNamespace / viewQueryColumnNames 通过 properties 中的特殊 key 存储
      val viewProps = encodeViewProperties(
        properties.asScala.toMap,
        currentCatalog,
        currentNamespace,
        queryColumnNames)

      val owner = Option(CurrentUserContext.CURRENT_USER.get()).getOrElse("")
      val now = System.currentTimeMillis()
      val viewTable = newCatalogTable(
        identifier = ident.asTableIdentifier,
        tableType = CatalogTableType.VIEW,
        storage = CatalogStorageFormat.empty,
        schema = viewSchema,
        provider = None,
        owner = owner,
        createTime = now,
        lastAccessTime = -1L,
        properties = viewProps,
        viewText = Some(sql),
        comment = Option(properties.get(ViewCatalog.PROP_COMMENT)),
        viewOriginalText = Some(sql))

      try {
        catalog.createTable(viewTable, ignoreIfExists = false)
      } catch {
        case _: TableAlreadyExistsException =>
          throw new ViewAlreadyExistsException(ident)
      }

      loadView(ident)
    }

  override def alterView(ident: Identifier, changes: ViewChange*): View =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      val catalogTable =
        try {
          catalog.getTableMetadata(ident.asTableIdentifier)
        } catch {
          case _: NoSuchTableException =>
            throw new NoSuchViewException(ident)
        }
      if (catalogTable.tableType != CatalogTableType.VIEW) {
        throw new NoSuchViewException(ident)
      }

      val newProperties = applyViewChanges(catalogTable.properties, changes)
      val newComment = newProperties.get(ViewCatalog.PROP_COMMENT)

      try {
        catalog.alterTable(catalogTable.copy(
          properties = newProperties,
          comment = newComment))
      } catch {
        case _: NoSuchTableException =>
          throw new NoSuchViewException(ident)
      }

      loadView(ident)
    }

  override def dropView(ident: Identifier): Boolean =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      try {
        val catalogTable = catalog.getTableMetadata(ident.asTableIdentifier)
        if (catalogTable.tableType != CatalogTableType.VIEW) {
          // 标识符指向的是表而不是视图，按 ViewCatalog 接口语义返回 false
          return false
        }
        catalog.dropTable(
          ident.asTableIdentifier,
          ignoreIfNotExists = true,
          purge = false)
        true
      } catch {
        case _: NoSuchTableException => false
        case _: NoSuchDatabaseException => false
      }
    }

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit =
    withSparkSQLConf(LEGACY_NON_IDENTIFIER_OUTPUT_CATALOG_NAME -> "true") {
      // 校验源必须是视图
      val sourceTable =
        try {
          catalog.getTableMetadata(oldIdent.asTableIdentifier)
        } catch {
          case _: NoSuchTableException =>
            throw new NoSuchViewException(oldIdent)
          case _: NoSuchDatabaseException =>
            throw new NoSuchViewException(oldIdent)
        }
      if (sourceTable.tableType != CatalogTableType.VIEW) {
        throw new NoSuchViewException(oldIdent)
      }
      // 校验目标不存在（无论是表还是视图）。按 ViewCatalog 接口约定，
      // 当 target 是表或视图时都应抛 ViewAlreadyExistsException。
      if (tableExists(newIdent) || viewExists(newIdent)) {
        throw new ViewAlreadyExistsException(newIdent)
      }
      catalog.renameTable(oldIdent.asTableIdentifier, newIdent.asTableIdentifier)
    }

  /**
   * 把 [[ViewChange]] 应用到 properties 上。
   *
   * 拒绝用户通过 [[ViewChange]] 写入或删除以下内部 properties，避免破坏视图元数据自洽性：
   *   - 以 `CatalogTable.VIEW_PREFIX`（即 `view.`）为前缀的内部 properties；
   *   - [[ViewCatalog.RESERVED_PROPERTIES]]（如 `comment`、`owner` 等保留属性）。
   */
  private def applyViewChanges(
      properties: Map[String, String],
      changes: Seq[ViewChange]): Map[String, String] = {
    val newProps = scala.collection.mutable.Map[String, String]() ++= properties
    changes.foreach {
      case set: ViewChange.SetProperty =>
        validateUserViewProperty(set.property)
        newProps += set.property -> set.value
      case remove: ViewChange.RemoveProperty =>
        validateUserViewProperty(remove.property)
        newProps -= remove.property
      case other =>
        throw new IllegalArgumentException(s"Unsupported view change: $other")
    }
    newProps.toMap
  }

  /**
   * 校验用户通过 [[ViewChange]] 修改的 property key 不属于内部保留范围。
   */
  private def validateUserViewProperty(key: String): Unit = {
    if (key.startsWith(CatalogTable.VIEW_PREFIX)) {
      throw new UnsupportedOperationException(
        s"Cannot modify reserved view property: $key (prefix '${CatalogTable.VIEW_PREFIX}' " +
          "is reserved by Spark for internal view metadata).")
    }
    if (ViewCatalog.RESERVED_PROPERTIES.contains(key)) {
      throw new UnsupportedOperationException(
        s"Cannot modify reserved view property: $key (defined in ViewCatalog.RESERVED_PROPERTIES).")
    }
  }

  /**
   * 把 columnAliases 和 columnComments 叠加到 schema 上。
   */
  private def applyColumnAliasesAndComments(
      schema: StructType,
      columnAliases: Array[String],
      columnComments: Array[String]): StructType = {
    val aliases = Option(columnAliases).getOrElse(Array.empty[String])
    val comments = Option(columnComments).getOrElse(Array.empty[String])
    val newFields = schema.fields.zipWithIndex.map { case (field, idx) =>
      val withName = if (idx < aliases.length && aliases(idx) != null) {
        field.copy(name = aliases(idx))
      } else {
        field
      }
      if (idx < comments.length && comments(idx) != null) {
        withName.withComment(comments(idx))
      } else {
        withName
      }
    }
    StructType(newFields)
  }

  /**
   * 把 currentCatalog/currentNamespace/queryColumnNames/SQL configs 编码到 properties 中。
   *
   * Spark 在 [[CatalogTable]] 中通过约定的 property key 来读取
   * `viewCatalogAndNamespace`、`viewQueryColumnNames`、`viewSQLConfigs`，参见
   * `CatalogTable.VIEW_CATALOG_AND_NAMESPACE` 等常量。
   *
   * 设计说明：
   *   - catalogAndNamespace 直接复用 [[CatalogTable.catalogAndNamespaceToProps]]，
   *     与 Spark 内部行为完全对齐；
   *   - sqlConfigs 借鉴 Spark `ViewHelper.sqlConfigsToProps` 实现，
   *     按相同的 allow/deny 规则捕获创建时的 SQL config（如 caseSensitive、
   *     SESSION_LOCAL_TIMEZONE 等），保证后续解析视图时语义稳定；
   *   - V2 [[ViewCatalog]] 接口未传 referredTempViewNames/Functions，
   *     V2 视图设计上不支持引用临时对象，因此不写入这两个 property，
   *     [[CatalogTable.viewReferredTempViewNames]] 的 getter 在缺省时返回空。
   */
  private def encodeViewProperties(
      userProps: Map[String, String],
      currentCatalog: String,
      currentNamespace: Array[String],
      queryColumnNames: Array[String]): Map[String, String] = {
    val builder = Map.newBuilder[String, String]
    // 用户自定义的 properties，过滤掉 ViewCatalog 保留属性以及 view 内部 properties 前缀
    userProps.foreach { case (k, v) =>
      if (!ViewCatalog.RESERVED_PROPERTIES.contains(k) && !k.startsWith(CatalogTable.VIEW_PREFIX)) {
        builder += k -> v
      }
    }

    // catalog + namespace：复用 Spark 官方实现，避免实现漂移
    val safeCatalog = Option(currentCatalog).getOrElse("")
    val safeNamespace = Option(currentNamespace).map(_.toSeq).getOrElse(Seq.empty)
    builder ++= CatalogTable.catalogAndNamespaceToProps(safeCatalog, safeNamespace)

    // query column names
    val cols = Option(queryColumnNames).getOrElse(Array.empty[String])
    builder += CatalogTable.VIEW_QUERY_OUTPUT_NUM_COLUMNS -> cols.length.toString
    cols.zipWithIndex.foreach { case (col, idx) =>
      builder += s"${CatalogTable.VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX}$idx" -> col
    }

    // 捕获创建时的 SQL configs，使后续解析视图时拿到与创建时一致的解析语义
    builder ++= captureViewSQLConfigs(conf)

    builder.result()
  }

  /**
   * 捕获当前 [[SQLConf]] 中需要持久化到视图 properties 中的配置项。
   *
   * 实现严格对齐 Spark `ViewHelper.sqlConfigsToProps`，包括：
   *   - 仅捕获用户已修改且通过 allow/deny 规则的 modifiable config；
   *   - 即便未修改也强制捕获 `SESSION_LOCAL_TIMEZONE`（其默认值依赖 JVM 时区，
   *     不固化会造成跨节点 / 跨时区行为漂移）。
   */
  private def captureViewSQLConfigs(conf: SQLConf): Map[String, String] = {
    val modifiedConfs = conf.getAllConfs.filter { case (k, _) =>
      conf.isModifiable(k) && shouldCaptureViewConfig(k)
    }
    val alwaysCaptured = Seq(SQLConf.SESSION_LOCAL_TIMEZONE)
      .filter(c => !modifiedConfs.contains(c.key))
      .map(c => (c.key, conf.getConf(c)))

    val props = Map.newBuilder[String, String]
    (modifiedConfs ++ alwaysCaptured).foreach { case (key, value) =>
      props += s"${CatalogTable.VIEW_SQL_CONFIG_PREFIX}$key" -> value
    }
    props.result()
  }

  /**
   * 是否应该把该 SQL config key 持久化到视图 properties 中，规则与 Spark
   * `ViewHelper.shouldCaptureConfig` 保持一致：
   *   1. 命中 [[VIEW_CONFIG_ALLOW_LIST]]，无条件捕获；
   *   2. 否则，只要不命中 [[VIEW_CONFIG_PREFIX_DENY_LIST]] 中的任意前缀就捕获。
   */
  private def shouldCaptureViewConfig(key: String): Boolean = {
    HiveTableCatalog.VIEW_CONFIG_ALLOW_LIST.contains(key) ||
    !HiveTableCatalog.VIEW_CONFIG_PREFIX_DENY_LIST.exists(key.startsWith)
  }
}

private object HiveTableCatalog extends Logging {
  private val HIVE_SERDE = "hive.serde"
  private val HIVE_STORED_AS = "hive.stored-as"
  private val HIVE_OUTPUT_FORMAT = "hive.output-format"
  private val HIVE_INPUT_FORMAT = "hive.input-format"

  private val HIVE_TABLE_RESERVED_SERDE_PROPERTIES = Set(
    HIVE_SERDE,
    HIVE_STORED_AS,
    HIVE_OUTPUT_FORMAT,
    HIVE_INPUT_FORMAT)

  /**
   * View 持久化 SQL config 时使用的前缀拒绝列表，与 Spark 内部
   * `ViewHelper.configPrefixDenyList` 保持一致：
   *   - 优化器/codegen/执行/shuffle/AQE 等运行期参数与视图语义无关；
   *   - Hive metastore 转换相关配置不应固化到视图中；
   *   - `MAX_NESTED_VIEW_DEPTH` 与视图嵌套保护相关，不固化。
   */
  private[hive] val VIEW_CONFIG_PREFIX_DENY_LIST: Seq[String] = Seq(
    SQLConf.MAX_NESTED_VIEW_DEPTH.key,
    "spark.sql.optimizer.",
    "spark.sql.codegen.",
    "spark.sql.execution.",
    "spark.sql.shuffle.",
    "spark.sql.adaptive.",
    "spark.sql.hive.convertMetastoreParquet",
    "spark.sql.hive.convertMetastoreOrc",
    "spark.sql.hive.convertInsertingPartitionedTable",
    "spark.sql.hive.convertMetastoreCtas")

  /**
   * View 持久化 SQL config 时无条件保留的白名单，与 Spark `ViewHelper.configAllowList` 一致。
   */
  private[hive] val VIEW_CONFIG_ALLOW_LIST: Set[String] = Set(
    SQLConf.DISABLE_HINTS.key)

  private def toCatalogDatabase(
      db: String,
      metadata: util.Map[String, String],
      defaultLocation: Option[URI] = None): CatalogDatabase = {
    CatalogDatabase(
      name = db,
      description = metadata.getOrDefault(SupportsNamespaces.PROP_COMMENT, ""),
      locationUri = Option(metadata.get(SupportsNamespaces.PROP_LOCATION))
        .map(CatalogUtils.stringToURI)
        .orElse(defaultLocation)
        .getOrElse(throw new IllegalArgumentException("Missing database location")),
      properties = metadata.asScala.toMap --
        Seq(SupportsNamespaces.PROP_COMMENT, SupportsNamespaces.PROP_LOCATION))
  }

  private def getStorageFormatAndProvider(
      provider: Option[String],
      location: Option[String],
      allProps: Map[String, String],
      optionsProps: Map[String, String],
      serdeProps: Map[String, String]): (CatalogStorageFormat, String) = {
    val nonHiveStorageFormat = CatalogStorageFormat.empty.copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = optionsProps)

    val conf = SQLConf.get
    val defaultHiveStorage = HiveSerDe.getDefaultStorage(conf).copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = optionsProps)

    if (provider.isDefined) {
      (nonHiveStorageFormat, provider.get)
    } else if (serdeIsDefined(allProps)) {
      val maybeSerde = allProps.get(HIVE_SERDE)
      val maybeStoredAs = allProps.get(HIVE_STORED_AS)
      val maybeInputFormat = allProps.get(HIVE_INPUT_FORMAT)
      val maybeOutputFormat = allProps.get(HIVE_OUTPUT_FORMAT)
      val storageFormat = if (maybeStoredAs.isDefined) {
        // If `STORED AS fileFormat` is used, infer inputFormat, outputFormat and serde from it.
        HiveSerDe.sourceToSerDe(maybeStoredAs.get) match {
          case Some(hiveSerde) =>
            defaultHiveStorage.copy(
              inputFormat = hiveSerde.inputFormat.orElse(defaultHiveStorage.inputFormat),
              outputFormat = hiveSerde.outputFormat.orElse(defaultHiveStorage.outputFormat),
              // User specified serde takes precedence over the one inferred from file format.
              serde = maybeSerde.orElse(hiveSerde.serde).orElse(defaultHiveStorage.serde),
              properties = serdeProps ++ defaultHiveStorage.properties)
          case _ => throw KyuubiHiveConnectorException(s"Unsupported serde ${maybeSerde.get}.")
        }
      } else {
        defaultHiveStorage.copy(
          inputFormat =
            maybeInputFormat.orElse(defaultHiveStorage.inputFormat),
          outputFormat =
            maybeOutputFormat.orElse(defaultHiveStorage.outputFormat),
          serde = maybeSerde.orElse(defaultHiveStorage.serde),
          properties = serdeProps ++ defaultHiveStorage.properties)
      }
      (storageFormat, DDLUtils.HIVE_PROVIDER)
    } else {
      val createHiveTableByDefault = conf.getConf(SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT)
      if (!createHiveTableByDefault) {
        (nonHiveStorageFormat, conf.defaultDataSourceName)
      } else {
        logWarning("A Hive serde table will be created as there is no table provider " +
          s"specified. You can set ${SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT.key} to false " +
          "so that native data source table will be created instead.")
        (defaultHiveStorage, DDLUtils.HIVE_PROVIDER)
      }
    }
  }

  private def serdeIsDefined(options: Map[String, String]): Boolean = {
    val maybeStoredAs = options.get(HIVE_STORED_AS)
    val maybeInputFormat = options.get(HIVE_INPUT_FORMAT)
    val maybeOutputFormat = options.get(HIVE_OUTPUT_FORMAT)
    val maybeSerde = options.get(HIVE_SERDE)
    maybeStoredAs.isDefined || maybeInputFormat.isDefined ||
    maybeOutputFormat.isDefined || maybeSerde.isDefined
  }

  implicit class NamespaceHelper(namespace: Array[String]) {
    def quoted: String = namespace.map(quoteIfNeeded).mkString(".")
  }

  implicit class IdentifierHelper(ident: Identifier) {
    def quoted: String = {
      if (ident.namespace.nonEmpty) {
        ident.namespace.map(quoteIfNeeded).mkString(".") + "." + quoteIfNeeded(ident.name)
      } else {
        quoteIfNeeded(ident.name)
      }
    }

    def asMultipartIdentifier: Seq[String] = ident.namespace :+ ident.name

    def asTableIdentifier: TableIdentifier = ident.namespace match {
      case ns if ns.isEmpty => TableIdentifier(ident.name)
      case Array(dbName) => TableIdentifier(ident.name, Some(dbName))
      case _ =>
        throw KyuubiHiveConnectorException(
          s"$quoted is not a valid TableIdentifier as it has more than 2 name parts.")
    }
  }

  implicit class CatalogDatabaseHelper(catalogDatabase: CatalogDatabase) {
    def toMetadata: util.Map[String, String] = {
      val metadata = mutable.HashMap[String, String]()

      catalogDatabase.properties.foreach {
        case (key, value) => metadata.put(key, value)
      }
      metadata.put(SupportsNamespaces.PROP_LOCATION, catalogDatabase.locationUri.toString)
      metadata.put(SupportsNamespaces.PROP_COMMENT, catalogDatabase.description)

      metadata.asJava
    }
  }
}
