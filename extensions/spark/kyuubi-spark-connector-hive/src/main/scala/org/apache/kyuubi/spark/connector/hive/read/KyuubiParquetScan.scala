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

package org.apache.kyuubi.spark.connector.hive.read

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, SupportsRuntimeFiltering}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.kyuubi.util.reflect.{DynClasses, DynConstructors}

/**
 * A DPP-aware wrapper around Spark's built-in [[ParquetScan]] that adds
 * [[SupportsRuntimeFiltering]] so Dynamic Partition Pruning can push runtime
 * IN predicates down to the Hive partitioned scan.
 *
 * Implementation notes:
 * 1. Only DPP-specific methods ([[filter]] / [[filterAttributes]] /
 *    [[planInputPartitions]]) contain custom logic, all other methods
 *    delegate to the wrapped [[ParquetScan]].
 * 2. [[equals]] / [[hashCode]] are overridden to key on `getClass`, so a
 *    `KyuubiParquetScan` is never reused in place of a plain [[ParquetScan]]
 *    during exchange/subquery reuse.
 */
class KyuubiParquetScan(
    val sparkSession: SparkSession,
    val hadoopConf: Configuration,
    val fileIndex: PartitioningAwareFileIndex,
    val dataSchema: StructType,
    val readDataSchema: StructType,
    val readPartitionSchema: StructType,
    val pushedFilters: Array[Filter],
    val options: CaseInsensitiveStringMap,
    val pushedAggregate: Option[Aggregation],
    val partitionFilters: Seq[Expression],
    val dataFilters: Seq[Expression],
    val catalogTable: CatalogTable)
  extends FileScan
  with SupportsRuntimeFiltering
  with KyuubiParquetColumnarMixin {

  private[hive] val inner: ParquetScan = KyuubiParquetScan.newParquetScan(
    sparkSession,
    hadoopConf,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    pushedFilters,
    options,
    pushedAggregate,
    partitionFilters,
    dataFilters)

  private var runtimeFilters: Seq[Expression] = Seq.empty

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  override def filterAttributes(): Array[NamedReference] = {
    HiveRuntimeFilterSupport.filterAttributes(readPartitionSchema.fieldNames.toSeq)
  }

  override def filter(filters: Array[Filter]): Unit = {
    runtimeFilters = HiveRuntimeFilterSupport.toCatalystPartitionFilters(
      filters,
      fileIndex.partitionSchema,
      isCaseSensitive)
    if (runtimeFilters.nonEmpty) {
      logInfo(s"Received ${runtimeFilters.length} runtime partition filter(s) for " +
        s"${catalogTable.identifier}")
      logDebug(s"Runtime partition filter(s) for ${catalogTable.identifier}: " +
        s"${runtimeFilters.mkString(", ")}")
    }
  }

  override def planInputPartitions(): Array[InputPartition] = {
    if (runtimeFilters.isEmpty) {
      inner.planInputPartitions()
    } else {
      // Delegate planning to a sibling ParquetScan carrying the merged
      // partitionFilters ++ runtimeFilters so DPP predicates take effect.
      val sibling = KyuubiParquetScan.newParquetScan(
        sparkSession,
        hadoopConf,
        fileIndex,
        dataSchema,
        readDataSchema,
        readPartitionSchema,
        pushedFilters,
        options,
        pushedAggregate,
        partitionFilters ++ runtimeFilters,
        dataFilters)
      sibling.planInputPartitions()
    }
  }

  override def isSplitable(path: Path): Boolean = inner.isSplitable(path)

  override def readSchema(): StructType = inner.readSchema()

  override def getMetaData(): Map[String, String] = inner.getMetaData()

  override def createReaderFactory(): PartitionReaderFactory = inner.createReaderFactory()

  override def equals(obj: Any): Boolean = obj match {
    case that: KyuubiParquetScan => this.inner.equals(that.inner)
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()
}

object KyuubiParquetScan {

  // Element type of `Array[VariantExtraction]` in Spark 4.1+, null when absent.
  private lazy val variantExtractionCls: Class[_] = DynClasses.builder()
    .impl("org.apache.spark.sql.connector.read.VariantExtraction")
    .orNull()
    .build()

  private lazy val emptyVariantExtractions: AnyRef =
    if (variantExtractionCls == null) null
    else java.lang.reflect.Array.newInstance(variantExtractionCls, 0).asInstanceOf[AnyRef]

  // scalastyle:off parameter.number
  private[hive] def newParquetScan(
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      fileIndex: PartitioningAwareFileIndex,
      dataSchema: StructType,
      readDataSchema: StructType,
      readPartitionSchema: StructType,
      pushedFilters: Array[Filter],
      options: CaseInsensitiveStringMap,
      pushedAggregate: Option[Aggregation],
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): ParquetScan = {
    if (variantExtractionCls != null) {
      // Spark 4.1+
      DynConstructors.builder()
        .impl(
          classOf[ParquetScan],
          classOf[SparkSession],
          classOf[Configuration],
          classOf[PartitioningAwareFileIndex],
          classOf[StructType],
          classOf[StructType],
          classOf[StructType],
          classOf[Array[Filter]],
          classOf[CaseInsensitiveStringMap],
          classOf[Option[Aggregation]],
          classOf[Seq[Expression]],
          classOf[Seq[Expression]],
          emptyVariantExtractions.getClass)
        .buildChecked()
        .invokeChecked[ParquetScan](
          null,
          sparkSession,
          hadoopConf,
          fileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          pushedAggregate,
          partitionFilters,
          dataFilters,
          emptyVariantExtractions)
    } else {
      // Spark 4.0 and previous
      DynConstructors.builder()
        .impl(
          classOf[ParquetScan],
          classOf[SparkSession],
          classOf[Configuration],
          classOf[PartitioningAwareFileIndex],
          classOf[StructType],
          classOf[StructType],
          classOf[StructType],
          classOf[Array[Filter]],
          classOf[CaseInsensitiveStringMap],
          classOf[Option[Aggregation]],
          classOf[Seq[Expression]],
          classOf[Seq[Expression]])
        .buildChecked()
        .invokeChecked[ParquetScan](
          null,
          sparkSession,
          hadoopConf,
          fileIndex,
          dataSchema,
          readDataSchema,
          readPartitionSchema,
          pushedFilters,
          options,
          pushedAggregate,
          partitionFilters,
          dataFilters)
    }
  }
  // scalastyle:on parameter.number
}
