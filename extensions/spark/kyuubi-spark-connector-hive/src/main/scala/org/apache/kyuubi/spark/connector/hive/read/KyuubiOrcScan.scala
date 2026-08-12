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
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A DPP-aware wrapper around Spark's built-in [[OrcScan]] that adds
 * [[SupportsRuntimeFiltering]] so Dynamic Partition Pruning can push runtime
 * IN predicates down to the Hive partitioned scan.
 *
 * Implementation notes:
 * 1. Only DPP-specific methods ([[filter]] / [[filterAttributes]] /
 *    [[planInputPartitions]]) contain custom logic, all other methods
 *    delegate to the wrapped [[OrcScan]].
 * 2. [[equals]] / [[hashCode]] are overridden to key on `getClass`, so a
 *    `KyuubiOrcScan` is never reused in place of a plain [[OrcScan]]
 *    during exchange/subquery reuse.
 */
class KyuubiOrcScan(
    val sparkSession: SparkSession,
    val hadoopConf: Configuration,
    val fileIndex: PartitioningAwareFileIndex,
    val dataSchema: StructType,
    val readDataSchema: StructType,
    val readPartitionSchema: StructType,
    val options: CaseInsensitiveStringMap,
    val pushedAggregate: Option[Aggregation],
    val pushedFilters: Array[Filter],
    val partitionFilters: Seq[Expression],
    val dataFilters: Seq[Expression],
    val catalogTable: CatalogTable)
  extends FileScan
  with SupportsRuntimeFiltering
  with KyuubiOrcColumnarMixin {

  private[hive] val inner: OrcScan = OrcScan(
    sparkSession,
    hadoopConf,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    options,
    pushedAggregate,
    pushedFilters,
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
      // Delegate planning to a sibling OrcScan carrying the merged
      // partitionFilters ++ runtimeFilters so DPP predicates take effect.
      val sibling = OrcScan(
        sparkSession,
        hadoopConf,
        fileIndex,
        dataSchema,
        readDataSchema,
        readPartitionSchema,
        options,
        pushedAggregate,
        pushedFilters,
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
    case that: KyuubiOrcScan => this.inner.equals(that.inner)
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()
}
