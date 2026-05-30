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

import java.io.IOException

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, RowOrdering, SortOrder, SortPrefix, UnsafeProjection}
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.execution.{SortPrefixUtils, UnsafeExternalRowSorter}
import org.apache.spark.sql.execution.datasources.{DynamicPartitionDataSingleWriter, WriteJobDescription}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * A [[DataWriter]] that sorts every row a Spark task receives by `(partitionColumns, bucketId,
 * bucketSortColumns)` using the same external (memory + disk) sorter that Spark's V1 writer
 * employs, and then forwards the sorted rows to an underlying
 * [[DynamicPartitionDataSingleWriter]].
 *
 * Why we need this:
 *   - [[DynamicPartitionDataSingleWriter]] expects its input to be already sorted by
 *     `(partitionValues, bucketId)`. When the input is unsorted, the writer keeps closing and
 *     re-opening files for the same `(partition, bucketId)` combination, and because
 *     `fileCounter` is reset to 0 for each new combination, those re-opens reuse the same file
 *     name, which raises `FileAlreadyExistsException` on HDFS.
 *   - Spark V1 (`InsertIntoHiveTable` / `FileFormatWriter`) solves this by injecting a
 *     `SortExec` over `bucketIdExpression` in the optimizer (`V1Writes`).
 *   - The V2 connector path cannot express that catalyst-only sort key through
 *     [[org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering#requiredOrdering]],
 *     so we replicate Spark's V1 sort here, at the task level.
 *
 * The implementation mirrors [[org.apache.spark.sql.execution.SortExec#createSorter]] so that we
 * inherit the standard memory accounting, spill-to-disk semantics and prefix-comparator
 * optimisations.
 */
class BucketSortingDataWriter(
    description: WriteJobDescription,
    sortOrder: Seq[SortOrder],
    underlying: DynamicPartitionDataSingleWriter) extends DataWriter[InternalRow] {

  require(
    sortOrder.nonEmpty,
    "BucketSortingDataWriter must be given a non-empty sort order")

  // Schema fed into the sorter is `allColumns` because we sort the whole row and forward it
  // unchanged to the underlying writer.
  private val sortInputSchema: StructType =
    StructType(description.allColumns.map(a =>
      StructField(a.name, a.dataType, a.nullable, a.metadata)))

  private val sorter: UnsafeExternalRowSorter = createSorter()

  // Project the input row to an `UnsafeRow` of `allColumns`, the sorter's input format.
  private val toUnsafe: UnsafeProjection =
    UnsafeProjection.create(description.allColumns, description.allColumns)

  override def write(record: InternalRow): Unit = {
    sorter.insertRow(toUnsafe(record))
  }

  override def commit(): WriterCommitMessage = {
    val it = sorter.sort()
    while (it.hasNext) {
      underlying.write(it.next())
    }
    underlying.commit()
  }

  override def abort(): Unit = {
    try sorter.cleanupResources()
    finally underlying.abort()
  }

  override def close(): Unit = {
    try sorter.cleanupResources()
    finally underlying.close()
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = underlying.currentMetricsValues()

  /**
   * Build an [[UnsafeExternalRowSorter]] in the exact shape used by
   * [[org.apache.spark.sql.execution.SortExec#createSorter]] so that memory accounting, spill,
   * radix sort and prefix-comparator optimisations are all preserved.
   */
  @throws[IOException]
  private def createSorter(): UnsafeExternalRowSorter = {
    val output: Seq[Attribute] = description.allColumns
    val ordering = RowOrdering.create(sortOrder, output)
    val boundSortExpression = BindReferences.bindReference(sortOrder.head, output)
    val prefixComparator = SortPrefixUtils.getPrefixComparator(boundSortExpression)

    val canUseRadixSort = sortOrder.length == 1 &&
      SortPrefixUtils.canSortFullyWithPrefix(boundSortExpression)

    val prefixExpr = SortPrefix(boundSortExpression)
    val prefixProjection = UnsafeProjection.create(Seq(prefixExpr))
    val prefixComputer = new UnsafeExternalRowSorter.PrefixComputer {
      private val result = new UnsafeExternalRowSorter.PrefixComputer.Prefix
      override def computePrefix(
          row: InternalRow): UnsafeExternalRowSorter.PrefixComputer.Prefix = {
        val prefix = prefixProjection.apply(row)
        result.isNull = prefix.isNullAt(0)
        result.value = if (result.isNull) prefixExpr.nullValue else prefix.getLong(0)
        result
      }
    }

    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    UnsafeExternalRowSorter.create(
      sortInputSchema,
      ordering,
      prefixComparator,
      prefixComputer,
      pageSize,
      canUseRadixSort)
  }
}
