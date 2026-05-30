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

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.sources.Filter

import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog

case class HiveWriteBuilder(
    sparkSession: SparkSession,
    catalogTable: CatalogTable,
    info: LogicalWriteInfo,
    hiveTableCatalog: HiveTableCatalog) extends WriteBuilder with SupportsOverwrite
  with SupportsDynamicOverwrite {

  private var forceOverwrite = false
  private val parts = catalogTable.partitionColumnNames
  private val bucketSpec = catalogTable.bucketSpec

  override def build(): Write = {
    HiveWrite(
      sparkSession,
      catalogTable,
      info,
      hiveTableCatalog,
      forceOverwrite,
      dynamicPartitionSpec(),
      writeOptions())
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    forceOverwrite = true
    this
  }

  override def overwriteDynamicPartitions(): WriteBuilder = {
    forceOverwrite = true
    this
  }

  private def dynamicPartitionSpec(): Map[String, Option[String]] = {
    var partSpec = Map.empty[String, Option[String]]
    parts.foreach(p => partSpec = partSpec.updated(p, None))
    partSpec
  }

  /**
   * Build the write options. For Hive bucketed tables we tag the options with
   * [[HiveWriteBuilder.HIVE_COMPATIBLE_BUCKET_WRITE_OPTION]] = `true` so that downstream writers
   * compute the bucket id with [[org.apache.spark.sql.catalyst.expressions.HiveHash]], matching
   * the convention shared by Hive, Presto, Trino and Spark's native Hive writer.
   */
  private def writeOptions(): Map[String, String] = {
    val opts = info.options().asScala.toMap
    bucketSpec match {
      case Some(_) => opts + (HiveWriteBuilder.HIVE_COMPATIBLE_BUCKET_WRITE_OPTION -> "true")
      case None => opts
    }
  }
}

object HiveWriteBuilder {

  /**
   * The reserved option name that triggers Hive-compatible bucket writes; mirrors
   * `org.apache.spark.sql.execution.datasources.BucketingUtils#optionForHiveCompatibleBucketWrite`.
   * We re-declare it here to avoid leaking a private Spark internal symbol via our shaded
   * `HiveBridgeHelper` and to keep the surface area of the connector self-contained.
   */
  val HIVE_COMPATIBLE_BUCKET_WRITE_OPTION: String = "__hive_compatible_bucketed_table_insertion__"
}
