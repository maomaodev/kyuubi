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

import java.util.{Locale, UUID}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.plan.{FileSinkDesc, TableDesc}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, BitwiseAnd, HiveHash, Literal, Pmod}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.connector.distributions.{Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, SortDirection, SortOrder}
import org.apache.spark.sql.connector.write.{BatchWrite, LogicalWriteInfo, RequiresDistributionAndOrdering}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, WriteJobDescription}
import org.apache.spark.sql.execution.datasources.v2.FileBatchWrite
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.HiveOptions
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.{HiveClientImpl, StructTypeHelper, WriterBucketSpec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import org.apache.kyuubi.spark.connector.hive.{HiveBucketUnboundFunction, HiveTableCatalog}
import org.apache.kyuubi.spark.connector.hive.HiveConnectorUtils.getHiveFileFormat

case class HiveWrite(
    sparkSession: SparkSession,
    table: CatalogTable,
    info: LogicalWriteInfo,
    hiveTableCatalog: HiveTableCatalog,
    forceOverwrite: Boolean,
    dynamicPartition: Map[String, Option[String]],
    writeOptions: Map[String, String])
  extends RequiresDistributionAndOrdering with Logging {

  private val options = info.options()

  private val hiveTable = HiveClientImpl.toHiveTable(table)

  private val hadoopConf = hiveTableCatalog.hadoopConfiguration()

  private val externalCatalog = hiveTableCatalog.externalCatalog

  private val tableLocation = hiveTable.getDataLocation

  private val allColumns = info.schema().toAttributes
  private val dataColumns = allColumns.take(allColumns.length - hiveTable.getPartCols.size())
  private val partColumns = allColumns.takeRight(hiveTable.getPartCols.size())

  private val bucketSpec: Option[BucketSpec] = table.bucketSpec

  lazy val tableDesc: TableDesc = new TableDesc(
    hiveTable.getInputFormatClass,
    hiveTable.getOutputFormatClass,
    hiveTable.getMetadata)

  override def description(): String = "Kyuubi-Hive-Connector"

  /**
   * For bucketed tables, request a clustered distribution on the bucket columns and a fixed
   * shuffle partition count equal to `numBuckets`. This ensures that all rows belonging to the
   * same Hive bucket id are co-located in the same task before being handed to
   * [[org.apache.spark.sql.execution.datasources.DynamicPartitionDataSingleWriter]], which is the
   * writer that actually splits the per-bucket files. The hash function used by Spark for
   * shuffling differs from `HiveHash`, but co-location is sufficient because the writer always
   * recomputes the bucket id with `HiveHash` and rows are sorted by bucket id within each task
   * (see [[requiredOrdering]]).
   */
  override def requiredDistribution(): Distribution = bucketSpec match {
    case Some(spec) =>
      val clustering: Array[Expression] =
        spec.bucketColumnNames.map(c => Expressions.column(c).asInstanceOf[Expression]).toArray
      Distributions.clustered(clustering)
    case None => Distributions.unspecified()
  }

  override def requiredNumPartitions(): Int = bucketSpec.map(_.numBuckets).getOrElse(0)

  /**
   * Build the per-task sort order required by Spark's [[DynamicPartitionDataSingleWriter]]:
   * `(dynamic partition columns, bucketId, bucketSortColumns)` ASC.
   *
   * The Hive bucket-id expression `Pmod(BitwiseAnd(HiveHash(cols), Int.MaxValue), numBuckets)`
   * has no public V2 connector representation, so we expose it as a connector-owned scalar
   * function `hive_bucket(numBuckets, col1, ...)` (resolved by [[HiveTableCatalog]] which mixes
   * in [[org.apache.spark.sql.connector.catalog.FunctionCatalog]]). Spark rewrites the named
   * transform into a catalyst `Pmod(BitwiseAnd(HiveHash(...)))` via
   * `V2ExpressionUtils#toCatalystTransformOpt` and feeds it into the standard `Sort` operator.
   *
   * This matches the semantics of Spark V1's `V1WritesUtils#getSortOrder` and removes the need
   * for a custom task-level sorting writer — Spark's stock spillable `SortExec` does the work.
   */
  override def requiredOrdering(): Array[SortOrder] = bucketSpec match {
    case Some(spec) =>
      val partitionOrdering: Seq[SortOrder] = partColumns.map { col =>
        Expressions.sort(Expressions.column(col.name), SortDirection.ASCENDING)
      }

      val numBucketsLit: Expression = Expressions.literal[Integer](spec.numBuckets)
      val bucketColumnExprs: Seq[Expression] =
        spec.bucketColumnNames.map(c => Expressions.column(c).asInstanceOf[Expression])
      val bucketIdArgs: Array[Expression] = (numBucketsLit +: bucketColumnExprs).toArray
      val bucketIdTransform = Expressions.apply(HiveBucketUnboundFunction.NAME, bucketIdArgs: _*)
      val bucketIdOrdering = Expressions.sort(bucketIdTransform, SortDirection.ASCENDING)

      val sortColumnOrdering: Seq[SortOrder] = spec.sortColumnNames.map { col =>
        Expressions.sort(Expressions.column(col), SortDirection.ASCENDING)
      }

      (partitionOrdering ++ Seq(bucketIdOrdering) ++ sortColumnOrdering).toArray

    case None =>
      partColumns.map { col =>
        Expressions.sort(Expressions.column(col.name), SortDirection.ASCENDING)
      }.toArray
  }

  override def toBatch: BatchWrite = {
    val tmpLocation = HiveWriteHelper.getExternalTmpPath(externalCatalog, hadoopConf, tableLocation)

    val fileSinkConf = new FileSinkDesc(tmpLocation, tableDesc, false)
    handleCompression(fileSinkConf, hadoopConf)

    val committer = FileCommitProtocol.instantiate(
      className = sparkSession.sessionState.conf.fileCommitProtocolClass,
      jobId = java.util.UUID.randomUUID().toString,
      outputPath = tmpLocation.toString)

    val job = getJobInstance(hadoopConf, tmpLocation)

    val description = createWriteJobDescription(
      fileSinkConf,
      sparkSession,
      hadoopConf,
      job,
      tmpLocation.toString,
      Map.empty,
      writeOptions ++ options.asScala.toMap)

    committer.setupJob(job)

    new HiveBatchWrite(
      sparkSession,
      table,
      hiveTableCatalog,
      Some(tmpLocation),
      dynamicPartition,
      forceOverwrite,
      hadoopConf,
      new FileBatchWrite(job, description, committer),
      externalCatalog,
      description,
      committer)
  }

  private def createWriteJobDescription(
      fileSinkConf: FileSinkDesc,
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      job: Job,
      pathName: String,
      customPartitionLocations: Map[TablePartitionSpec, String],
      options: Map[String, String]): WriteJobDescription = {
    val hiveFileFormat = getHiveFileFormat(fileSinkConf)
    val dataSchema = StructType(info.schema().fields.take(dataColumns.length))
    val outputWriterFactory = hiveFileFormat.prepareWrite(sparkSession, job, options, dataSchema)
    val metrics: Map[String, SQLMetric] = BasicWriteJobStatsTracker.metrics
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    val statsTracker = new BasicWriteJobStatsTracker(serializableHadoopConf, metrics)

    new WriteJobDescription(
      uuid = UUID.randomUUID().toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = allColumns,
      dataColumns = dataColumns,
      partitionColumns = partColumns,
      bucketSpec = writerBucketSpecOf(options),
      path = pathName,
      customPartitionLocations = customPartitionLocations,
      maxRecordsPerFile = sparkSession.sessionState.conf.maxRecordsPerFile,
      timeZoneId = sparkSession.sessionState.conf.sessionLocalTimeZone,
      statsTrackers = Seq(statsTracker))
  }

  /**
   * Build the [[WriterBucketSpec]] consumed by Spark's
   * [[org.apache.spark.sql.execution.datasources.FileFormatDataWriter]].
   *
   * Mirrors Spark V1's `V1WritesUtils#getWriterBucketSpec`. When the
   * `__hive_compatible_bucketed_table_insertion__` option is set (always set by
   * [[HiveWriteBuilder]] for Hive bucketed tables), the bucket id expression follows the Hive
   * convention `Pmod(BitwiseAnd(HiveHash(bucketColumns), Int.MaxValue), numBuckets)` so the
   * resulting files can be read by Hive, Trino and Presto.
   */
  private def writerBucketSpecOf(options: Map[String, String]): Option[WriterBucketSpec] = {
    bucketSpec.map { spec =>
      val bucketColumns: Seq[Attribute] = spec.bucketColumnNames.map { name =>
        dataColumns.find(_.name == name).getOrElse {
          throw new IllegalArgumentException(
            s"Bucket column '$name' is not found in data columns: " +
              dataColumns.map(_.name).mkString(", "))
        }
      }

      if (options.getOrElse(
          HiveWriteBuilder.HIVE_COMPATIBLE_BUCKET_WRITE_OPTION,
          "false") == "true") {
        // Hive bucketed table: use `HiveHash` and bitwise-and as bucket id expression. Without
        // the extra bitwise-and, a negative hash produces a wrong bucket id; see Hive's
        // `ObjectInspectorUtils#getBucketNumber`.
        val hashId = BitwiseAnd(HiveHash(bucketColumns), Literal(Int.MaxValue))
        val bucketIdExpression = Pmod(hashId, Literal(spec.numBuckets))

        // The bucket file name prefix follows the Hive/Presto/Trino convention so that Hive
        // bucketed tables written here can be read by other SQL engines. References:
        //   Hive : `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile`
        //   Trino: `io.trino.plugin.hive.BackgroundHiveSplitLoader#BUCKET_PATTERNS`
        val fileNamePrefix = (bucketId: Int) => f"$bucketId%05d_0_"
        WriterBucketSpec(bucketIdExpression, fileNamePrefix)
      } else {
        // Spark bucketed table: use `HashPartitioning.partitionIdExpression` so the data
        // distribution is consistent with shuffle.
        val bucketIdExpression = HashPartitioning(bucketColumns, spec.numBuckets)
          .partitionIdExpression
        WriterBucketSpec(bucketIdExpression, (_: Int) => "")
      }
    }
  }

  private def getJobInstance(hadoopConf: Configuration, path: Path): Job = {
    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, path)
    job
  }

  private def handleCompression(fileSinkConf: FileSinkDesc, hadoopConf: Configuration): Unit = {
    val isCompressed =
      fileSinkConf.getTableInfo.getOutputFileFormatClassName.toLowerCase(Locale.ROOT) match {
        case formatName if formatName.endsWith("orcoutputformat") =>
          // For ORC,"mapreduce.output.fileoutputformat.compress",
          // "mapreduce.output.fileoutputformat.compress.codec", and
          // "mapreduce.output.fileoutputformat.compress.type"
          // have no impact because it uses table properties to store compression information.
          false
        case _ => hadoopConf.get("hive.exec.compress.output", "false").toBoolean
      }

    if (isCompressed) {
      hadoopConf.set("mapreduce.output.fileoutputformat.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(hadoopConf
        .get("mapreduce.output.fileoutputformat.compress.codec"))
      fileSinkConf.setCompressType(hadoopConf
        .get("mapreduce.output.fileoutputformat.compress.type"))
    } else {
      // Set compression by priority
      HiveOptions.getHiveWriteCompression(fileSinkConf.getTableInfo, sparkSession.sessionState.conf)
        .foreach { case (compression, codec) => hadoopConf.set(compression, codec) }
    }
  }
}
