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

import java.util.Date

import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapreduce.{TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.execution.datasources.{DynamicPartitionDataSingleWriter, SingleDirectoryDataWriter, WriteJobDescription}
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.SparkHadoopWriterUtils

/**
 * Forked from Spark's `org.apache.spark.sql.execution.datasources.v2.FileWriterFactory`.
 *
 * Differences from the upstream factory:
 *   - Carries the SPARK-42478 fix needed by Spark 3.3.2 (persist jobTrackerID, recreate jobId
 *     lazily on each executor).
 *   - When the target table is bucketed (`description.bucketSpec.isDefined`), routes the write
 *     to [[DynamicPartitionDataSingleWriter]] even if there are no partition columns. The
 *     upstream factory only chooses [[DynamicPartitionDataSingleWriter]] for partitioned
 *     tables, which loses the bucket layout for non-partitioned bucketed tables.
 *
 * The input must already be sorted by `(partitionColumns, bucketIdExpression, sortColumns)`
 * before reaching this factory; that ordering is established by
 * [[HiveWrite#requiredOrdering]] which expresses the bucket-id sort key as a connector named
 * transform that resolves to `Pmod(BitwiseAnd(HiveHash(...)))` through
 * [[org.apache.kyuubi.spark.connector.hive.HiveTableCatalog]]'s `FunctionCatalog` mixin.
 */
case class FileWriterFactory(
    description: WriteJobDescription,
    committer: FileCommitProtocol) extends DataWriterFactory {

  // SPARK-42478: jobId across tasks should be consistent to meet the contract
  // expected by Hadoop committers, but `JobId` cannot be serialized.
  // thus, persist the serializable jobTrackerID in the class and make jobId a
  // transient lazy val which recreates it each time to ensure jobId is unique.
  private[this] val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date)
  @transient private lazy val jobId = createJobID(jobTrackerID, 0)

  override def createWriter(partitionId: Int, realTaskId: Long): DataWriter[InternalRow] = {
    val taskAttemptContext = createTaskAttemptContext(partitionId, realTaskId.toInt & Int.MaxValue)
    committer.setupTask(taskAttemptContext)
    if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
      new SingleDirectoryDataWriter(description, taskAttemptContext, committer)
    } else {
      new DynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
    }
  }

  private def createTaskAttemptContext(
      partitionId: Int,
      realTaskId: Int): TaskAttemptContextImpl = {
    val taskId = new TaskID(jobId, TaskType.MAP, partitionId)
    val taskAttemptId = new TaskAttemptID(taskId, realTaskId)
    // Set up the configuration object
    val hadoopConf = description.serializableHadoopConf.value
    hadoopConf.set("mapreduce.job.id", jobId.toString)
    hadoopConf.set("mapreduce.task.id", taskId.toString)
    hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    hadoopConf.setBoolean("mapreduce.task.ismap", true)
    hadoopConf.setInt("mapreduce.task.partition", 0)

    new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
  }

  /**
   * Create a job ID.
   *
   * @param jobTrackerID unique job track id
   * @param id job number
   * @return a job ID
   */
  def createJobID(jobTrackerID: String, id: Int): JobID = {
    if (id < 0) {
      throw new IllegalArgumentException("Job number is negative")
    }
    new JobID(jobTrackerID, id)
  }
}
