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

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.orc.OrcUtils
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan

/**
 * Spark 3.5+ specialisation that overrides [[Scan.columnarSupportMode]] so
 * `DataSourceV2ScanExecBase.supportsColumnar` does NOT have to iterate
 * `inputPartitions` to probe each partition. With `PARTITION_DEFINED`
 * (the default), Spark would touch `inputPartitions` during planning, which
 * through `FileScan.partitions` -> `fileIndex.listFiles` triggers an eager
 * full-table HDFS listing BEFORE DPP [[SupportsRuntimeFiltering.filter]]
 * has been invoked.
 *
 * `Scan.ColumnarSupportMode` is @since 3.5.0 (SPARK-44505), a no-op version of
 * this trait is provided in `src/main/scala-spark-pre-3.5/` for Spark 3.3 / 3.4.
 *
 * The decision returned here is semantically identical to Spark's
 * `OrcPartitionReaderFactory.supportColumnarReads` evaluated against
 * every partition, so advertising it at scan-level is safe.
 */
trait KyuubiOrcColumnarMixin { this: OrcScan =>

  override def columnarSupportMode(): Scan.ColumnarSupportMode = {
    val sqlConf = sparkSession.sessionState.conf
    val schema = readSchema()
    val columnar = sqlConf.orcVectorizedReaderEnabled &&
      sqlConf.wholeStageEnabled &&
      !WholeStageCodegenExec.isTooManyFields(sqlConf, schema) &&
      schema.forall(s =>
        OrcUtils.supportColumnarReads(
          s.dataType,
          sqlConf.orcVectorizedReaderNestedColumnEnabled))
    if (columnar) Scan.ColumnarSupportMode.SUPPORTED
    else Scan.ColumnarSupportMode.UNSUPPORTED
  }
}
