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

import org.apache.spark.sql.catalyst.expressions.{BoundReference, Generator}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, UnboundFunction}
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.HiveSessionCatalog
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog.IdentifierHelper

/**
 * 把 V1 持久化函数（Hive metastore 中注册的 UDF）桥接成 V2 UnboundFunction。
 *
 * Spark 3.5 的 Analyzer 会对 V2 Catalog 返回的函数强制调用 bind()，因此原来直接返回
 * V1Function 会抛 "Cannot bind a V1 function"。这里我们在 bind() 阶段提前把 V1 函数
 * 解析为一个 Catalyst Expression，再用 [[KyuubiHiveV1ScalarFunction]] 包装成 V2
 * ScalarFunction 返回。
 */
class KyuubiHiveUnboundFunction(
    ident: Identifier,
    catalog: HiveSessionCatalog) extends UnboundFunction {

  override def name(): String = ident.name()

  override def description(): String = {
    try {
      val info = catalog.lookupFunctionInfo(ident.asFunctionIdentifier)
      Option(info).map { i =>
        val usage = Option(i.getUsage).getOrElse("")
        val extended = Option(i.getExtended).getOrElse("")
        if (extended.nonEmpty) s"$usage\n$extended" else usage
      }.getOrElse("")
    } catch {
      case _: Throwable => ""
    }
  }

  override def bind(inputType: StructType): BoundFunction = {
    val dummyArgs = inputType.zipWithIndex.map { case (field, index) =>
      BoundReference(index, field.dataType, field.nullable)
    }
    val expr = catalog.resolvePersistentFunction(ident.asFunctionIdentifier, dummyArgs)

    expr match {
      case _: AggregateFunction =>
        throw new UnsupportedOperationException(
          s"Aggregate function '${ident.name()}' is not supported via V2 catalog bridge. " +
            "Please invoke it through the Spark session catalog directly.")
      case _: Generator =>
        throw new UnsupportedOperationException(
          s"Generator function '${ident.name()}' is not supported via V2 catalog bridge. " +
            "Please invoke it through the Spark session catalog directly.")
      case _ =>
        new KyuubiHiveV1ScalarFunction(
          name(),
          expr,
          inputType.fields.map(_.dataType))
    }
  }
}
