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

package org.apache.kyuubi.spark.connector.hive.function

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Generator}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, UnboundFunction}
import org.apache.spark.sql.hive.kyuubi.connector.HiveBridgeHelper.HiveSessionCatalog
import org.apache.spark.sql.types.StructType

import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog.IdentifierHelper

/**
 * Bridges a persistent Hive metastore function into a V2 [[UnboundFunction]].
 *
 * Spark's internal `V1Function.bind()` throws on Spark 3.5 and is bypassed via a private
 * type-dispatch in `FunctionResolution` on master — third-party V2 function catalogs
 * cannot reuse that bypass, so [[bind]] must return a real, usable [[BoundFunction]].
 * We resolve the catalyst expression eagerly and wrap it in [[HiveScalarFunction]].
 *
 * Aggregate (UDAF) and generator (UDTF) Hive functions are not bridged: the V2
 * [[BoundFunction]] hierarchy has no compatible aggregate state contract or UDTF type.
 * Invoke them via `spark_catalog` instead.
 */
class HiveUnboundFunction(
    ident: Identifier,
    catalog: HiveSessionCatalog) extends UnboundFunction {

  override def name(): String = ident.name()

  override def description(): String = {
    try {
      val info = catalog.lookupPersistentFunction(ident.asFunctionIdentifier)
      Option(info).map { i =>
        val usage = Option(i.getUsage).getOrElse("")
        val extended = Option(i.getExtended).getOrElse("")
        if (extended.nonEmpty) s"$usage\n$extended" else usage
      }.getOrElse("")
    } catch {
      // best-effort metadata for `DESC FUNCTION`
      case _: AnalysisException => ""
    }
  }

  override def bind(inputType: StructType): BoundFunction = {
    // Positional placeholders: at exec time the V2 framework feeds an `InternalRow`
    // whose i-th field is the i-th argument's value, and `BoundReference(i).eval(row)`
    // reads exactly that slot.
    val argRefs = inputType.zipWithIndex.map { case (field, index) =>
      BoundReference(index, field.dataType, field.nullable)
    }
    val funcIdent = ident.asFunctionIdentifier
    val expr = catalog.resolvePersistentFunction(funcIdent, argRefs)

    expr match {
      case _: AggregateFunction =>
        throw new UnsupportedOperationException(
          s"Aggregate function '${ident.name()}' cannot be bridged through the V2 catalog; " +
            "invoke it via spark_catalog instead.")
      case _: Generator =>
        throw new UnsupportedOperationException(
          s"Generator function '${ident.name()}' cannot be bridged through the V2 catalog; " +
            "invoke it via spark_catalog instead.")
      case _ =>
        val className = catalog.getFunctionMetadata(funcIdent).className
        new HiveScalarFunction(name(), className, expr, inputType.fields.map(_.dataType))
    }
  }
}
