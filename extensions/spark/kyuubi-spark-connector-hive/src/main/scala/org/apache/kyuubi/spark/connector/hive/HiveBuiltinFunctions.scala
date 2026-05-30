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

import org.apache.spark.sql.catalyst.expressions.{BitwiseAnd, BoundReference, HiveHash, Literal, Pmod}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, UnboundFunction}
import org.apache.spark.sql.types.{ByteType, IntegerType, ShortType, StructType}

/**
 * Built-in functions exposed by the Hive connector itself, resolved without going through the
 * Hive metastore. These are the functions whose semantics need to be controlled by the connector
 * (e.g. the Hive bucket-id expression used during writes).
 */
private[hive] object HiveBuiltinFunctions {

  /**
   * Look up a connector built-in function by [[Identifier]]. Built-in functions live in the
   * connector's empty namespace; identifiers carrying any namespace are treated as user
   * functions and are NOT matched here.
   */
  def lookup(ident: Identifier): Option[UnboundFunction] = {
    if (ident.namespace().nonEmpty) {
      None
    } else {
      ident.name() match {
        case HiveBucketUnboundFunction.NAME => Some(new HiveBucketUnboundFunction)
        case _ => None
      }
    }
  }
}

/**
 * V2 [[UnboundFunction]] computing the Hive-compatible bucket id for a given row.
 *
 * Signature: `hive_bucket(numBuckets, col1, col2, ...)` -> `Int`
 *
 * Bucket id formula matches Hive / Trino / Presto and Spark V1's `V1WritesUtils`:
 * {{{
 *   Pmod(BitwiseAnd(HiveHash(col1, col2, ...), Int.MaxValue), numBuckets)
 * }}}
 *
 * Used by [[org.apache.kyuubi.spark.connector.hive.write.HiveWrite#requiredOrdering]] to
 * express the per-task bucket-id sort key in terms a V2 connector [[Transform]] (named
 * transform) can carry. Spark resolves the transform at planning time via
 * `V2ExpressionUtils#toCatalystTransformOpt`, finds this function through
 * [[HiveTableCatalog]] (which mixes in [[org.apache.spark.sql.connector.catalog.FunctionCatalog]]),
 * binds it here, and ultimately rewrites it into a catalyst `Pmod(BitwiseAnd(HiveHash(...)))`
 * that participates in the standard `Sort` operator. This removes the need for a custom
 * task-level sorting writer (Spark's stock `SortExec` performs the spillable sort).
 */
private[hive] class HiveBucketUnboundFunction extends UnboundFunction {

  override def name(): String = HiveBucketUnboundFunction.NAME

  override def description(): String =
    """hive_bucket(numBuckets, col1[, col2, ...]) -> Int
      |Compute the Hive-compatible bucket id (`Pmod(BitwiseAnd(HiveHash(cols), Int.MaxValue),
      |numBuckets)`). The first argument must be a literal integer; remaining arguments are the
      |bucket key columns. Used internally by the Kyuubi Hive connector to express the bucket-id
      |sort key during writes; not intended for direct invocation in user queries.""".stripMargin

  override def bind(inputType: StructType): BoundFunction = {
    if (inputType.size < 2) {
      throw new UnsupportedOperationException(
        s"$name requires at least 2 arguments (numBuckets, col1), got ${inputType.size}")
    }
    val numBucketsField = inputType.fields.head
    numBucketsField.dataType match {
      case ByteType | ShortType | IntegerType => // ok
      case other =>
        throw new UnsupportedOperationException(
          s"$name expects the first argument (numBuckets) to be an integer type, got $other")
    }

    val numBucketsRef = BoundReference(0, numBucketsField.dataType, numBucketsField.nullable)
    val columnRefs = inputType.fields.zipWithIndex.tail.map { case (field, idx) =>
      BoundReference(idx, field.dataType, field.nullable)
    }
    val hashId =
      BitwiseAnd(HiveHash(columnRefs.toSeq), Literal(Int.MaxValue))
    val expr = Pmod(hashId, numBucketsRef)

    new KyuubiHiveV1ScalarFunction(
      name(),
      expr,
      inputType.fields.map(_.dataType))
  }
}

private[hive] object HiveBucketUnboundFunction {
  /**
   * Reserved function name. We use a deliberately verbose, namespace-less name to minimize the
   * chance of colliding with user-defined Hive UDFs.
   */
  val NAME: String = "hive_bucket"
}
