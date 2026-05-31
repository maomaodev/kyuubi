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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.DataType

/**
 * Adapts a catalyst [[Expression]] (e.g. `HiveSimpleUDF` / `HiveGenericUDF`) into a V2
 * [[ScalarFunction]] for use by [[HiveUnboundFunction#bind]].
 *
 * Notes:
 *   - [[produceResult]] simply delegates to `expr.eval(input)`. The wrapped expression's
 *     argument slots are `BoundReference(i, _, _)` set up at `bind()` time, which line
 *     up with the i-th field of the [[InternalRow]] handed in by Spark.
 *   - [[inputTypes]] is the V2-level argument types captured at `bind()` time, NOT
 *     `expr.children.map(_.dataType)`. The latter may carry implicit `Cast` nodes
 *     inserted by Spark's analyzer; reporting those types here would trip the V2
 *     `ImplicitCastInputTypes` rule into double-casting at execution.
 *   - The codegen magic `invoke` method is intentionally not implemented — the wrapped
 *     expression's runtime type is unknown at compile time.
 */
class HiveScalarFunction(
    funcName: String,
    className: String,
    expr: Expression,
    declaredInputTypes: Array[DataType]) extends ScalarFunction[Any] with Serializable {

  override def name(): String = funcName

  override def inputTypes(): Array[DataType] = declaredInputTypes

  override def resultType(): DataType = expr.dataType

  override def isResultNullable: Boolean = expr.nullable

  override def isDeterministic: Boolean = expr.deterministic

  override def produceResult(input: InternalRow): Any = expr.eval(input)

  /**
   * Stable canonical name `kyuubi-hive-connector.<funcName>.<className>(input types)`.
   * The default [[BoundFunction]] implementation returns a fresh UUID per call, which would
   * defeat `TransformExpression#isSameFunction` (storage-partitioned join planning) and
   * any other Spark internal equivalence check on bound functions.
   */
  override def canonicalName(): String =
    s"${HiveScalarFunction.CANONICAL_NAME_PREFIX}.$funcName.$className(" +
      declaredInputTypes.map(_.catalogString).mkString(",") + ")"

  override def equals(other: Any): Boolean = other match {
    case that: ScalarFunction[_] => canonicalName() == that.canonicalName()
    case _ => false
  }

  override def hashCode(): Int = canonicalName().hashCode
}

private[hive] object HiveScalarFunction {
  val CANONICAL_NAME_PREFIX: String = "kyuubi-hive-connector"
}
