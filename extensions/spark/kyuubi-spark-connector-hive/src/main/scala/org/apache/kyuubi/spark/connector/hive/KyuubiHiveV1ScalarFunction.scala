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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.DataType

/**
 * 把 Spark V1 Catalyst Expression（如 HiveSimpleUDF / HiveGenericUDF / 内置标量表达式）
 * 包装成 V2 ScalarFunction，从而绕过 Spark 3.5 中 V2 Catalog 不能返回 V1Function 的限制。
 *
 * 注意：
 * 1. Spark 调用 produceResult 时，传入的 InternalRow 已经是"参数求值后的结果行"，
 *    即第 i 列是第 i 个参数 child 的值。因此 expr 内部的 BoundReference(i) 可以直接
 *    用 expr.eval(input) 求值，无需再包一层 SafeProjection。
 * 2. inputTypes 必须用 bind 阶段记录下来的"声明类型"，而不是 expr.children.map(_.dataType)，
 *    否则当 expr.children 中被插入了 Cast 节点时，会与 V2 框架的 ImplicitCastInputTypes
 *    产生类型双重转换。
 */
class KyuubiHiveV1ScalarFunction(
    funcName: String,
    expr: Expression,
    declaredInputTypes: Array[DataType]) extends ScalarFunction[Any] with Serializable {

  override def name(): String = funcName

  override def inputTypes(): Array[DataType] = declaredInputTypes

  override def resultType(): DataType = expr.dataType

  override def isResultNullable: Boolean = expr.nullable

  override def isDeterministic: Boolean = expr.deterministic

  override def produceResult(input: InternalRow): Any = expr.eval(input)
}
