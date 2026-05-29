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

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.types.StructType

/**
 * 把 V1 [[CatalogTable]] 包装成 V2 [[View]] 对象。
 *
 * 仅当 `catalogTable.tableType == VIEW` 时使用。
 */
case class HiveView(catalogTable: CatalogTable) extends View {

  override def name(): String =
    catalogTable.identifier.unquotedString

  override def query(): String =
    catalogTable.viewText.getOrElse("")

  override def currentCatalog(): String =
    catalogTable.viewCatalogAndNamespace.headOption.orNull

  override def currentNamespace(): Array[String] =
    catalogTable.viewCatalogAndNamespace match {
      case ns if ns.length > 1 => ns.tail.toArray
      case _ => Array.empty
    }

  override def schema(): StructType = catalogTable.schema

  override def queryColumnNames(): Array[String] =
    catalogTable.viewQueryColumnNames.toArray

  override def columnAliases(): Array[String] =
    catalogTable.schema.fields.map(_.name)

  override def columnComments(): Array[String] =
    catalogTable.schema.fields.map(_.getComment().orNull)

  override def properties(): util.Map[String, String] =
    catalogTable.properties.asJava
}
