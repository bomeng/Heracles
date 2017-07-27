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
package org.apache.spark.sql.hbase.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.hbase.util.DataTypeUtils
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

@DeveloperApi
case class AlterDropColCommand(namespace: String, tableName: String, columnName: String)
  extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.sharedState.externalCatalog.asInstanceOf[HBaseCatalog]
      .alterTableDropNonKey(namespace, tableName, columnName)
    sparkSession.sharedState.externalCatalog.asInstanceOf[HBaseCatalog].stopAdmin()
    Seq.empty[Row]
  }
}

@DeveloperApi
case class AlterAddColCommand(namespace: String,
                              tableName: String,
                              colName: String,
                              colType: String,
                              colFamily: String,
                              colQualifier: String) extends RunnableCommand {

  def run(sparkSession: SparkSession): Seq[Row] = {
    val hbaseCatalog = sparkSession.sharedState.externalCatalog.asInstanceOf[HBaseCatalog]
    hbaseCatalog.alterTableAddNonKey(namespace, tableName,
      NonKeyColumn(colName, DataTypeUtils.getDataType(colType), colFamily, colQualifier))
    hbaseCatalog.stopAdmin()
    Seq.empty[Row]
  }
}

@DeveloperApi
case class InsertValueIntoTableCommand(tid: TableIdentifier, valueSeq: Seq[String])
  extends RunnableCommand {
  override def run(sparkSession: SparkSession) = {
    val relation: HBaseRelation = sparkSession.sessionState.catalog.externalCatalog
      .asInstanceOf[HBaseCatalog]
      .getHBaseRelation(tid.database.getOrElse(null), tid.table).getOrElse(null)

    val bytes = valueSeq.zipWithIndex.map(v =>
      DataTypeUtils.string2TypeData(v._1, relation.schema(v._2).dataType))

    val rows = sparkSession.sparkContext.makeRDD(Seq(Row.fromSeq(bytes)))
    val inputValuesDF = sparkSession.createDataFrame(rows, relation.schema)
    relation.insert(inputValuesDF, overwrite = false)

    Seq.empty[Row]
  }

  override def output: Seq[Attribute] = Seq.empty
}
