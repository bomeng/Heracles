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
package org.apache.spark.sql.hbase

import org.apache.hadoop.hbase._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hbase.util.HBaseKVHelper
import org.apache.spark.sql.types._

import scala.collection.Seq

class HBaseCatalogTestSuite extends TestBase {
  val (catalog, configuration) = (TestHbase.sharedState.externalCatalog.asInstanceOf[HBaseCatalog],
    TestHbase.sparkContext.hadoopConfiguration)

  test("Create Table") {
    // prepare the test data
    val namespace = "default"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"
    val family1 = "family1"
    val family2 = "family2"

    if (!catalog.checkHBaseTableExists(TableName.valueOf(namespace, hbaseTableName))) {
      val admin = catalog.admin
      val desc = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      desc.addFamily(new HColumnDescriptor(family1))
      desc.addFamily(new HColumnDescriptor(family2))
      admin.createTable(desc)
      admin.close()
    }

    var allColumns = List[AbstractColumn]()
    allColumns = allColumns :+ KeyColumn("column2", IntegerType, 1)
    allColumns = allColumns :+ KeyColumn("column1", StringType, 0)
    allColumns = allColumns :+ NonKeyColumn("column4", FloatType, family2, "qualifier2")
    allColumns = allColumns :+ NonKeyColumn("column3", BooleanType, family1, "qualifier1")

    val splitKeys: Array[Array[Byte]] = Array(
      new GenericRow(Array(1024.0, "Upen", 128: Short)),
      new GenericRow(Array(1024.0, "Upen", 256: Short)),
      new GenericRow(Array(4096.0, "SF", 512: Short))
    ).map(HBaseKVHelper.makeRowKey(_, Seq(DoubleType, StringType, ShortType)))

    catalog.createTable(tableName, namespace, hbaseTableName, allColumns, splitKeys)

    assert(catalog.tableExists(namespace, tableName) === true)
    catalog.stopAdmin()
  }

  test("Get Table") {
    // prepare the test data
    val hbaseNamespace = "default"
    val tableName = "testTable"
    val hbaseTableName = "hbaseTable"

    val oresult = catalog.getHBaseRelation(hbaseNamespace, tableName)
    assert(oresult.isDefined)
    val result = oresult.get
    assert(result.tableName === tableName)
    assert(result.hbaseNamespace === hbaseNamespace)
    assert(result.hbaseTableName === hbaseTableName)
    assert(result.keyColumns.size === 2)
    assert(result.nonKeyColumns.size === 2)
    assert(result.allColumns.size === 4)

    // check the data type
    assert(result.keyColumns.head.dataType === StringType)
    assert(result.keyColumns(1).dataType === IntegerType)
    assert(result.nonKeyColumns(1).dataType === FloatType)
    assert(result.nonKeyColumns.head.dataType === BooleanType)

    assert(result.nonKeyColumns.map(_.family) == List("family1", "family2"))
    val keyColumns = Seq(KeyColumn("column1", StringType, 0), KeyColumn("column2", IntegerType, 1))
    assert(result.keyColumns.equals(keyColumns))
    catalog.stopAdmin()
  }

  test("Alter Table") {
    val namespace = "default"
    val tableName = "testTable"

    val family1 = "family1"
    val column = NonKeyColumn("column5", BooleanType, family1, "qualifier3")

    catalog.alterTableAddNonKey(namespace, tableName, column)

    var result = catalog.getHBaseRelation(namespace, tableName)
    var table = result.get
    assert(table.allColumns.size === 5)

    catalog.alterTableDropNonKey(namespace, tableName, column.sqlName)
    result = catalog.getHBaseRelation(namespace, tableName)
    table = result.get
    assert(table.allColumns.size === 4)
    catalog.stopAdmin()
  }

  test("Delete Table") {
    // prepare the test data
    val namespace = "default"
    val tableName = "testTable"

    catalog.dropTable(namespace, tableName, true, true)
    assert(catalog.tableExists(namespace, tableName) === false)
    catalog.stopAdmin()
  }

  test("Check Logical Table Exist") {
    val namespace = "default"
    val tableName = "non-exist"

    assert(catalog.tableExists(namespace, tableName) === false)
    catalog.stopAdmin()
  }

  test("Namespce operations") {
    runSql("CREATE DATABASE db1")
    runSql(s"""CREATE TABLE db1.t1 (c1 INT, c2 INT) TBLPROPERTIES('hbaseTableName'='ht',
          'keyCols'='c1','nonKeyCols'='c2,cf,q')""")
    assert(runSql("SHOW DATABASES").length == 3)
    assert(runSql("SHOW tables IN db1").length == 1)
    runSql("DROP DATABASE db1 CASCADE")
    assert(runSql("SHOW DATABASES").length == 2)
  }
}
