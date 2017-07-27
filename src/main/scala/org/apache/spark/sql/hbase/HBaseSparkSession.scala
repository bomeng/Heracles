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

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.hbase.execution.{HBaseSourceAnalysis, HBaseStrategies}
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SQLConf, SessionState, SharedState}

class HBaseSparkSession(sc: SparkContext) extends SparkSession(sc) {
  self =>

  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  @transient
  override lazy val sessionState: SessionState = new HBaseSessionStateBuilder(this).build()

  HBaseConfiguration.merge(
    sc.hadoopConfiguration, HBaseConfiguration.create(sc.hadoopConfiguration))

  @transient
  override lazy val sharedState: SharedState =
    new HBaseSharedState(sc, this.sqlContext)
}

class HBaseSessionStateBuilder(session: SparkSession, parentState: Option[SessionState] = None) extends BaseSessionStateBuilder(session) {
  override lazy val conf: SQLConf = new HBaseSQLConf

  override protected def newBuilder: NewBuilder = new HBaseSessionStateBuilder(_, _)

  override lazy val experimentalMethods: ExperimentalMethods = {
    val result = new ExperimentalMethods;
    result.extraStrategies = Seq((new SparkPlanner(session.sparkContext, conf, new ExperimentalMethods)
      with HBaseStrategies).HBaseDataSource)
    result
  }

  override lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
          new FindDataSourceTable(session) +:
          new ResolveSQLOnFile(session) +:
          customResolutionRules

      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
          PreprocessTableCreation(session) +:
          PreprocessTableInsertion(conf) +:
          DataSourceAnalysis(conf) +:
          HBaseSourceAnalysis(session) +:
          customPostHocResolutionRules

      override val extendedCheckRules =
        customCheckRules
    }
  }
}

class HBaseSharedState(sc: SparkContext, sqlContext: SQLContext) extends SharedState(sc) {
  override lazy val externalCatalog: ExternalCatalog =
    new HBaseCatalog(sqlContext, sc.hadoopConfiguration)
}
