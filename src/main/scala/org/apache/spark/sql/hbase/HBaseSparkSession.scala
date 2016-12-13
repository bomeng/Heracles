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
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, FindDataSourceTable, PreprocessTableInsertion, ResolveDataSource}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner, datasources}
import org.apache.spark.sql.hbase.execution.{HBaseSourceAnalysis, HBaseStrategies}
import org.apache.spark.sql.internal.{SQLConf, SessionState, SharedState}

class HBaseSparkSession(sc: SparkContext) extends SparkSession(sc) {
  self =>

  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  @transient
  override private[sql] lazy val sessionState: SessionState = new HBaseSessionState(this)

  HBaseConfiguration.merge(
    sc.hadoopConfiguration, HBaseConfiguration.create(sc.hadoopConfiguration))

  @transient
  override  private[sql] lazy val sharedState: SharedState =
    new HBaseSharedState(sc, this.sqlContext)

  experimental.extraStrategies = Seq((new SparkPlanner(sc, sessionState.conf, Nil)
    with HBaseStrategies).HBaseDataSource)

  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches = Batch("Add exchange", Once, EnsureRequirements(sessionState.conf)) ::
      // No AddCoprocessor now for lack of unsafe support in coprocessor
      // maybe added later
      Nil
  }
}

class HBaseSessionState(sparkSession: SparkSession) extends SessionState(sparkSession) {
  override lazy val conf: SQLConf = new HBaseSQLConf

  override lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        PreprocessTableInsertion(conf) ::
          new FindDataSourceTable(sparkSession) ::
          DataSourceAnalysis(conf) ::
          HBaseSourceAnalysis(conf, sparkSession) ::
          (if (conf.runSQLonFile) new ResolveDataSource(sparkSession) :: Nil else Nil)

      override val extendedCheckRules = Seq(datasources.PreWriteCheck(conf, catalog))
    }
  }
}

class HBaseSharedState(sc: SparkContext, sqlContext: SQLContext) extends SharedState(sc) {
  override lazy val externalCatalog: ExternalCatalog =
    new HBaseCatalog(sqlContext, sc.hadoopConfiguration)
}
