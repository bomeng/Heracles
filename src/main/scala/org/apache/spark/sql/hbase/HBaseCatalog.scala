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

import java.io._
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.zip._

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.Batch
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.TaskID
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hbase.HBaseCatalog._
import org.apache.spark.sql.hbase.HBasePartitioner.HBaseRawOrdering
import org.apache.spark.sql.hbase.util.{BinaryBytesUtils, DataTypeUtils, Util}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.util.SerializableConfiguration
import org.apache.spark.{SparkEnv, SparkException, SparkHadoopWriter, TaskContext}

import scala.annotation.meta.param
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Column represent the sql column
 * sqlName the name of the column
 * dataType the data type of the column
 */
sealed abstract class AbstractColumn extends Serializable {
  val sqlName: String
  val dataType: DataType
  var ordinal: Int = -1

  def isKeyColumn: Boolean

  override def toString: String = {
    s"$isKeyColumn,$ordinal,$sqlName,${dataType.typeName}"
  }
}

case class KeyColumn(sqlName: String, dataType: DataType, order: Int)
  extends AbstractColumn {
  override def isKeyColumn: Boolean = true

  override def toString = super.toString + s",$order"
}

case class NonKeyColumn(sqlName: String, dataType: DataType, family: String, qualifier: String)
  extends AbstractColumn {
  @transient lazy val familyRaw = Bytes.toBytes(family)
  @transient lazy val qualifierRaw = Bytes.toBytes(qualifier)

  override def isKeyColumn: Boolean = false

  override def toString = super.toString + s",$family,$qualifier"
}

private[hbase] class HBaseCatalog(@(transient@param) sqlContext: SQLContext,
                                  @(transient@param) configuration: Configuration)
  extends ExternalCatalog with Logging with Serializable {

  @transient
  lazy val connection = ConnectionFactory.createConnection(configuration)

  lazy val relationMapCache = new ConcurrentHashMap[String, HBaseRelation].asScala

  private var admin_ : Option[Admin] = None

  private val caseSensitive = sqlContext.conf.caseSensitiveAnalysis

  private[sql] def admin: Admin = {
    if (admin_.isEmpty) {
      admin_ = Some(connection.getAdmin)
    }
    admin_.get
  }

  private[sql] def stopAdmin(): Unit = {
    admin_.foreach(_.close())
    admin_ = None
  }

  private def processName(name: String): String = {
    if (!caseSensitive) {
      name.toLowerCase
    } else {
      name
    }
  }

  protected[hbase] def createHBaseUserTable(tableName: TableName,
                                            families: Set[String],
                                            splitKeys: Array[Array[Byte]],
                                            useCoprocessor: Boolean = true): Unit = {
    val tableDescriptor = new HTableDescriptor(tableName)

    families.foreach(family => {
      val colDsc = new HColumnDescriptor(family)
      colDsc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
      tableDescriptor.addFamily(colDsc)
    })

    if (useCoprocessor && deploySuccessfully.get) {
      tableDescriptor.addCoprocessor(
        "org.apache.spark.sql.hbase.SparkSqlRegionObserver",
        null, Coprocessor.PRIORITY_USER, null)
    }

    admin.createTable(tableDescriptor, splitKeys)
  }

  def hasCoprocessor(hbaseTableName: TableName): Boolean = {
    val hTableDescriptor = admin.getTableDescriptor(hbaseTableName)
    hTableDescriptor.hasCoprocessor("org.apache.spark.sql.hbase.SparkSqlRegionObserver")
  }

  @transient protected[hbase] var deploySuccessfully_internal: Option[Boolean] = _

  def deploySuccessfully: Option[Boolean] = {
    if (deploySuccessfully_internal == null) {
      if (sqlContext.conf.asInstanceOf[HBaseSQLConf].useCoprocessor) {
        val metadataTable = getMetadataTable
        // When building the connection to the hbase table, we need to check
        // whether the current directory in the regionserver is accessible or not.
        // Or else it might crash the HBase regionserver!!!
        // For details, please read the comment in CheckDirEndPointImpl.
        val request = CheckDirProtos.CheckRequest.getDefaultInstance
        val batch = new Batch.Call[CheckDirProtos.CheckDirService, Boolean]() {
          def call(counter: CheckDirProtos.CheckDirService): Boolean = {
            val rpcCallback = new BlockingRpcCallback[CheckDirProtos.CheckResponse]
            counter.getCheckResult(null, request, rpcCallback)
            val response = rpcCallback.get
            if (response != null && response.hasAccessible) {
              response.getAccessible
            } else false
          }
        }
        val results = metadataTable.coprocessorService(
          classOf[CheckDirProtos.CheckDirService], null, null, batch
        )

        deploySuccessfully_internal = Some(!results.isEmpty)
        if (results.isEmpty) {
          logWarning("""CheckDirEndPoint coprocessor deployment failed.""")
        }

        pwdIsAccessible = !results.containsValue(false)
        if (!pwdIsAccessible) {
          logWarning(
            """The directory of a certain regionserver is not accessible,
              |please add 'cd ~' before 'start regionserver' in your regionserver start script.""")
        }
        metadataTable.close()
      } else {
        deploySuccessfully_internal = Some(true)
      }
    }
    deploySuccessfully_internal
  }

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val db = dbDefinition.name
    if (NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR != db &&
      NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR != db) {

      if (admin.listNamespaceDescriptors().exists(_.getName == db)) {
        if (!ignoreIfExists) {
          throw new DatabaseAlreadyExistsException(dbDefinition.name)
        }
      } else {
        admin.createNamespace(NamespaceDescriptor.create(db)
          .addConfiguration(mapAsJavaMap(dbDefinition.properties)).build())
      }
      stopAdmin()
    } else {
      logInfo(s"Cannot create default namespace or system namespace: $db")
    }
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    if (NamespaceDescriptor.DEFAULT_NAMESPACE.getName == db ||
      NamespaceDescriptor.SYSTEM_NAMESPACE.getName == db) {
      throw new SparkException(s"Cannot drop default namespace or system namespace: $db")
    }

    val namespace = admin.getNamespaceDescriptor(db)
    if (namespace != null) {
      val tables = admin.listTableNamesByNamespace(db)
      if (!cascade) {
        if (tables.nonEmpty) {
          throw new AnalysisException(s"Database '$db' is not empty. One or more tables exist.")
        }
      }
      // Remove the tables
      for (table <- tables) {
        admin.disableTable(table)
        admin.deleteTable(table)
      }
      // delete the entries in the metadata table
      val metadataTable = getMetadataTable
      for (key <- relationMapCache.keys) {
        if (key.startsWith(getCacheMapKey(db, ""))) {
          val delete = new Delete(Bytes.toBytes(key))
          metadataTable.delete(delete)
          relationMapCache.remove(key)
        }
      }
      metadataTable.close()
      admin.deleteNamespace(db)
      stopAdmin()
    } else {
      if (!ignoreIfNotExists) {
        throw new NoSuchDatabaseException(db)
      }
    }
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    throw new UnsupportedOperationException("Alter database is not implemented")
  }

  override def getDatabase(db: String): CatalogDatabase = {
    requireDbExists(db)
    val namespace = admin.getNamespaceDescriptor(db)
    stopAdmin()
    CatalogDatabase(namespace.getName, "", "", namespace.getConfiguration.asScala.toMap)
  }

  override def databaseExists(db: String): Boolean = {
    val result = admin.getNamespaceDescriptor(db) != null
    stopAdmin()
    result
  }

  override def listDatabases(): Seq[String] = {
    val result = admin.listNamespaceDescriptors().map(_.getName).sorted
    stopAdmin()
    result
  }

  override def listDatabases(pattern: String): Seq[String] = {
    StringUtils.filterPattern(listDatabases(), pattern)
  }

  override def setCurrentDatabase(db: String): Unit = {
    throw new UnsupportedOperationException("setCurrentDatabase is not implemented")
  }

  // When building the connection to the hbase table, we need to check
  // whether the current directory in the regionserver is accessible or not.
  // Or else it might crash the HBase regionserver!!!
  // For details, please read the comment in CheckDirEndPointImpl.
  @transient var pwdIsAccessible = false

  def createTable(tableName: String, hbaseNamespace: String, hbaseTableName: String,
                  allColumns: Seq[AbstractColumn], splitKeys: Array[Array[Byte]],
                  encodingFormat: String = BinaryBytesUtils.name): HBaseRelation = {
    val metadataTable = getMetadataTable

    if (checkLogicalTableExist(hbaseNamespace, tableName, metadataTable)) {
      throw new SparkException(s"The logical table: $tableName already exists")
    }
    // create a new hbase table for the user if not exist
    val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
      .asInstanceOf[Seq[NonKeyColumn]]
    val families = nonKeyColumns.map(_.family).toSet
    val hTableName = TableName.valueOf(hbaseNamespace, hbaseTableName)
    if (!checkHBaseTableExists(hTableName)) {
      createHBaseUserTable(hTableName, families, splitKeys,
        sqlContext.conf.asInstanceOf[HBaseSQLConf].useCoprocessor)
    } else {
      families.foreach { family =>
        if (!checkFamilyExists(hTableName, family)) {
          throw new SparkException(s"HBase table does not contain column family: $family")
        }
      }
    }

    val get = new Get(Bytes.toBytes(getCacheMapKey(hbaseNamespace, tableName)))
    val result = if (metadataTable.exists(get)) {
      throw new SparkException(s"Row key $tableName exists")
    } else {
      val hbaseRelation = HBaseRelation(tableName, hbaseNamespace, hbaseTableName,
        allColumns, deploySuccessfully,
        hasCoprocessor(TableName.valueOf(hbaseNamespace, hbaseTableName)),
        encodingFormat, connection)(sqlContext)
      hbaseRelation.setConfig(configuration)

      writeObjectToTable(hbaseRelation, metadataTable)

      relationMapCache.put(getCacheMapKey(hbaseNamespace, tableName), hbaseRelation)
      hbaseRelation
    }
    metadataTable.close()
    stopAdmin()
    result
  }

  private def getCacheMapKey(namespace: String, table: String): String = {
    val ns = if (namespace == null || namespace.isEmpty) {
      NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR
    } else {
      namespace
    }
    s"${processName(ns)}:${processName(table)}"
  }

  override def createTable(namespace: String, tableDefinition: CatalogTable, ignoreIfExists: Boolean) = {
    requireDbExists(namespace)
    val table = tableDefinition.identifier.table
    if (tableExists(namespace, table)) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistsException(db = namespace, table = table)
      }
    } else {
      val hbaseNamespace = tableDefinition.identifier.database.orNull
      val hbaseTableName = tableDefinition.properties.getOrElse(HBaseSQLConf.HBASE_TABLENAME, null)
      if (hbaseTableName == null) {
        throw new SparkException(s"HBase table name is not defined")
      }
      val encodingFormat = tableDefinition.properties.getOrElse(HBaseSQLConf.ENCODING_FORMAT, BinaryBytesUtils.name)
      val colsSeq = tableDefinition.schema.map(f => processName(f.name)).asJava
      val keyCols: Seq[(String, String)] = {
        val keys = tableDefinition.properties.getOrElse(HBaseSQLConf.KEY_COLS, "")
          .split(";").filter(_.nonEmpty)
        keys.map { f =>
          val name = processName(f.trim)
          val column = tableDefinition.schema.find(field => processName(field.name) == name)
          if (column.isEmpty) {
            throw new SparkException(s"HBase key column name $name is not defined properly")
          }
          (name, column.get.dataType)
        }
      }
      val nonKeyCols: Seq[(String, String, String, String)] = {
        val nonkeys = tableDefinition.properties.getOrElse(HBaseSQLConf.NONKEY_COLS, "").split(";")
              .map(_.trim).filter(_.nonEmpty)
        nonkeys.map { c =>
          val cols = c.split(",")
          val name = processName(cols(0).trim)
          val column = tableDefinition.schema.find(field => processName(field.name) == name)
          if (column.isEmpty) {
            throw new SparkException(s"HBase non-key column name $name is not defined properly")
          }
          val family = cols(1).trim
          val qualifier = cols(2).trim
          (name, column.get.dataType, family, qualifier)
        }
      }

      val keyMap: Map[String, String] = keyCols.toMap
      val allColumns = colsSeq.map {
        name =>
          if (keyMap.contains(name)) {
            KeyColumn(
              name,
              DataTypeUtils.getDataType(keyMap(name)),
              keyCols.indexWhere(_._1 == name))
          } else {
            val nonKeyCol = nonKeyCols.find(_._1 == name).get
            NonKeyColumn(
              name,
              DataTypeUtils.getDataType(nonKeyCol._2),
              nonKeyCol._3,
              nonKeyCol._4
            )
          }
      }

      createTable(table, hbaseNamespace, hbaseTableName, allColumns, null, encodingFormat)
    }
  }

  override def dropTable(namespace: String, table: String, ignoreIfNotExists: Boolean): Unit = {
    requireDbExists(namespace)
    if (tableExists(namespace, table)) {
      val metadataTable = getMetadataTable
      val delete = new Delete(Bytes.toBytes(getCacheMapKey(namespace, table)))
      metadataTable.delete(delete)
      metadataTable.close()
      relationMapCache.remove(getCacheMapKey(namespace, table))
    } else {
      if (!ignoreIfNotExists) {
        throw new NoSuchTableException(db = namespace, table = table)
      }
    }
  }

  override def renameTable(namespace: String, oldName: String, newName: String): Unit = {
    throw new UnsupportedOperationException("Rename table is not implemented")
  }

  override def alterTable(namespace: String, tableDefinition: CatalogTable): Unit = {
    throw new UnsupportedOperationException("Alter table is not implemented")
  }

  private def requireTableExists(namespace: String, table: String): Unit = {
    if (!tableExists(namespace, table)) {
      throw new NoSuchTableException(db = namespace, table = table)
    }
  }

  override def getTable(namespace: String, table: String): CatalogTable = {
    requireTableExists(namespace, table)

    val relation = getHBaseRelation(namespace, table)
    val identifier = TableIdentifier(table, Some(namespace))

    // prepare the schema for the table
    val schema = relation.get.allColumns.map {
      case k: KeyColumn =>
        CatalogColumn(k.sqlName, k.dataType.simpleString, nullable = false,
          comment = Some(s"KEY COLUMNS ${k.order}"))
      case nk: NonKeyColumn =>
        CatalogColumn(nk.sqlName, nk.dataType.simpleString, nullable = true,
          comment = Some(s"NON KEY COLUMNS ${nk.family}:${nk.qualifier}"))
    }

    val catalogTable = CatalogTable(identifier, CatalogTableType.EXTERNAL,
      CatalogStorageFormat.empty, schema,
      properties = immutable.Map(HBaseSQLConf.PROVIDER -> HBaseSQLConf.HBASE,
        HBaseSQLConf.NAMESPACE -> namespace, HBaseSQLConf.TABLE -> table))
    catalogTable
  }

  override def getTableOption(namespace: String, table: String): Option[CatalogTable] = {
    if (!tableExists(namespace, table)) None else Option(getTable(namespace, table))
  }

  override def tableExists(namespace: String, table: String): Boolean = {
    requireDbExists(namespace)

    val result = checkLogicalTableExist(namespace, table)
    result
  }

  override def listTables(namespace: String): Seq[String] = {
    requireDbExists(namespace)

    val metadataTable = getMetadataTable
    val tables = new ArrayBuffer[String]()
    val scanner = metadataTable.getScanner(ColumnFamily)
    var result = scanner.next()
    while (result != null) {
      val relation = getRelationFromResult(result)
      if (relation.hbaseNamespace == namespace) {
        tables.append(relation.tableName)
      }
      result = scanner.next()
    }
    metadataTable.close()
    tables.sorted
  }

  override def listTables(namespace: String, pattern: String): Seq[String] = {
    StringUtils.filterPattern(listTables(namespace), pattern)
  }

  override def loadTable(
                          namespace: String,
                          table: String,
                          loadPath: String,
                          isOverwrite: Boolean,
                          holdDDLTime: Boolean): Unit = {
    @transient val solvedRelation = lookupRelation(namespace, table)
    @transient val relation: HBaseRelation = solvedRelation.asInstanceOf[SubqueryAlias]
      .child.asInstanceOf[LogicalRelation]
      .relation.asInstanceOf[HBaseRelation]
    @transient val hbContext = sqlContext

    // tmp path for storing HFile
    @transient val tmpPath = Util.getTempFilePath(
      hbContext.sparkContext.hadoopConfiguration, relation.tableName)
    @transient val job = Job.getInstance(hbContext.sparkContext.hadoopConfiguration)

    HFileOutputFormat2.configureIncrementalLoad(job, relation.htable,
      relation.connection_.getRegionLocator(relation.hTableName))
    job.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", tmpPath)

    @transient val conf = job.getConfiguration

    @transient val hadoopReader = {
      if (loadPath.toLowerCase().startsWith("hdfs")) {
        new HadoopReader(sqlContext.sparkContext, loadPath)(relation)
      } else {
        val fs = FileSystem.getLocal(conf)
        val pathString = fs.pathToFile(new Path(loadPath)).toURI.toURL.toString
        new HadoopReader(sqlContext.sparkContext, pathString)(relation)
      }
    }

    @transient val splitKeys = relation.getRegionStartKeys.toArray
    @transient val wrappedConf = new SerializableConfiguration(job.getConfiguration)

    @transient val rdd = hadoopReader.makeBulkLoadRDDFromTextFile
    @transient val partitioner = new HBasePartitioner(splitKeys)
    @transient val ordering = Ordering[HBaseRawType]
    @transient val shuffled =
      new HBaseShuffledRDD(rdd, partitioner, relation.partitions).setKeyOrdering(ordering)

    @transient val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    @transient val jobtrackerID = formatter.format(new Date())
    @transient val stageId = shuffled.id
    @transient val jobFormat = new HFileOutputFormat2

    if (SparkEnv.get.conf.getBoolean("spark.hadoop.validateOutputSpecs", defaultValue = true)) {
      // FileOutputFormat ignores the filesystem parameter
      jobFormat.checkOutputSpecs(job)
    }

    @transient val par = true
    @transient val writeShard =
      (context: TaskContext, iter: Iterator[(HBaseRawType, Array[HBaseRawType])]) => {
        val config = wrappedConf.value
        /* "reduce task" <split #> <attempt # = spark task #> */

        val attemptId = (context.taskAttemptId % Int.MaxValue).toInt
        val jID = SparkHadoopWriter.createJobID(new Date(), context.stageId)
        val taID = new TaskAttemptID(
          new TaskID(jID, TaskType.MAP, context.partitionId), attemptId)

        val hadoopContext = new TaskAttemptContextImpl(config, taID)

        val format = new HFileOutputFormat2
        format match {
          case c: Configurable => c.setConf(config)
          case _ => ()
        }
        val committer = format.getOutputCommitter(hadoopContext).asInstanceOf[FileOutputCommitter]
        committer.setupTask(hadoopContext)

        val writer = format.getRecordWriter(hadoopContext).
          asInstanceOf[RecordWriter[ImmutableBytesWritable, KeyValue]]
        val bytesWritable = new ImmutableBytesWritable
        var recordsWritten = 0L
        var kv: (HBaseRawType, Array[HBaseRawType]) = null
        var prevK: HBaseRawType = null
        val columnFamilyNames =
          relation.htable.getTableDescriptor.getColumnFamilies.map(_.getName)

        var isEmptyRow = true

        try {
          while (iter.hasNext) {
            kv = iter.next()

            if (prevK != null && Bytes.compareTo(kv._1, prevK) == 0) {
              // force flush because we cannot guarantee intra-row ordering
              logInfo(s"flushing HFile writer " + writer)
              // look at the type so we can print the name of the flushed file
              writer.write(null, null)
            }

            isEmptyRow = true
            for (i <- kv._2.indices) {
              if (kv._2(i).nonEmpty) {
                isEmptyRow = false
                val nkc = relation.nonKeyColumns(i)
                bytesWritable.set(kv._1)
                writer.write(bytesWritable, new KeyValue(kv._1, nkc.familyRaw,
                  nkc.qualifierRaw, kv._2(i)))
              }
            }

            if (isEmptyRow) {
              bytesWritable.set(kv._1)
              writer.write(bytesWritable,
                new KeyValue(
                  kv._1,
                  columnFamilyNames(0),
                  HConstants.EMPTY_BYTE_ARRAY,
                  HConstants.EMPTY_BYTE_ARRAY))
            }

            recordsWritten += 1

            prevK = kv._1
          }
        } finally {
          writer.close(hadoopContext)
        }

        committer.commitTask(hadoopContext)
        logInfo(s"commit HFiles in $tmpPath")

        val targetPath = committer.getCommittedTaskPath(hadoopContext)
        if (par) {
          val load = new LoadIncrementalHFiles(config)
          // there maybe no target path
          logInfo(s"written $recordsWritten records")
          if (recordsWritten > 0) {
            load.doBulkLoad(targetPath, relation.connection_.getAdmin, relation.htable,
              relation.connection_.getRegionLocator(relation.hTableName))
            relation.close()
          }
        }
        1
      }: Int

    @transient val jobAttemptId =
      new TaskAttemptID(new TaskID(jobtrackerID, stageId, TaskType.MAP, 0), 0)
    @transient val jobTaskContext = new TaskAttemptContextImpl(wrappedConf.value, jobAttemptId)
    @transient val jobCommitter = jobFormat.getOutputCommitter(jobTaskContext)
    jobCommitter.setupJob(jobTaskContext)
    logDebug(s"Starting doBulkLoad on table ${relation.htable.getName} ...")

    sqlContext.sparkContext.runJob(shuffled, writeShard)
    logDebug(s"finished BulkLoad : ${System.currentTimeMillis()}")
    jobCommitter.commitJob(jobTaskContext)
    relation.close()
    Util.dropTempFilePath(hbContext.sparkContext.hadoopConfiguration, tmpPath)
    logDebug(s"finish BulkLoad on table ${relation.htable.getName}:" +
      s" ${System.currentTimeMillis()}")
    Seq.empty[Row]
  }

  override def loadPartition(
                              namespace: String,
                              table: String,
                              loadPath: String,
                              partition: TablePartitionSpec,
                              isOverwrite: Boolean,
                              holdDDLTime: Boolean,
                              inheritTableSpecs: Boolean,
                              isSkewedStoreAsSubdir: Boolean): Unit = {
    throw new UnsupportedOperationException("loadPartition is not implemented")
  }

  override def createPartitions(
                                 namespace: String,
                                 table: String,
                                 parts: Seq[CatalogTablePartition],
                                 ignoreIfExists: Boolean): Unit = {
    throw new UnsupportedOperationException("createPartitions is not implemented")
  }

  override def dropPartitions(
                               namespace: String,
                               table: String,
                               parts: Seq[TablePartitionSpec],
                               ignoreIfNotExists: Boolean): Unit = {
    throw new UnsupportedOperationException("dropPartitions is not implemented")
  }

  override def renamePartitions(
                                 namespace: String,
                                 table: String,
                                 specs: Seq[TablePartitionSpec],
                                 newSpecs: Seq[TablePartitionSpec]): Unit = {
    throw new UnsupportedOperationException("renamePartitions is not implemented")
  }

  override def alterPartitions(
                                namespace: String,
                                table: String,
                                parts: Seq[CatalogTablePartition]): Unit = {
    throw new UnsupportedOperationException("alterPartitions is not implemented")
  }

  override def getPartition(
                             namespace: String,
                             table: String,
                             spec: TablePartitionSpec): CatalogTablePartition = {
    throw new UnsupportedOperationException("getPartition is not implemented")
  }

  override def listPartitions(
                               namespace: String,
                               table: String,
                               partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    throw new UnsupportedOperationException("listPartitions is not implemented")
  }

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    throw new UnsupportedOperationException("createFunction is not implemented")
  }

  override def dropFunction(db: String, funcName: String): Unit = {
    throw new UnsupportedOperationException("dropFunction is not implemented")
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = {
    throw new UnsupportedOperationException("renameFunction is not implemented")
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = {
    throw new UnsupportedOperationException("getFunction is not implemented")
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    throw new UnsupportedOperationException("functionExists is not implemented")
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    throw new UnsupportedOperationException("listFunctions is not implemented")
  }

  def alterTableDropNonKey(namespace: String, tableName: String, columnName: String) = {
    val metadataTable = getMetadataTable
    val result = getHBaseRelation(namespace, tableName, metadataTable)
    if (result.isDefined) {
      val relation = result.get
      val allColumns = relation.allColumns.filter(_.sqlName != columnName)
      val hbaseRelation = HBaseRelation(relation.tableName,
        relation.hbaseNamespace, relation.hbaseTableName,
        allColumns, deploySuccessfully, relation.hasCoprocessor,
        connection = connection)(sqlContext)
      hbaseRelation.setConfig(configuration)

      writeObjectToTable(hbaseRelation, metadataTable)

      relationMapCache.put(getCacheMapKey(namespace, tableName), hbaseRelation)
    }
    metadataTable.close()
  }

  def alterTableAddNonKey(namespace: String, tableName: String, column: NonKeyColumn) = {
    val metadataTable = getMetadataTable
    val result = getHBaseRelation(namespace, tableName, metadataTable)
    if (result.isDefined) {
      val relation = result.get
      val allColumns = relation.allColumns :+ column
      val hbaseRelation = HBaseRelation(relation.tableName, relation.hbaseNamespace,
        relation.hbaseTableName, allColumns, deploySuccessfully, relation.hasCoprocessor,
        connection = connection)(sqlContext)
      hbaseRelation.setConfig(configuration)

      writeObjectToTable(hbaseRelation, metadataTable)

      relationMapCache.put(getCacheMapKey(namespace, tableName), hbaseRelation)
    }
    metadataTable.close()
  }

  private def writeObjectToTable(hbaseRelation: HBaseRelation,
                                 metadataTable: Table) = {
    val namespace = hbaseRelation.hbaseNamespace
    val tableName = hbaseRelation.tableName

    val put = new Put(Bytes.toBytes(getCacheMapKey(namespace, tableName)))
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream)
    val objectOutputStream = new ObjectOutputStream(deflaterOutputStream)

    objectOutputStream.writeObject(hbaseRelation)
    objectOutputStream.close()

    put.addImmutable(ColumnFamily, QualData, byteArrayOutputStream.toByteArray)

    // write to the metadata table
    metadataTable.put(put)
    metadataTable.close()
  }

  def getHBaseRelation(namespace: String, tableName: String,
                       metadataTable_ : Table = null): Option[HBaseRelation] = {
    val (metadataTable, needToCloseAtTheEnd) = {
      if (metadataTable_ == null) (getMetadataTable, true)
      else (metadataTable_, false)
    }

    val key = getCacheMapKey(namespace, tableName)
    var result = relationMapCache.get(key)
    if (result.isEmpty) {
      val get = new Get(Bytes.toBytes(key))
      val values = metadataTable.get(get)
      if (values == null || values.isEmpty) {
        result = None
      } else {
        result = Some(getRelationFromResult(values))
        relationMapCache.put(key, result.get)
      }
    }
    if (result.isDefined) {
      result.get.fetchPartitions()
    }
    if (needToCloseAtTheEnd) metadataTable.close()

    result
  }

  private def getRelationFromResult(result: Result): HBaseRelation = {
    val value = result.getValue(ColumnFamily, QualData)
    val byteArrayInputStream = new ByteArrayInputStream(value)
    val inflaterInputStream = new InflaterInputStream(byteArrayInputStream)
    val objectInputStream = new ObjectInputStream(inflaterInputStream)
    val hbaseRelation: HBaseRelation = objectInputStream.readObject().asInstanceOf[HBaseRelation]
    hbaseRelation.context = sqlContext
    hbaseRelation.setConfig(configuration)
    hbaseRelation
  }

  def lookupRelation(namespace: String, tableName: String, alias: Option[String] = None): LogicalPlan = {
    val hbaseRelation = getHBaseRelation(namespace, tableName)
    stopAdmin()
    if (hbaseRelation.isEmpty) {
      sys.error(s"Table Not Found: $tableName")
    } else {
      val tableWithQualifiers = SubqueryAlias(tableName, hbaseRelation.get.logicalRelation)
      alias.map(a => SubqueryAlias(a.toLowerCase, tableWithQualifiers))
        .getOrElse(tableWithQualifiers)
    }
  }

  private def getMetadataTable: Table = {
    // create the metadata table if it does not exist
    def checkAndCreateMetadataTable() = {
      if (!admin.tableExists(MetaData)) {
        val descriptor = new HTableDescriptor(MetaData)
        val columnDescriptor = new HColumnDescriptor(ColumnFamily)
        columnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
        descriptor.addFamily(columnDescriptor)
        admin.createTable(descriptor)
      }
    }

    checkAndCreateMetadataTable()

    // return the metadata table
    connection.getTable(MetaData)
  }

  private[hbase] def checkHBaseTableExists(hbaseTableName: TableName): Boolean = {
    admin.tableExists(hbaseTableName)
  }

  private def checkLogicalTableExist(namespace: String, tableName: String,
                                     metadataTable_ : Table = null): Boolean = {
    val (metadataTable, needToCloseAtTheEnd) = {
      if (metadataTable_ == null) (getMetadataTable, true)
      else (metadataTable_, false)
    }
    val get = new Get(Bytes.toBytes(getCacheMapKey(namespace, tableName)))
    val result = metadataTable.get(get)

    if (needToCloseAtTheEnd) metadataTable.close()
    result.size() > 0
  }

  private[hbase] def checkFamilyExists(hbaseTableName: TableName, family: String): Boolean = {
    val tableDescriptor = admin.getTableDescriptor(hbaseTableName)
    tableDescriptor.hasFamily(Bytes.toBytes(family))
  }
}

object HBaseCatalog {
  private final val MetaData = TableName.valueOf("metadata")
  private final val ColumnFamily = Bytes.toBytes("colfam")
  private final val QualData = Bytes.toBytes("data")
}
