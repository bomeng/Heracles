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

import java.io.{File, PrintWriter}

import jline.console.ConsoleReader
import jline.console.completer.{ArgumentCompleter, Completer, FileNameCompleter, StringsCompleter}
import jline.console.history.FileHistory
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * HBaseSQLCliDriver
 *
 */
object HBaseSQLCliDriver extends Logging {
  final val ANSI_RESET = "\u001B[0m"
  final val ANSI_BLACK = "\u001B[30m"
  final val ANSI_RED = "\u001B[31m"
  final val ANSI_GREEN = "\u001B[32m"
  final val ANSI_YELLOW = "\u001B[33m"
  final val ANSI_BLUE = "\u001B[34m"
  final val ANSI_PURPLE = "\u001B[35m"
  final val ANSI_CYAN = "\u001B[36m"
  final val ANSI_WHITE = "\u001B[37m"

  private var currentColor = ANSI_RESET

  private val prompt = "hspark> "
  private val conf = new SparkConf(true).set("spark.hadoop.hbase.zookeeper.quorum", "localhost")
  private val sc = new SparkContext("local[2]", "hspark", conf)
  private val hbaseCtx = new HBaseSparkSession(sc)

  private val QUIT = "QUIT"
  private val EXIT = "EXIT"
  private val HELP = "HELP"
  private val COLOR = "COLOR"

  def getCompleters: Seq[Completer] = {
    val completers = ArrayBuffer[Completer]()

    completers.append(new StringsCompleter("create", "drop", "load", "select", "insert", "describe", "show", "color", "help"))
    completers.append(new ArgumentCompleter(new StringsCompleter("create"),
      new StringsCompleter("table () tblproperties ('hbaseTableName'='', 'keyCols'='', 'nonKeyCols'='')")))
    completers.append(new ArgumentCompleter(new StringsCompleter("drop"),
      new StringsCompleter("table", "database")))
    completers.append(new ArgumentCompleter(new StringsCompleter("load"),
      new StringsCompleter("data inpath into")))
    completers.append(new ArgumentCompleter(new StringsCompleter("select"),
      new StringsCompleter("from where")))
    completers.append(new ArgumentCompleter(new StringsCompleter("insert"),
      new StringsCompleter("into table values")))
    completers.append(new ArgumentCompleter(new StringsCompleter("describe"),
      new StringsCompleter("database")))
    completers.append(new ArgumentCompleter(new StringsCompleter("show"),
      new StringsCompleter("tables", "databases", "schemas")))
    completers.append(new ArgumentCompleter(new StringsCompleter("color"),
      new StringsCompleter("black", "red", "green", "yellow", "blue", "purple", "cyan", "white")))
    completers.append(new ArgumentCompleter(new StringsCompleter("help"),
      new StringsCompleter("create", "drop", "load", "select", "insert", "describe", "show", "color")))
    completers.append(new FileNameCompleter)

    completers
  }

  def main(args: Array[String]) {
    try {
      val reader = new ConsoleReader()
      reader.setPrompt(prompt)

      // set the completers
      getCompleters.foreach(reader.addCompleter)
      val out = new PrintWriter(reader.getOutput)

      // set history
      val historyDirectory = System.getProperty("user.home")
      try {
        if (new File(historyDirectory).exists()) {
          val historyFile = historyDirectory + File.separator + ".hsparkhistory"
          reader.setHistory(new FileHistory(new File(historyFile)))
        } else {
          System.err.println("WARNING: Directory for hspark history file: " + historyDirectory +
            " does not exist.   History will not be available during this session.")
        }
      } catch {
        case e: Exception =>
          System.err.println("WARNING: Encountered an error while trying to initialize hspark's " +
            "history file.  History will not be available during this session.")
          System.err.println(e.getMessage)
      }

      var break = false
      while (!break) {
        val line = reader.readLine
        if (line == null) {
          break = true
        } else {
          break = process(line, out)
        }
        if (break) {
          reader.getHistory.asInstanceOf[FileHistory].flush()
          reader.shutdown()
        }
      }
    } catch {
      case t: Throwable => t.printStackTrace()
    }
  }

  /**
   * process the line
   * @param input the user input
   * @param out the output writer
   * @return true if user wants to terminate; otherwise return false
   */
  private def process(input: String, out: PrintWriter): Boolean = {
    var line = input.trim
    if (line.length == 0) return false
    if (line.endsWith(";")) {
      line = line.substring(0, line.length - 1)
    }
    val token = line.split("\\s")
    token(0).toUpperCase match {
      case COLOR =>
        token(1).toUpperCase match {
          case "RESET" => currentColor = ANSI_RESET
          case "BLACK" => currentColor = ANSI_BLACK
          case "RED" => currentColor = ANSI_RED
          case "GREEN" => currentColor = ANSI_GREEN
          case "YELLOW" => currentColor = ANSI_YELLOW
          case "BLUE" => currentColor = ANSI_BLUE
          case "PURPLE" => currentColor = ANSI_PURPLE
          case "CYAN" => currentColor = ANSI_CYAN
          case "WHITE" => currentColor = ANSI_WHITE
          case _ => currentColor = ANSI_RESET
        }
        false
      case QUIT => true
      case EXIT => true
      case HELP => printHelp(token); false
      case _ =>
        try {
          logInfo(s"Processing $line")
          val start = System.currentTimeMillis()
          val df = hbaseCtx.sql(line)
          val str = df.showString(Integer.MAX_VALUE - 1, truncate =
            if (token(0) == "EXPLAIN") false else true
          )
          val end = System.currentTimeMillis()
          out.println("OK")
          if (!str.equals("++\n||\n++\n++\n")) out.println(s"$currentColor$str$ANSI_RESET")
          val timeTaken: Double = (end - start) / 1000.0
          out.println(s"Time taken: $timeTaken seconds")
          out.flush()
          false
        } catch {
          case e: Exception =>
            e.printStackTrace(out)
            false
        }
    }
  }

  private def printHelp(token: Array[String]) = {
    if (token.length > 1) {
      token(1).toUpperCase match {
        case "CREATE" =>
          println( """CREATE TABLE table_name (col_name data_type, ... , col_name, data_type) TBLPROPERTIES(
                      |'hbaseTableName'='hbase_table_name',
                      |'keyCols'='col_name;...;col_name',
                      |'nonKeyCols'='col_name,column_family,qualifier;...;col_name,column_family,qualifier')"""
            .stripMargin)
        case "DROP" =>
          println("DROP DATABASE db_name")
          println("DROP TABLE table_name")
        case "ALTER" =>
          println("Unsupported yet - ")
          println("ALTER TABLE table_name ADD (col_name data_type, ...) MAPPED BY (expression)")
          println("ALTER TABLE table_name DROP col_name")
        case "LOAD" =>
          println( """LOAD DATA INPATH file_path INTO TABLE table_name""".stripMargin)
        case "SELECT" =>
          println( """SELECT [ALL | DISTINCT] select_expr, select_expr, ...
                     |FROM table_reference
                     |[WHERE where_condition]
                     |[GROUP BY col_list]
                     |[CLUSTER BY col_list
                     |  | [DISTRIBUTE BY col_list] [SORT BY col_list]
                     |]
                     |[LIMIT number]""")
        case "INSERT" =>
          println("INSERT INTO TABLE table_name SELECT clause")
          println("INSERT INTO TABLE table_name VALUES (value, ...)")
        case "DESCRIBE" =>
          println("DESCRIBE table_name")
          println("DESCRIBE DATABASE [EXTENDED] db_name")
        case "SHOW" =>
          println("SHOW TABLES [(IN|FROM) database_name] [[LIKE] 'pattern']")
          println("SHOW (DATABASES|SCHEMAS) [LIKE 'pattern']")
        case _ =>
          printHelpUsage()
      }
    } else {
      printHelpUsage()
    }
  }
 
  private def printHelpUsage() = {
    println("""Usage: HELP Statement    
      Statement:
        CREATE | DROP | ALTER | LOAD | SELECT | INSERT | DESCRIBE | SHOW""")    
  }
}

