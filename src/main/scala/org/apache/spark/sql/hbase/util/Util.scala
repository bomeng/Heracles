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

package org.apache.spark.sql.hbase.util

import java.io._
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.{DeflaterOutputStream, InflaterInputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration

object Util {
  val iteration = new AtomicInteger(0)

  /**
   * Generate a temp folder on hadoop file system.
   * @param conf the hadoop configuration
   * @param prefix the prefix for that folder name to be generated
   * @return the folder name in string format
   */
  def getTempFilePath(conf: Configuration, prefix: String): String = {
    val fileSystem = FileSystem.get(conf)
    val path = new Path(s"$prefix-${System.currentTimeMillis()}-${iteration.getAndIncrement}")
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
    path.getName
  }

  /**
   * Drop the temp folder, return true if it exists and is dropped successfully,
   * otherwise return false
   * @param conf the hadoop configuration
   * @param path the path to be dropped
   * @return true if the folder path exists ad is dropped successfully, otherwise return false
   */
  def dropTempFilePath(conf: Configuration, path: String): Boolean = {
    val fileSystem = FileSystem.get(conf)
    val filePath = new Path(path)
    if (fileSystem.exists(filePath)) {
      fileSystem.delete(filePath, true)
    } else {
      false
    }
  }

  def serializeHBaseConfiguration(configuration: Configuration): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val deflaterOutputStream = new DeflaterOutputStream(bos)
    val dos = new DataOutputStream(deflaterOutputStream)
    configuration.write(dos)
    dos.close()
    bos.toByteArray
  }

  def deserializeHBaseConfiguration(arr: Array[Byte]) = {
    val conf = HBaseConfiguration.create
    conf.readFields(new DataInputStream(new InflaterInputStream(new ByteArrayInputStream(arr))))
    conf
  }
}
