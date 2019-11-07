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

package org.apache.spark.sql.execution.customDatasource.jdbc

import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.execution.customDatasource.jdbc.JDBCUtils._
class DefaultSource extends CreatableRelationProvider with RelationProvider with DataSourceRegister {

  override def shortName(): String = "jdbc"
  import JdbcOptions._
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = new JdbcOptions(parameters)
    val partitionColumn = jdbcOptions.partitionColumn//分区字段
    val lowerBound = jdbcOptions.lowerBound
    val upperBound = jdbcOptions.upperBound
    val numPartitions = jdbcOptions.numPartitions

    val partitionInfo = if (partitionColumn == null) {
      //TODO 新加高版本的安全校验
      assert(lowerBound == null && upperBound == null, "When 'partitionColumn' is not specified, " + s"'$JDBC_LOWER_BOUND' and '$JDBC_UPPER_BOUND' are expected to be empty")
      null
    } else {
      //TODO 新加高版本的安全校验
      assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions.nonEmpty, s"When 'partitionColumn' is specified, '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', and " + s"'$JDBC_NUM_PARTITIONS' are also required")
      JDBCPartitioningInfo(partitionColumn, lowerBound.toLong, upperBound.toLong, numPartitions.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    JDBCRelation(parts, jdbcOptions)(sqlContext.sparkSession)
  }
  //TODO parameters需要携带url,table
  //TODO createTableOptions  : ENGINE=InnoDB DEFAULT CHARSET=utf8
  //TODO isTruncate : if to truncate the table from the JDBC database
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val jdbcOptions = new JdbcOptions(parameters)
    val url = jdbcOptions.url
    val table = jdbcOptions.table
    val createTableOptions = jdbcOptions.createTableOptions
    val isTruncate = jdbcOptions.isTruncate

    //todo 自适配update
    var saveMode = mode match{
      case SaveMode.Append => CustomSaveMode.Append
      case SaveMode.Overwrite => CustomSaveMode.Overwrite
      case SaveMode.ErrorIfExists => CustomSaveMode.ErrorIfExists
      case SaveMode.Ignore => CustomSaveMode.Ignore
    }
    val parameterLower = parameters.map(line =>  (line._1.toLowerCase() , line._2))
    if(parameterLower.keySet.contains("savemode")){
      println(s"########${parameterLower("savemode")}#########")
      saveMode = if(parameterLower("savemode").equals("update")) CustomSaveMode.Update else saveMode
    }

    val conn = JDBCUtils.createConnectionFactory(jdbcOptions)()
    try {
      val tableExists = JDBCUtils.tableExists(conn, url, table)
      //提前去数据库拿表的schema信息
      val tableSchema = JDBCUtils.getSchemaOption(conn, jdbcOptions)

      if (tableExists) {
        saveMode match {
          case CustomSaveMode.Overwrite =>
            if (isTruncate && isCascadingTruncateTable(url) == Some(false)) {
              // In this case, we should truncate table and then load.
              truncateTable(conn, table)
              saveTable(df, url, table,tableSchema,saveMode, jdbcOptions)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              dropTable(conn, table)
              createTable(df.schema, url, table, createTableOptions, conn)
              saveTable(df, url, table,Some(df.schema), saveMode,jdbcOptions)
            }

          case CustomSaveMode.Append =>
            saveTable(df, url, table,tableSchema,saveMode, jdbcOptions)

          case CustomSaveMode.ErrorIfExists =>
            throw new AnalysisException(
              s"Table or view '$table' already exists. SaveMode: ErrorIfExists.")

          case CustomSaveMode.Ignore =>
            // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
            // to not save the contents of the DataFrame and to not change the existing data.
            // Therefore, it is okay to do nothing here and then just return the relation below.
          case CustomSaveMode.Update =>
            saveTable(df, url, table,tableSchema,saveMode, jdbcOptions)
        }
      } else {
        createTable(df.schema, url, table, createTableOptions, conn)
        saveTable(df, url, table,Some(df.schema), saveMode, jdbcOptions)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }
}
