package com.microsoft.azure.sqldb.spark.connect

import com.microsoft.azure.sqldb.spark.config.{Config, SqlDBConfig}
import com.microsoft.azure.sqldb.spark.connect.ConnectionUtils.{createConnectionProperties, createJDBCUrl, getTableOrQuery}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcRelationProvider}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.JavaConverters._


class DefaultSource extends JdbcRelationProvider
  with DataSourceRegister {

  override def shortName(): String = "sqlserver"

  /** Creates the RDD used for reading from the database */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = sqlConfigToJdbcOptions(parameters)
    super.createRelation(sqlContext, jdbcOptions)
  }

  /** Creates the RDD used for writing to the database */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val jdbcOptions = sqlConfigToJdbcOptions(parameters)
    super.createRelation(sqlContext, mode, jdbcOptions, df)
  }

  private def sqlConfigToJdbcOptions(sqlDbOptions: Map[String, String]): Map[String, String] = {
    val readConfig = Config(sqlDbOptions)
    val url = createJDBCUrl(readConfig.get[String](SqlDBConfig.URL).get)
    val table = getTableOrQuery(readConfig)
    val properties = createConnectionProperties(readConfig)
    val jdbcOptions = new scala.collection.mutable.HashMap[String, String]
    jdbcOptions ++= sqlDbOptions
    jdbcOptions ++= properties.asScala
    jdbcOptions += (JDBCOptions.JDBC_URL -> url, JDBCOptions.JDBC_TABLE_NAME -> table)
    jdbcOptions.toMap
  }

}
