package com.zho.greenplumutils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkGpSourceTest {
  def main(args: Array[String]): Unit = {
    //MysqlDriver:"com.mysql.jdbc.Driver"
    //OracleDriver:"oracle.jdbc.driver.OracleDriver"
    val GreenplumDriver = "com.pivotal.jdbc.GreenplumDriver"
    //postgresqlDriver:"org.postgresql.Driver"

    //MysqlURL:"jdbc:mysql://localhost:3306/databaseName"
    //OracleURL:"jdbc:oracle:thin:@//localhost:1521:databaseName"
    val GreenplumURL = "jdbc:pivotal:greenplum://localhost:15432;DatabaseName=databaseName"
    //postgresqlURL:"jdbc:postgresql://localhost:5432/databaseName"



  }

  def JDBCSource(session: SparkSession): Unit = {
    session.read.format("jdbc")
      .option("driver", "com.pivotal.jdbc.GreenplumDrive")
      .option("url", "jdbc:pivotal:greenplum://localhost:15432;DatabaseName=databaseName")
      .option("dbtable", "")
      .option("user", "")
      .option("password", "")
      .load()
  }

  def JDBCSink(df: DataFrame): Unit = {
    df.write.mode(SaveMode.Append).format("jdbc")
      .option("driver", "com.pivotal.jdbc.GreenplumDrive")
      .option("url", "jdbc:pivotal:greenplum://localhost:15432;DatabaseName=databaseName")
      .option("dbtable", "")
      .option("user", "")
      .option("password", "")
      .save()
  }


}
