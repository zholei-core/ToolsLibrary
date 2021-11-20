package com.zho.sparkutils.sparksql

import com.zho.logger.LoggerFactoryUtil
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

trait ResolveSQLRegistrationView extends LoggerFactoryUtil {

  /**
   * 解析 SQL 获取表名与元数据信息
   *
   * @param sqlCode sqlCode
   * @return meta.tablename List
   */
  def analysisMetabaseAndTableName(sqlCode: String): List[String] = {
    val sourceTablePattern: Regex = "from\\s(\\w+)\\.(\\w+)\\s|join\\s(\\w+)\\.(\\w+)\\s".r
    sourceTablePattern.findAllMatchIn(sqlCode.toLowerCase.replaceAll("\n|\\s+", " "))
      .map(s => s.toString
        .toLowerCase
        .replaceAll("from\\s+|join\\s+", "")
        .trim
      ).toList.distinct
  }

  /**
   *
   * @param sqlCode sqlCode
   * @param session       session
   */
  def registerView(sqlCode: String, session: SparkSession): (List[String], List[String]) = {
    val tableMetaList: List[String] = analysisMetabaseAndTableName(sqlCode)
    val listMeta = ListBuffer.empty[String]
    tableMetaList.foreach(metaElem => {
      //      val tempTable = line.substring(line.indexOf(".") + 1)
      // metabase tableName Split
      val databaseTableIter = metaElem.split("\\.")
      databaseTableIter(0) match {
        case "dwd_vw_dev" | "dm_vw_dev" =>
          //          session
          //            .read
          //            .parquet(s"/user/hive/warehouse/${metaElem.split("\\.")(0)}.db/${metaElem.split("\\.")(1)}")
          //            .createOrReplaceTempView(s"${metaElem.split("\\.")(1)}")
          println(s"register hiveTable view : metadatabase : ${metaElem.split("\\.")(0)} , tableName : ${metaElem.split("\\.")(1)}")
        case "ods" =>
          println(s"register kuduTable view : metadatabase : ${metaElem.split("\\.")(0)} , tableName : ${metaElem.split("\\.")(1)}")
        case _ =>
          logger.error(s"Metabase does not exist. Please update the data source configuration : $metaElem")
      }
      listMeta += databaseTableIter(0)
    })
    (tableMetaList, listMeta.toList.distinct)
  }

  def registerView(sqlCode: String): (List[String], List[String]) = {
    val tableMetaList: List[String] = analysisMetabaseAndTableName(sqlCode)
    val listMeta = ListBuffer.empty[String]
    tableMetaList.foreach(f = metaElem => {
      // metabase tableName Split
      val databaseTableIter = metaElem.split("\\.")
      databaseTableIter(0) match {
        case "dwd_vw_dev" | "dm_vw_dev" =>
          println(s"register hiveTable view : metadatabase : ${metaElem.split("\\.")(0)} , tableName : ${metaElem.split("\\.")(1)}")
        case "ods" =>
          println(s"register kuduTable view : metadatabase : ${metaElem.split("\\.")(0)} , tableName : ${metaElem.split("\\.")(1)}")
        case _ =>
          logger.error(s"Metabase does not exist. Please update the data source configuration : $metaElem")
      }
      listMeta += databaseTableIter(0)
    })
    (tableMetaList, listMeta.toList.distinct)
  }


}
