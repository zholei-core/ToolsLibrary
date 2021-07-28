package com.zho.flinkutils.flinktable

import com.zho.flinkutils.flinkstream.flinksource.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._

object FlinkTableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputPath = "/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt"
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    // 基于流创建一张表
    val dataTable = tableEnv.fromDataStream(dataStream)

    // 调用 table api 进行转换
    val resultTable = dataTable
      .select("id,temperature")
      .filter("id =='sensor_1'")

    // 直接 sql 实现
    tableEnv.createTemporaryView("dataTable",dataTable)
    val sqlCode = "select id,temperature from dataTable where id='sensor_1'"
    val resultSqlTable = tableEnv.sqlQuery(sqlCode)

    resultTable.toAppendStream[(String,Double)].print("result")
    resultSqlTable.toAppendStream[(String,Double)].print("result sql")

    env.execute("table api example")
  }
}
