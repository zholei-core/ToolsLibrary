package com.zho.flinkutils.flinktable

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}



object FlinkTableSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    val filePath = "/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    val sensorTable: Table = tableEnv.from("inputTable")

    val result: Table = sensorTable.select('id,'temperature.count())
//      .filter('id == "sensor_1")


    tableEnv.from("inputTable").toAppendStream[(String,Long,Double)].print("temp View:")
    sensorTable.toAppendStream[(String,Long,Double)].print("Table:")
//    result.toRetractStream[(String,Long)].print("Table agg:")
    env.execute("Flink Table Example Sink")
  }

}
