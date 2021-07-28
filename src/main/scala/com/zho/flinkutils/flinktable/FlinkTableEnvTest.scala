package com.zho.flinkutils.flinktable

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}


object FlinkTableEnvTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
/*
    // 1.1 基于老版本planner 的刘处理
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

    // 1.2 基于老版本的批处理
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 基于blink planner  的流处理
    val blinkStreamSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

    // 1.4 基于blink planner  的批处理
    val blinkBatchSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)
*/

    /**
     * 2. 链接外部系统，读取数据，注册表
     */

    // 2.1 读取文件
    val filePath = "/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    // 2.2 从kafka 读取数据
    tableEnv.connect(new Kafka()
      .version("")
      .topic("")
      .property("zookeper.connect","localhost:2181")
      .property("bootstrap.servers","localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    val inputTable: Table = tableEnv.from("inputTable")
    inputTable.toAppendStream[(String,Long,Double)].print()



    env.execute("Table Env Tesst")

  }
}
