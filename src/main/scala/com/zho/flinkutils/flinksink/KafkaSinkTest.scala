package com.zho.flinkutils.flinksink

import com.zho.flinkutils.flinksource.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 0.解析文件数据
    val inputData: DataStream[String] = env.readTextFile("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt")
    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
    })

    dataStream.addSink(new FlinkKafkaProducer[String]("localhoost:9092","sinktest",new SimpleStringSchema()))
  }
}
