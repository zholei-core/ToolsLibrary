package com.zho.flinkutils.flinksink

import com.zho.flinkutils.flinksource.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object FileSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 0.解析文件数据
    val inputData: DataStream[String] = env.readTextFile("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt")
    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    dataStream.print()

    dataStream.writeAsCsv("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )
  }
}
