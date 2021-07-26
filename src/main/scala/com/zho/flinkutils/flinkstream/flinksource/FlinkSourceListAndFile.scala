package com.zho.flinkutils.flinkstream.flinksource

import org.apache.flink.streaming.api.scala._

case class FlinkSourceListAndFile(
                            adreess: String, timedate: String, temperature: Double
                          )

object FlinkSourceListAndFile {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1、读取集合中的数据
    val dataList = List(
      FlinkSourceListAndFile("beijing", "12345433356", 37.5)
    )
    val streamData: DataStream[FlinkSourceListAndFile] = env.fromCollection(dataList)
    // 传入各种类型的数据
    //    val streamData: DataStream[Any] = env.fromElements(1,"Hello",37.2)

    streamData.print("-------------------------- Read List: ").setParallelism(1)

    // 2、读取文件中的数据
    val inputPath = "/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt"
    val stream2 = env.readTextFile(inputPath)
    stream2.print("-------------------------- Read File: ").setParallelism(1)
    env.execute("Source List")
  }
}
