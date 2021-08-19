package com.zho.flinkutils.flinkstream.flinktransform

import org.apache.flink.streaming.api.scala._


case class TransformTest(
                          id: String, timestamp: Long, temperature: Double
                        )

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 0.解析文件数据
    val inputData: DataStream[String] = env.readTextFile("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt")
    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new TransformTest(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    // 1.聚合操作 对数据进行转换
    val aggStream = dataStream.keyBy(_.id)
      .minBy("temperature")

    // 2. 输出最小的温度值，最新的时间戳
    val resultStream = dataStream.keyBy(_.timestamp)
      .reduce((curState, newData) =>
        TransformTest(curState.id, newData.timestamp, curState.temperature.min(newData.temperature))
      )
    //    dataStream.print()

    // 3. 拆分流 操作
    val splitStream = dataStream.split(data =>
      if (data.temperature > 30.0) Seq("Heigh") else Seq("low")
    )
    val heighStream = splitStream.select("Heigh")
    val lowStream = splitStream.select("low")
    val allStream = splitStream.select("Heigh", "low")

    //    heighStream.print("Heigh")
    //    lowStream.print("low")
    //    allStream.print("all")

    // 4. 合并流 connect
    val lowStreamTuple = lowStream.map(data=>(data.id,data.temperature))
    val  connectedStream = heighStream.connect(lowStreamTuple)
    val coMapResultStream =connectedStream.map(
      warnData=>  (warnData.id,warnData.timestamp,"Warning")
        ,
        lowData=>(lowData._1,"Health")
    )

//    coMapResultStream.print("Connection Stream")

    // 5. 合并流 union
    val unionStream = heighStream.union(lowStream)
    unionStream.print("Union Stream")

    env.execute("Transform Test")
  }
}