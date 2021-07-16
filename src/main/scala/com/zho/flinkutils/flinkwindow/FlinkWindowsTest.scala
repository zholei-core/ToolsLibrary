package com.zho.flinkutils.flinkwindow

import com.zho.flinkutils.flinksource.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

class FlinkWindowsTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 0.解析文件数据
    val inputData: DataStream[String] = env.readTextFile("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt")
    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    val resultStream = dataStream
      .map(data=>(data.id,data.temperature))
      .keyBy(_._1)
//      .window(TumblingEventTimeWindows.of(Time.seconds(15)))  //滚动时间窗口
//      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5))) // 滑动时间窗口
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))  // 会话窗口
      .timeWindow(Time.seconds(15))
  }

}
