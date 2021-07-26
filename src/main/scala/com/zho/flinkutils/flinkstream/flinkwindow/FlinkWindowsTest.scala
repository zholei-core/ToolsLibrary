package com.zho.flinkutils.flinkstream.flinkwindow

import com.zho.flinkutils.flinkstream.flinksource.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object FlinkWindowsTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 时间语义
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 0.解析文件数据
    val inputData: DataStream[String] = env.readTextFile("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt")
    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    val resultStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      //      .window(TumblingEventTimeWindows.of(Time.seconds(15)))  //滚动时间窗口
      //      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5))) // 滑动时间窗口
      //      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))  // 会话窗口
      .timeWindow(Time.seconds(15))
    val value = resultStream.reduce((r1, r2) => (r1._1, r1._2.min(r2._2)))

    value.print()
    env.execute()
  }

}
