package com.zho.flinkutils.flinkprocessfunction

import com.zho.flinkutils.flinksource.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlinkSideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputData = env.socketTextStream("localhost", 9999)

    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    val highTempStream = dataStream
      .process(new SplitTempProcessor(30.0))

    highTempStream.print("high")

    highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")
    env.execute("Split Stream")
  }

  // 实现自定义 ProcessFunction, 进行分流
  class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading] {
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {

      if (value.temperature > threshold) {
        // 如果当前温度 大于 30 ，那么输出到主流
        out.collect(value)
      } else {
        // 如果不超过30 ，那么输出到侧输出刘
        ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temperature))
      }
    }
  }
}
