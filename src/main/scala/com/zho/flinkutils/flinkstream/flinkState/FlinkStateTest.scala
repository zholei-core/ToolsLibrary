package com.zho.flinkutils.flinkstream.flinkState

import com.zho.flinkutils.flinkstream.flinksource.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class FlinkStateTest {

}

object FlinkStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputData = env.socketTextStream("localhost", 9999)

    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
    // 需求：对于温度传感器温度值跳变，超过10度  报警
    val alertStream = dataStream
      .keyBy(_.id)
      .flatMap(new TempChangeAlert(10.0))

    alertStream.print()

    env.execute("FlinkState Test")
  }

  // 实现自定义RichFlatMapFunction
  class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading,(String, Double, Double)]() {
    // 定义状态保存上一次的温度值
    lazy val lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))

    override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
      // 获取上一次的温度值
      val lastTemp = lastTempState.value()
      // 跟最新的温度值求差值作比较
      val diff = (in.temperature - lastTemp).abs
      if (diff > threshold) {
        collector.collect((in.id, lastTemp, in.temperature))
      }
      // 更新状态
      lastTempState.update(in.temperature)
    }
  }
}