package com.zho.flinkutils.flinkprocessfunction

import com.zho.flinkutils.flinksource.SensorReading
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class FlinkProcessFuncTest {

}

object FlinkProcessFuncTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputData = env.socketTextStream("localhost", 9999)

    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    val warningStream = dataStream
      .keyBy(_.id)
      .process(new TempIncrWarning(10000L))

    warningStream.print()
    env.execute("process function test")

  }

  // 实现自定义的 KeyedProcessFunction
  class TempIncrWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {
    // 定义状态：保存上一个温度值进行比较，保存注册定时器的时间戳用于删除
    lazy val lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    lazy val timerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {

      // 先取出状态
      val lastTemp = lastTempState.value()
      val timerTs = timerTsState.value()

      // 更新温度值
      lastTempState.update(value.temperature)

      // 当前温度值和上次温度比较
      if (value.temperature > lastTemp && timerTs == 0) {
        // 如果温度上升，且没有定时器，那么就注册当前时间10s 后嗯定时器
        val ts = ctx.timerService().currentProcessingTime() + interval
        ctx.timerService().registerProcessingTimeTimer(ts)
        timerTsState.update(ts)
      }else if(value.temperature<lastTemp){
        ctx.timerService().deleteProcessingTimeTimer(timerTs)
        timerTsState.clear()
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器"+ctx.getCurrentKey+"的温度连续" + interval/1000 +"秒连续上升")
      timerTsState.clear()
    }
  }

  @deprecated("old - fashioned")
  class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {
    override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
      context.getCurrentKey
      context.timestamp()
      context.timerService().currentWatermark()
      context.timerService().registerEventTimeTimer(1L)
    }
  }
}
