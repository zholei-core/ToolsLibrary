package com.zho.flinkutils.flinkstream.flinksource


import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random


class MySensorSource extends SourceFunction[SensorReading] {


  //定义一个标志位 flag ，表示 数据源是否正常发出数据
  var running: Boolean = true

  override def cancel(): Unit = running = false

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    // 随机生成一组（10个）传感器的初始温度
    var curTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))
    // 定义无限循环，不停的产生数据，除非被 cancel
    while (running) {
      // 在上次数据基础上，更新温度值
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      val curTime: Long = System.currentTimeMillis()
      curTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2.formatted("%.2f").toDouble))

      )
      //间隔100ms
      Thread.sleep(500)
    }

  }

}
