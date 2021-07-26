package com.zho.flinkutils.flinkstream.flinkState

import com.zho.flinkutils.flinkstream.flinksource.SensorReading
import org.apache.flink.streaming.api.scala._

class FlinkStateTest2 {

}

object FlinkStateTest2 {
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
      .flatMapWithState[(String,Double,Double),Double] {
        case (data: SensorReading, None) => (List.empty, Some(data.temperature))
        case (data: SensorReading, lastTemp:Some[Double] ) =>
        {
          // 跟最新的温度值求差值作比较
          val diff = (data.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((data.id,lastTemp.get,data.temperature)),Some(data.temperature))
          }else{
            (List.empty,Some(data.temperature))
          }
        }
      }

    alertStream.print()

    env.execute("FlinkState Test")
  }
}

