package com.zho.flinkutils.flinkcore

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object WordCountStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // flink 自带传参
    val paramTool = ParameterTool.fromArgs(args)
    val local = paramTool.get("local")
    val post = paramTool.getInt("post")

    val inputData = env.socketTextStream("localhost", 9999)
    val resultData = inputData
      .flatMap(_.split(" ")).disableChaining()  // 不参与共享slot 合并
      .filter(_.nonEmpty).startNewChain()   //此算子一下 重新开启slot
      .map((_, 1)).slotSharingGroup("a")  // 独立共享组 默认值为 default
      .keyBy(0)
      .sum(1)

    val resultData1: KeyedStream[(String, Int), Tuple] = inputData
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)


    resultData.print()
    env.execute()
  }
}
