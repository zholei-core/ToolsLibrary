package com.zho.flinkutils.flinksource

import org.apache.flink.streaming.api.scala._

class FlinkCustomSource {

}

object FlinkCustomSource {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val customStream = env.addSource(new MySensorSource())
    customStream.print()
    env.execute("Custom Source")
  }
}
