package com.zho.flinkutils.flinkcore

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object WordCountBatch {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataPath = "/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/flink_data.txt"
    val inputData: DataSet[String] = env.readTextFile(dataPath)
    val resultData = inputData
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    resultData.print()
  }
}
