package com.zho.flinkutils.flinkcheckpoint

import com.zho.flinkutils.flinksource.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit

object FlinkCheckpointTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 容错检查点设置
    env.enableCheckpointing(1000L) // 检查点间隔
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 精确消费
    env.getCheckpointConfig.setCheckpointTimeout(60000L) // 延迟时间
    // 以下两个配置 二选其一
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2) // 当前最大的Checkpoint
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500l) // Checkpoint 最小间隔时间
    // --------------------
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true) // 默认是 false ，是否用checkpoint 做故障回复
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2) // 能容忍 checkpoint 的失败次数

    // 重启策略配置
    /**
     * 失败重启
     * first : 失败重启次数
     * second : 失败重启时间间隔
     */
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 500L))

    /**
     * 失败率
     * first： 失败重试次数
     * second: 在此时间段内
     * third: 重试时间间隔
     */
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))

    // dataSource Reading
    val inputData = env.socketTextStream("localhost", 9999)
    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
  }
}
