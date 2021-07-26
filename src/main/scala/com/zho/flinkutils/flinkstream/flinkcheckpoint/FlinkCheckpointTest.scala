package com.zho.flinkutils.flinkstream.flinkcheckpoint

import com.zho.flinkutils.flinkstream.flinksource.SensorReading
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
    // fallBackRestart - 当前不做处理，如果上层有重启机制 采用上层；
    // fixedDelayRestart - 固定时间间隔重启；
    // noRestart - 无重启策略；
    // failureRateRestart - 失败率重启
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
      // * 自定义 SavePoint 时， 需要设置 id 方便集群迁移 检查点重启，的唯一标识； 默认时根据算子随机的，如果算子更改 这个ID 也会随之更改，便不能 根据存储检查点 进行程序恢复 *
      .uid("1")
  }
}
