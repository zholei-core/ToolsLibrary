package com.zho.kafkautils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object SparkAndFlinkConsumer {

}

class SparkAndFlinkConsumer {

  def sparkConsumer(): Unit = {

  }

  def flinkConsumer(): Unit = {
    // 隐式转换
    import org.apache.flink.api.scala._
    // 获取 Flink 的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //    env.setStateBackend(new RocksDBStateBackend("hdfs://path", true))
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop321:9092")
    props.setProperty("group.id", "con1")
    val myConsumer = new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), props)
    myConsumer.setStartFromGroupOffsets() // 默认消费策略
    val text = env.addSource(myConsumer)
    text.print()
    env.execute(this.getClass.getSimpleName.stripPrefix("$"))
  }

}