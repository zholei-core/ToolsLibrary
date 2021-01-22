package com.zho.kafkautils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

object SparkAndFlinkConsumer {

}

class SparkAndFlinkConsumer {
/*

  def sparkConsumer(): Unit = {
    val ssc = new StreamingContext(new SparkConf().setMaster("local[2]").setAppName("Test"), Seconds(2))
    // 创建 kafka 配置参数集合
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "group.id" -> "use_group_id_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> false,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )
    // 创建 Topic 集合
    val topics = Array("topicA", "topicB")


    val fromOffsets: Map[TopicPartition, Long] = Map[TopicPartition, Long](new TopicPartition("zho_test", 1) -> 324L)
    var consumerStrategy: ConsumerStrategy[String, String] = null

    // 若读取到 偏移量则根据 消费的偏移量位置消费，否则从当前位置消费
    if (fromOffsets.nonEmpty)
      consumerStrategy = ConsumerStrategies.Subscribe(topics, kafkaParams, fromOffsets)
    else
      consumerStrategy = ConsumerStrategies.Subscribe(topics, kafkaParams)

    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy
    )
    stream.foreachRDD(RDD=>{

      RDD.foreachPartition(line=>{

        line.foreach(line=>{
          line.topic()
          line.partition()
          line.offset()
          line.value()
        })
      })

      // 业务操作后，再进行偏移量保存
      val offsetRanges: Array[OffsetRange] = RDD.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(range=>{
        /*
        *@param topic Kafka主题名
        *@param partition卡夫卡分区id
        *@param fromOffset包含起始偏移量
        *@param untilOffset独占结束偏移量
         */
//        range.topic
//        range.partition
//        range.fromOffset
//        range.untilOffset
      })
    })
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
*/

}