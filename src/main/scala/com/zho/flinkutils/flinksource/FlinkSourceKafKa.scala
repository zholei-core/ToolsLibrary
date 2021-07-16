package com.zho.flinkutils.flinksource

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

class FlinkSourceKafKa {

}

object FlinkSourceKafKa {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "con1")
    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), props))
    kafkaStream.print()
    env.execute("KafKa Source")
  }
}
