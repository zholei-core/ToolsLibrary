package com.zho.kafkautils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SparkAndFlinkProducer extends App {

}

class SparkAndFlinkProducer {

  def sparkProducer(): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", 0)
    props.put("linger.ms", 1)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    // ProducerRecord 参数详解
    // 1、topic:String 2、partition:Integer 3、timestamp:Long 4、key:String 5、value:String 6、headers:Iterable[header]
    // producer.send 参数详解
    // 1、record:ProducerRecord[String, String] 2、callBack:CallBack
    producer.send(new ProducerRecord[String, String]("topic", "testjson"))

    producer.close()
  }

  def flinkProducer(): Unit = {

  }


}
