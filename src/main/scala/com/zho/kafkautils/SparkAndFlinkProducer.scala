package com.zho.kafkautils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object SparkAndFlinkProducer{

}

class SparkAndFlinkProducer {
/*

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
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("hadoop321", 9001, '\n')

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", 0)
    props.put("linger.ms", 1)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 这个构造函数不支持自定义语义
    //    val myProducer = new FlinkKafkaProducer[String](topic, new SimpleStringSchema(), prop)
    //注意：在使用Exactly-once 语义的时候执行代码会报错，提示 The transaction timeout is larger than the maximum value
    // allowed by the brock,因为Kafka 服务中默认事务的超时时间是15min ，但是 FlinkKafkaProducer 中的任务超时时间魔人是 1h。
    // 第一种解决方案，设置 FlinkKafkaProducer 中的事务超时时间
    //    prop.setProperty("transaction.timeout.ms", 60000 * 15 + "")
    // 第二种解决方案，修改Kafka的server.properties配置文件，设置Kafka的事务超时时间，修改后需要重启Kafka集群服务

    // 这个构造函数支持自定义语义，使用Exactly-once语义的Kafka Producer
    val myProducer = new FlinkKafkaProducer[String](
      "topic",
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      props,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

    text.addSink(myProducer)
    env.execute(this.getClass.getSimpleName.stripPrefix("$"))
  }

*/

}
