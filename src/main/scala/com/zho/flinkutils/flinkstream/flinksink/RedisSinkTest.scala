package com.zho.flinkutils.flinkstream.flinksink

//import com.zho.flinkutils.flinkstream.flinksource.SensorReading
//import com.zho.flinkutils.flinkstream.flinktransform.TransformTest
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.redis.RedisSink
//import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
//import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)

    // 0.解析文件数据
    //    val inputData: DataStream[String] = env.readTextFile("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt")
    //    val dataStream = inputData.map(dataElem => {
    //      val arr = dataElem.split(",")
    //      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    //    })

    // 定义一个FlinkJedisConfigBase
    //    val config = new FlinkJedisPoolConfig.Builder()
    //      .setHost("localhost")
    //      .setPort(6379)
    //      .build()
    //
    //    dataStream.addSink(new RedisSink[SensorReading](config, new MyRedisMapper))
  }
}

//// 定义一个RedisMapper
//class MyRedisMapper extends RedisMapper[SensorReading] {
//  // 定义保存写入redis 数据的命令
//  override def getCommandDescription: RedisCommandDescription ={
//    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
//  }
//
//  override def getKeyFromData(t: SensorReading): String = t.temperature.toString
//
//  override def getValueFromData(t: SensorReading): String = t.id
//}