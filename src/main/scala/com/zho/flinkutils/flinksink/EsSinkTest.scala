package com.zho.flinkutils.flinksink

import com.zho.flinkutils.flinksource.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 0.解析文件数据
    val inputData: DataStream[String] = env.readTextFile("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt")
    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 定义httpHosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200))

    // 自定义写入es 的 myEsSinkFunc
    val myEsSinkFunc  = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {

        // 包装一个Map 作为Source
        val dataSource = new util.HashMap[String,String]()
        dataSource.put("id",t.id)
        dataSource.put("temperature",t.temperature.toString)
        dataSource.put("ts",t.timestamp.toString )

        // 创建 index request ,用于发送 http 请求
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("resding")
          .source(dataSource)

        // 用 indexer 发送请求
        requestIndexer.add(indexRequest)
      }
    }

    dataStream.addSink(
      new ElasticsearchSink.Builder[SensorReading](httpHosts,myEsSinkFunc).build()
    )
  }

}
