package com.zho.esutils

import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestHighLevelClient}

object InitRestHighClient {
  /**
   * 通过 Rest 方案：初始化 ES 客户端连接
   *
   * @return RestHighLevelClient
   */
  def initConnection(): RestHighLevelClient = {
    new RestHighLevelClient(
      RestClient.builder(new HttpHost("ES_NODES", 9092, "ES_SCHEME"))
    )
  }
}
