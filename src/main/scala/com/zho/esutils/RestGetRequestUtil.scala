package com.zho.esutils

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}

object RestGetRequestUtil {

}

class RestGetRequestUtil {

  /**
   * 单条查询 ES 中索引数据
   *
   * @param client RestHighLevelClient
   */
  def getRequestData(client: RestHighLevelClient): Unit = {
    val getRequest = new GetRequest()
    // index（文档名）/type（文档类型）/id（文档Id）  此三个参数可以直接传入 IndexRequest 中使用
    getRequest.index("index_post_zl")
    getRequest.`type`("doc")
    getRequest.id("1")

    /** ************************* ES 同步执行【Synchronous】 Start ********************************/
    val syncResponse = client.get(getRequest, RequestOptions.DEFAULT)
    // 获取Json 类型的数据 ，并打印输出
    val jsonStr = syncResponse.getSourceAsString
    println(jsonStr)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/

    client.getAsync(getRequest, RequestOptions.DEFAULT, new ActionListener[GetResponse] {
      // 执行成功完成时调用
      override def onResponse(response: GetResponse): Unit = {
        // 此方法中 对 ES中读取的数据 进行各种操作
        println(response.getSourceAsString)
      }

      // 当失败时调用
      override def onFailure(e: Exception): Unit = {
        client.close()
      }
    })

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/

  }
}
