package com.zho.esutils

import java.lang

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}

object RestGetRequestExistsUtil {

}

class RestGetRequestExistsUtil {

  /**
   * 判断 查询的 ES 索引数据 是否存在
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
    val syncResponse = client.exists(getRequest, RequestOptions.DEFAULT)
    // 获取Json 类型的数据 ，并打印输出
    val jsonStr = syncResponse
    println(jsonStr)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/

    client.existsAsync(getRequest, RequestOptions.DEFAULT, new ActionListener[lang.Boolean] {
      // 执行成功完成时调用
      override def onResponse(response: lang.Boolean): Unit = {
        // Return: True Or False
        println(response)
      }

      // 当失败时调用
      override def onFailure(e: Exception): Unit = {
        client.close()
      }
    }
    )

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/

  }
}
