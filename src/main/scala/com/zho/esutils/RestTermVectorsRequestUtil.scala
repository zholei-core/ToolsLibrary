package com.zho.esutils

import org.elasticsearch.action.ActionListener
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.client.core.{TermVectorsRequest, TermVectorsResponse}
import org.elasticsearch.common.xcontent.XContentFactory

object RestTermVectorsRequestUtil {

}

class RestTermVectorsRequestUtil {

  /**
   * ES 向量操作 ApI
   *
   * @param client ES 连接客户端
   */
  def termVectorsRequestData(client: RestHighLevelClient): Unit = {

    val termVectorsRequest = new TermVectorsRequest("index_post_zl", "doc", "1")
    termVectorsRequest.setFields("vectors_user")

    val docBuilder = XContentFactory.jsonBuilder()
    docBuilder.startObject().field("vectors_user", "kim").endObject()

    /** ************************* ES 同步执行【Synchronous】 Start ********************************/
    client.termvectors(termVectorsRequest, RequestOptions.DEFAULT)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/
    client.termvectorsAsync(termVectorsRequest, RequestOptions.DEFAULT, new ActionListener[TermVectorsResponse] {
      override def onResponse(response: TermVectorsResponse): Unit = {

      }

      override def onFailure(e: Exception): Unit = {

      }
    })

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/
  }
}
