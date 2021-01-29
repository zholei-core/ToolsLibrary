package com.zho.esutils

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.{MultiGetRequest, MultiGetResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.search.fetch.subphase.FetchSourceContext

object RestMultiGetRequestUtil {

}

class RestMultiGetRequestUtil {

  def multiGetRequestUtilData(client: RestHighLevelClient): Unit = {

    val multiGetRequest = new MultiGetRequest()
    multiGetRequest.add(new MultiGetRequest.Item("index_post_zl", "doc", "1"))
    multiGetRequest.add(new MultiGetRequest.Item("index_post_zl", "doc", "2"))
    //禁用源检索，默认情况下启用
    multiGetRequest.add(
      new MultiGetRequest.Item("index_post_zl", "doc", "3")
        .fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE)
    )

    /** ************************* ES 同步执行【Synchronous】 Start ********************************/
    client.mget(multiGetRequest, RequestOptions.DEFAULT)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/
    client.mgetAsync(multiGetRequest, RequestOptions.DEFAULT, new ActionListener[MultiGetResponse] {
      override def onResponse(response: MultiGetResponse): Unit = {

      }

      override def onFailure(e: Exception): Unit = {

      }
    })

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/
  }
}