package com.zho.esutils

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.reindex.DeleteByQueryRequest

object RestDeleteRequestUtil {

}

class RestDeleteRequestUtil {

  /**
   * 删除 ES 中的 指定数据
   *
   * @param client RestHighLevelClient
   */
  def deleteRequestData(client: RestHighLevelClient): Unit = {

    val deleteRequest = new DeleteRequest("index_post_zl", "doc", "1")

    /** ************************* ES 同步执行【Synchronous】 Start ********************************/
    val syncDeleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT)
    println(syncDeleteResponse.getIndex)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/
    client.deleteAsync(deleteRequest, RequestOptions.DEFAULT, new ActionListener[DeleteResponse] {
      override def onResponse(aSyncDeleteResponse: DeleteResponse): Unit = {
        println(aSyncDeleteResponse.getIndex)
        client.close()
      }

      override def onFailure(e: Exception): Unit = {
        client.close()
      }
    })

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/
  }

  def deleteByQueryRequest(client: RestHighLevelClient): Unit ={

    val bulkRequest = new BulkRequest()
    val deleteByqueryRequest = new DeleteByQueryRequest()

    deleteByqueryRequest.indices("")
    deleteByqueryRequest.setQuery(QueryBuilders.termsQuery("",""))
//    bulkRequest.add(deleteByqueryRequest)
  }
}