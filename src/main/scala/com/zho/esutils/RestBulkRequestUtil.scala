package com.zho.esutils


import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest.OpType
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

object RestBulkRequestUtil {

}

/**
 * 批量提交 请求，进行数据处理
 */
class RestBulkRequestUtil {

  def bulkRequestData(client: RestHighLevelClient): Unit = {

    val bulkRequest = new BulkRequest()
    bulkRequest.add(
      new IndexRequest("posts", "doc", "4")
        .source(XContentType.JSON, "field", "baz")
    )
    bulkRequest.add(
      new UpdateRequest("posts", "doc", "2")
        .doc(XContentType.JSON, "other", "test")
    )
    bulkRequest.add(
      new DeleteRequest("posts", "doc", "3")
    )

    /** ************************* ES 同步执行【Synchronous】 Start ********************************/
    client.bulk(bulkRequest, RequestOptions.DEFAULT)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/
    client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener[BulkResponse] {
      override def onResponse(bulkResponse: BulkResponse): Unit = {
        client.close()
      }

      override def onFailure(e: Exception): Unit = {
        client.close()
      }
    })

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/
  }
}