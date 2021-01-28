package com.zho.esutils

import java.util.Date

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.{XContentFactory, XContentType}

object RestIndexRequestUtil {

}

class RestIndexRequestUtil {

  /**
   * ES 中 插入数据【单条】
   *
   * @param client RestHighLevelClient
   */
  def indexRequestData(client: RestHighLevelClient): Unit = {

    val indexRequest = new IndexRequest()
    // index（文档名）/type（文档类型）/id（文档Id）  此三个参数可以直接传入 IndexRequest 中使用
    indexRequest.index("index_post_zl")
    indexRequest.`type`("doc")
    indexRequest.id("1")

    /** ********************** JSON 数据格式 存入 ES Start ****************************************/
    val jsonString =
      """
        |{"user":"Ralph","post_date":"2020-01-28"}
        |""".stripMargin
    indexRequest.source(jsonString, XContentType.JSON)

    /** ********************** JSON 数据格式 存入 ES End *****************************************/
    /** ********************** Map 数据格式 存入 ES Start ****************************************/
    import scala.collection.mutable
    val mapData = mutable.Map.empty[String, Object]
    mapData.+=(("map_user", "Ralph"))
    mapData.+=(("map_post_date", "2020-01-28"))
    indexRequest.source(mapData)

    /** ********************** Map 数据格式 存入 ES End ****************************************/
    /** ********************** XContentBuilder 数据格式 存入 ES Start **************************/
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()
    builder.field("xcb_user", "Ralph")
    builder.timeField("xcd_post_date", new Date())
    builder.endObject()
    indexRequest.source(builder)

    /** ********************** XContentBuilder 数据格式 存入 ES End **************************/
    /** ************************* Object 数据格式 存入 ES Start ********************************/
    indexRequest.source(
      "", "",
      "", "",
      "", "",
      "", "")

    /** ************************* Object 数据格式 存入 ES End ********************************/

    /*************************** ES 同步执行【Synchronous】 Start ********************************/
    val syncResponse = client.index(indexRequest, RequestOptions.DEFAULT)

    /*************************** ES 同步执行【Synchronous】 End ********************************/
    /*************************** ES 异步执行【Asynchronous】 Start ********************************/
    val aSyncResponse = client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener[IndexResponse] {
      // 执行成功完成时调用
      override def onResponse(response: IndexResponse): Unit = ???

      // 当整体IndexRequest失败时调用
      override def onFailure(e: Exception): Unit = ???
    })

    /*************************** ES 异步执行【Asynchronous】 End ********************************/
  }
}
