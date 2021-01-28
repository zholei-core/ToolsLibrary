package com.zho.esutils

import java.util
import java.util.Date

import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.{XContentFactory, XContentType}

object RestIndexRequestUtil {

}

/**
 * 1、JSON 数据格式 存入 ES
 * 2、Map 数据格式 存入 ES
 * 3、XContentBuilder 数据格式 存入 ES
 */
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
    /*
    此处 Map 集合 采用 util 类库下的 HashMap 集合 只有这个方案 可以通过 Jackson 转换为 JSON
    Scala 的Map 集合 不支持 Jackson 转换，未到找 其对应的解决方案
    */
    val mapData = new util.HashMap[String, Object]()
    mapData.put("map_user", "Ralph")
    mapData.put("map_post_date", "2020-01-28")
    //  hashMap 集合 转换为 JSON 数据
    val jackson = new ObjectMapper()
    val mapToJsonStr = jackson.writeValueAsString(mapData)
    indexRequest.source(mapToJsonStr)

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

    /** ************************* ES 同步执行【Synchronous】 Start ********************************/
    val syncResponse = client.index(indexRequest, RequestOptions.DEFAULT)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/
    val aSyncResponse = client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener[IndexResponse] {
      // 执行成功完成时调用
      override def onResponse(response: IndexResponse): Unit = ???

      // 当整体IndexRequest失败时调用
      override def onFailure(e: Exception): Unit = ???
    })

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/
  }
}
