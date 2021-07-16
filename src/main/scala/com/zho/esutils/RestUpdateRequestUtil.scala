package com.zho.esutils

import java.util
import java.util.Date

import org.apache.commons.collections.map.SingletonMap
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.{XContentFactory, XContentType}
import org.elasticsearch.script.{Script, ScriptType}

object RestUpdateRequestUtil {

}

class RestUpdateRequestUtil {

  def updateRequestData(client: RestHighLevelClient): Unit = {

    val updateRequest = new UpdateRequest("index_post_zl", "doc", "1")

    /** ************************* ES 根据 painless语言 Script 更新数据 Start ********************************/
    //    var singletonMap = new SingletonMap[String, Object]("count", 4)
    var singletonMap = new util.HashMap[String, Object]()
//    singletonMap.put("count", 4)
    val inLine = new Script(ScriptType.INLINE, "painless", "ctx._source.field += params.count", singletonMap)
    // or
    //    val inLine = new Script(ScriptType.INLINE, null, "ctx._source.field += params.count", singletonMap)
    updateRequest.script(inLine)

    /** ************************* ES 根据 painless语言 Script 更新数据 End ********************************/
    /** ************************* ES 根据 JSON 更新数据 Start ********************************/
    val jsonString =
      """
        |{"user":"Ralph","post_date":"2020-01-28"}
        |""".stripMargin
    updateRequest.doc(jsonString, XContentType.JSON)

    /** ************************* ES 根据 JSON 更新数据 End ********************************/
    /** ************************* ES 根据 Map 更新数据 Start ********************************/
    val mapData = new util.HashMap[String, Object]()
    mapData.put("map_user", "Ralph")
    mapData.put("map_post_date", "2020-01-29")
    updateRequest.doc(mapData)

    /** ************************* ES 根据 Map 更新数据 End ********************************/
    /** ************************* ES 根据 XContentBuilder 更新数据 Start ******************/
    val builder = XContentFactory.jsonBuilder()
    builder.startObject()
    builder.field("xcb_user", "Ralphs")
    builder.timeField("xcd_post_date", new Date())
    builder.endObject()
    updateRequest.doc(builder)

    /** ************************* ES 根据 MapXContentBuilder 更新数据 End *****************/
    /** ************************* ES 根据 Object 更新数据 Start ***************************/
    updateRequest.doc(
      "", "",
      "", "")

    /** ************************* ES 根据 Object 更新数据 End ***************************/

    /*--------------------------- ES 根据 Upsert 方案 更新数据 Start --------------------------*/
    val upsertJsonStr =
    """
      |{"user":"Ralph","post_date":"2020-01-28"}
      |""".stripMargin
    // 如果文档尚不存在，则可以使用以下upsert方法定义一些内容，这些内容将作为新文档插入
    updateRequest.upsert(upsertJsonStr, XContentType.JSON)

    /*--------------------------- ES 根据 Upsert 方案 更新数据 End --------------------------*/

    /** ************************* ES 同步执行【Synchronous】 Start ********************************/
    val syncUpdateResponse = client.update(updateRequest, RequestOptions.DEFAULT)
    println(syncUpdateResponse.getIndex)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/
    client.updateAsync(updateRequest, RequestOptions.DEFAULT, new ActionListener[UpdateResponse] {
      // 执行成功完成时调用
      override def onResponse(aSyncUpdateResponse: UpdateResponse): Unit = {
        println(aSyncUpdateResponse.getIndex)
        client.close()
      }

      // 当失败时调用
      override def onFailure(e: Exception): Unit = {
        client.close()
      }
    })

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/
  }
}
