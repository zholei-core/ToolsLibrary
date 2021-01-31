package com.zho.esutils

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.{ClearScrollRequest, MultiSearchRequest, MultiSearchResponse, SearchRequest, SearchResponse, SearchScrollRequest}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.Scroll
import org.elasticsearch.search.aggregations.{AggregationBuilder, AggregationBuilders}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder

object RestSearchRequestUtil {

}

class RestSearchRequestUtil {

  /**
   * 批量查询全部索引
   *
   * @param client 客户端连接
   */
  def searchRequestData(client: RestHighLevelClient): Unit = {

    val searchRequest = new SearchRequest()
    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.query(QueryBuilders.matchAllQuery())
    searchRequest.source(searchSourceBuilder)

    /** ************************* ES 同步执行【Synchronous】 Start ********************************/
    client.search(searchRequest, RequestOptions.DEFAULT)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/
    client.searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener[SearchResponse] {
      override def onResponse(response: SearchResponse): Unit = {

      }

      override def onFailure(e: Exception): Unit = {

      }
    })

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/
  }

  /**
   * 滚动获取 ES 中的数据
   *
   * @param client 客户端连接
   */
  def searchScrollRequestData(client: RestHighLevelClient): Unit = {

    // Step One :
    val searchRequest = new SearchRequest("index_post_zl")
    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.query(QueryBuilders.matchQuery("map_user", "Ralph"))
    searchSourceBuilder.size(5) // 一次检索的数据量
    searchRequest.source(searchSourceBuilder)
    searchRequest.scroll(TimeValue.timeValueMinutes(1L)) // 设置滚动间隔

    val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
    var scrollId = searchResponse.getScrollId
    var searchHits = searchResponse.getHits.getHits

    // Step Two : 此处是循环操作
    while (searchHits != null && searchHits.length > 0) {

      val searchScrollRequest = new SearchScrollRequest(scrollId)
      searchScrollRequest.scroll(TimeValue.timeValueMinutes(1L))
      val searchScrollResponse = client.scroll(searchScrollRequest, RequestOptions.DEFAULT)
      scrollId = searchScrollResponse.getScrollId
      searchHits = searchScrollResponse.getHits.getHits

    }

    // Step Three : 滚动完成后，清楚滚动上下文
    val clearScrollRequest = new ClearScrollRequest()
    clearScrollRequest.addScrollId(scrollId)
    val clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT)
    clearScrollResponse.isSucceeded

  }

  /**
   * 批量 Ssearch 查询
   *
   * @param client 客户端连接
   */
  def multiSearchRequestData(client: RestHighLevelClient): Unit = {

    val multiSearchRequest = new MultiSearchRequest()
    val searchSourceBuilder = new SearchSourceBuilder()

    val searchRequestFirst = new SearchRequest()
    searchSourceBuilder.query(QueryBuilders.matchQuery("", ""))
    searchRequestFirst.source(searchSourceBuilder)
    multiSearchRequest.add(searchRequestFirst)

    val searchRequestSecond = new SearchRequest()
    searchSourceBuilder.query(QueryBuilders.matchQuery("", ""))
    searchRequestSecond.source(searchSourceBuilder)
    multiSearchRequest.add(searchRequestSecond)

    /** ************************* ES 同步执行【Synchronous】 Start ********************************/
    client.msearch(multiSearchRequest, RequestOptions.DEFAULT)

    /** ************************* ES 同步执行【Synchronous】 End ********************************/
    /** ************************* ES 异步执行【Asynchronous】 Start ********************************/
    client.msearchAsync(multiSearchRequest, RequestOptions.DEFAULT, new ActionListener[MultiSearchResponse] {
      override def onResponse(response: MultiSearchResponse): Unit = {

      }

      override def onFailure(e: Exception): Unit = {

      }
    })

    /** ************************* ES 异步执行【Asynchronous】 End ********************************/


  }

  /**
   * 高亮 突出显示结果
   *
   * @param client 客户端连接
   */
  def searchHighLightBuilderData(client: RestHighLevelClient): Unit = {
    val searchRequest = new SearchRequest()

    val searchSourceBuilder = new SearchSourceBuilder()
    val highLightBuilder = new HighlightBuilder()

    // 设置突出显示 属性
    val highlightTitle: HighlightBuilder.Field = new HighlightBuilder.Field("title")
    highlightTitle.highlighterType("unified")
    highLightBuilder.field(highlightTitle)

    val highLightUser = new HighlightBuilder.Field("user")
    highLightBuilder.field(highLightUser)

    // 突出显示属性 添加到 SearchSourceBuilder
    searchSourceBuilder.highlighter(highLightBuilder)

    // SearchSourceBuilder 添加到 searchRequest 中
    searchRequest.source(searchSourceBuilder)
  }

  /**
   * 聚合类 查询操作
   *
   * @param client 客户端连接
   */
  def searchcAggregationBuilderData(client: RestHighLevelClient): Unit = {
    val searchRequest = new SearchRequest()

    val searchSourceBuilder = new SearchSourceBuilder()
    val aggregationBuilder = AggregationBuilders.terms("")
    searchSourceBuilder.aggregation(aggregationBuilder)
    searchRequest.source(searchSourceBuilder)

    client.search(searchRequest, RequestOptions.DEFAULT)
  }

}
