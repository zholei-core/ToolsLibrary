package com.zho.esutils

import java.io.IOException

import com.zho.dynamicloadutils.DynamicPropsFileUtil
import org.apache.http.HttpHost
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.elasticsearch.client.indices.{GetMappingsRequest, GetMappingsResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.cluster.metadata.MappingMetaData
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * Author:zholei
 * Date:2021-01-26
 * Description:
 * 读取配置文件并获取 ES 连接信息
 * 通过 RestHighLevelClient API 方式 获取ES 全量 索引 Mappings 信息
 * 程序产出 ，ES 索引名称 与索引对应的 索引列信息
 */
object RestGetMappingsRequestUtil extends App {
  // 程序初始化 ， 加载配置文件
  DynamicPropsFileUtil.getProperties("PROJECT")

  // 初始化连接， 调用 解析Mappings 方法
  val esHighClientUtils = new RestGetMappingsRequestUtil()
  esHighClientUtils.getEsMappingsData(InitRestHighClient.initConnection())
}

class RestGetMappingsRequestUtil {
  val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)


  /** ******************************* GetMappingsRequest Start ************************************************/
  /**
   * 通过 算子 解析 ES 中索引【Mappings】列的集合
   *
   * @param client ES 连接客户端
   * @return Map[索引名称，List[索引对应的列信息] ]
   */
  def getEsMappingsData(client: RestHighLevelClient): mutable.Map[String, List[String]] = {

    var idxNameAndColsMap = mutable.Map.empty[String, List[String]]
    try {
      // 初始化 MappingsRequest 请求
      val request = new GetMappingsRequest()
      // 根据 请求 返回结果集
      val response = client.indices().getMapping(request, RequestOptions.DEFAULT)
      // 获取 索引 mappings 信息
      // {dwd_cl_idx_casefolder=org.elasticsearch.cluster.metadata.MappingMetaData@11030b75,
      // dwd_cl_rp_totallosscost=org.elasticsearch.cluster.metadata.MappingMetaData@e649dd64}
      val mappings = response.mappings()
      // 获取 ES 中 所有 索引的 Key -> Array(dwd_cl_idx_casefolder, dwd_cl_rp_totallosscost)
      val mapkeys = mappings.keySet().toArray
      // 根据 索引名集合 获取对应 索引列信息
      mapkeys.foreach(idxKeyName => {
        // 根据索引名称 获取其属性 Mapping 信息
        val idxMapping = mappings.get(idxKeyName)

        // 在 Mapping 信息中 获取索引字段名，并转换为List 集合
        val idxColsInfo = idxMapping
          .sourceAsMap() // {properties={DATE={type=text, fields={keyword={ignore_above=256, type=keyword}}}, CASEFOLDERID={type=keyword}}}
          .values() // [{DATE={type=text, fields={keyword={ignore_above=256, type=keyword}}}, CASEFOLDERID={type=keyword}}]
          .toArray // 转换集合，去掉‘[]'  结构，行迭代操作
          .flatMap(_.toString.split("},")) // 将以上数据根据 【},】 进行切分 ,并 List[List[String]] 结构 扁平化为 List[String]
          .map(elem => {
            //将数据 根据 "=" 切分 取出第一个元素，并进行字节替换，获取最终列信息
            val idxColInfo = elem.split("=")(0)
            if (idxColInfo.contains("{")) idxColInfo.replace("{", "") else idxColInfo
          }).toList

        // 批量将数据存入Map 集合中  Map[String,List[String]]
        idxNameAndColsMap.+=((idxKeyName.toString, idxColsInfo))
        // 数据控制台 输出
        println(idxKeyName.toString + " --> " + idxColsInfo)
      })
    } catch {
      case io: IOException => logger.error("IOException", io)
      case e: Exception => logger.error("OtherException", e)
    } finally {
      if (client != null)
        client.close()
    }
    idxNameAndColsMap

  }

  /**
   * 通过 Jackson 解析 ES 中索引【Mappings】列的集合
   *
   * @param client ES 连接客户端
   * @return Map[索引名称，List[索引对应的列信息] ]
   */
  def getEsMappingsDataJackson(client: RestHighLevelClient): mutable.Map[String, List[String]] = {

    var idxNameAndColsMap = mutable.Map.empty[String, List[String]]
    val jackson = new ObjectMapper()
    try {

      val request: GetMappingsRequest = new GetMappingsRequest()
      val response: GetMappingsResponse = client.indices().getMapping(request, RequestOptions.DEFAULT)
      val mappings = response.mappings()
      val mapkeys = mappings.keySet().toArray
      // 根据 索引名集合 获取对应 索引列信息
      mapkeys.foreach(idxKeyName => {
        // 根据索引名称 获取其属性 Mapping 信息
        val idxMapping: MappingMetaData = mappings.get(idxKeyName)

        // 将  Mapping 信息 通过 Jackson 转换为Json  , 并获取列信息集合
        val idxColsInfo = idxMapping.source().toString
        val jsonNode = jackson.readTree(idxColsInfo)
        val propsColsInfo: JsonNode = jsonNode.path("properties")
        val idxColsInfoList = propsColsInfo.getFieldNames.asScala.toList

        // 批量将数据存入Map 集合中  Map[String,List[String]]
        idxNameAndColsMap.+=((idxKeyName.toString, idxColsInfoList))
        // 数据控制台 输出
        //        println(idxKeyName.toString + " --> " + idxColsInfo)
      })
    } catch {
      case io: IOException => logger.error("IOJacksonException", io)
      case e: Exception => logger.error("OtherJacksonException", e)
    } finally {
      if (client != null)
        client.close()
    }
    idxNameAndColsMap

  }

  /** ******************************* GetMappingsRequest End ************************************************/

}
