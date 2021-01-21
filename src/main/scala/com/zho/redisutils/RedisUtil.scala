package com.zho.redisutils

import java.util

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{JedisPool, Response}

object RedisUtil {
  val password = ""
  val host = ""
  val port = ""
  val timeout = ""
}

class RedisUtil {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  private[this] var jedisPool: JedisPool = _

  // 初始化 jedis 连接池
  def init(host: String, port: Int, timeout: Int, password: String, database: Int = 0): Unit = {
    jedisPool = new JedisPool(new GenericObjectPoolConfig, host, port, timeout, password, database)
  }

  // 获取 String 类型的数据
  def get(key: Array[Byte]): Array[Byte] = {
    val jedis = jedisPool.getResource
    val result: Array[Byte] = jedis.get(key)
    jedis.close()
    result
  }

  // 存入 String 数据类型数据
  def set(key: Array[Byte], value: Array[Byte]): Boolean = {
    try {
      val jedis = jedisPool.getResource
      jedis.set(key, value)
      jedis.close()
      true
    } catch {
      case e: Exception =>
        logger.error(s"写入数据到Redis出错: $e")
        false
    }
  }

  def getCols(key: String,cols: Array[String] = Array.empty): Map[String, Array[Byte]] = {
    import scala.collection.JavaConverters._
    val jedis = jedisPool.getResource
    var map = Map.empty[String, Array[Byte]]
    if (cols.length > 0) {
      val pipe = jedis.pipelined()
      val response = pipe.hmget(key.getBytes(), cols.map(_.getBytes()): _*)
      pipe.sync()
      map = cols.zip(response.get.asScala).toMap.filter(x => x._2 != null)
      pipe.close()
    } else {
      logger.info(s"key: $key")
      val tmpMap: util.Map[Array[Byte], Array[Byte]] = jedis.hgetAll(key.getBytes())
      map = tmpMap.asScala.toMap.map(x => (new String(x._1), x._2))
    }
    jedis.close()
    map
  }

  def getCols2(key: String,cols: Array[String] = Array.empty): Map[String, Array[Byte]] = {
    val jedis = jedisPool.getResource
    var map = Map.empty[String, Array[Byte]]
    if (cols.length > 0) {
      for (col <- cols) {
        val value: Array[Byte] = jedis.hget(key.getBytes(), col.getBytes())
        if (null != value) {
          map = map + (col -> value)
        }
      }
    } else {
      logger.info(s"rowkey: $key")
      val tmpMap: util.Map[Array[Byte], Array[Byte]] = jedis.hgetAll(key.getBytes())
      import scala.collection.JavaConverters._
      map = tmpMap.asScala.toMap.map(x => (new String(x._1), x._2))
    }
    jedis.close()
    map
  }

  def bulkGetCols(keys: Array[String],cols: Array[String] = Array.empty): Map[String, Map[String, Array[Byte]]] = {
    import scala.collection.JavaConverters._
    var result: Map[String, Map[String, Array[Byte]]] = Map.empty
    val jedis = jedisPool.getResource
    val pipe = jedis.pipelined
    if (cols.length > 0) {
      val data = keys.map(x => {
        pipe.hmget(x.getBytes(), cols.map(_.getBytes()): _*)
      })

      pipe.sync()
      pipe.close()
      jedis.close()

      result = keys.zip(data.map(_.get().asScala.toArray).map(cols.zip(_).toMap.filter(null != _._2)))
        .toMap.filter(_._2.nonEmpty)
    } else {
      val data: Array[Response[util.Map[Array[Byte], Array[Byte]]]] = keys.map(x => {
        pipe.hgetAll(x.getBytes())
      })
      pipe.sync()
      pipe.close()
      jedis.close()

      result = keys.zip(data.map(_.get().asScala.map(x => (new String(x._1), x._2)).toMap))
        .toMap.filter(_._2.nonEmpty)
    }
    result
  }

  def bulkGetCols2(rowkeys: Array[String],cols: Array[String] = Array.empty): Map[String, Map[String, Array[Byte]]] = {
    val jedis = jedisPool.getResource
    var map = Map.empty[String, Map[String, Array[Byte]]]
    import scala.collection.JavaConverters._
    for (rowkey <- rowkeys) {
      var cellMap = Map.empty[String, Array[Byte]]
      if (cols.length > 0) {
        for (col <- cols) {
          val value = jedis.hget(rowkey.getBytes(), col.getBytes())
          if (null != value) {
            cellMap = cellMap + (col -> value)
          }
        }
      } else {
        logger.info(s"rowkey: $rowkey")
        val tmpMap = jedis.hgetAll(rowkey.getBytes())
        cellMap = tmpMap.asScala.toMap.map(x => (new String(x._1), x._2))
      }
      if (cellMap.nonEmpty) {
        map = map + (rowkey -> cellMap)
      }
    }
    jedis.close()
    map
  }

  def setCols(key: String,fieldValues: Map[String, String]): Unit = {
    import scala.collection.JavaConverters._
    val data = fieldValues.map(element => {
      (element._1.getBytes(), element._2.getBytes())
    }).asJava
    val jedis = jedisPool.getResource
    jedis.hmset(key.getBytes(), data)
    jedis.close()
  }
}
