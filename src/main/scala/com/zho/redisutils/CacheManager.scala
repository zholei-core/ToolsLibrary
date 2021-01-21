package com.zho.redisutils


/**
 *
 */
object CacheManager {

  val redisClientPool: Unit = "dev".equalsIgnoreCase(System.getenv("SCALA_ENV")) match {
    //开发环境
    case true => // new RedisClientPool("127.0.0.1", 6379)
    //其他环境
    case false => // new RedisClientPool("10.180.x.y", 6379, 8, 0, Some("root"))
  }

  val redisDBNum = 10

  def getRedisKeyPrefix(isPersist:Boolean): String ={
    if(isPersist){
      //永久缓存前缀
      "persist_"
    }else{
      //临时缓存前缀
      "tmp_"
    }
  }

}
