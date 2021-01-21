package com.zho.redisutils

/**
 * redis分布式锁
 */
object RedisTool {

  //加锁是否成功标志
  val LOCK_SUCCESS:String = "OK"

  //即当key不存在时，我们进行set操作；若key已经存在，则不做任何操作；
  val SET_IF_NOT_EXIST:String = "NX"

  //意思是我们要给这个key加一个过期的设置，具体时间由第五个参数决定。
  val SET_WITH_EXPIRE_TIME:String = "PX"

  val RELEASE_SUCCESS:String = "1"

  /**
   *
   * @param lockKey   锁key
   * @param requestId  请求标识
   * @param expireTime  超期时间
   * @param isPersist  临时缓存或者永久缓存
   */
  def tryGetDistributedLock(lockKey:String, requestId:String, expireTime:Int,isPersist:Boolean=false): Unit = {
//    CacheManager.redisClientPool.withClient(
//      client => {
//        //val redisKeyPrefix = CacheManager.getRedisKeyPrefix(isPersist)
//        client.select(CacheManager.redisDBNum)
//        val result = client.set(lockKey, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expireTime)
//        var flag = false
//        if(LOCK_SUCCESS == result){
//          flag = true
//        }
//        flag
//      }
//    )
  }


  /**
   *释放分布式锁
   * @param lockKey   锁key
   * @param requestId  请求标识
   * @param expireTime  超期时间
   * @param isPersist  临时缓存或者永久缓存
   * @return
   */
  def releaseDistributedLock(lockKey:String, requestId:String,expireTime: Int = 10,isPersist:Boolean=false): Unit ={
//    CacheManager.redisClientPool.withClient(
//      client => {
//        val redisKeyPrefix = CacheManager.getRedisKeyPrefix(isPersist)
//        client.select(CacheManager.redisDBNum)
//        //lua脚本也是单例模式，同样也可以保证同一时刻只有一个线程执行脚本
//        val lua =
//          s"""
//             |local current = redis.call('incrBy',KEYS[1],ARGV[1]);
//             |if current == tonumber(ARGV[1]) then
//             |  local t = redis.call('ttl',KEYS[1]);
//             |  if t == -1 then
//             |    redis.call('expire',KEYS[1],ARGV[2])
//             |  end;
//             |end;
//             |return current;
//      """.stripMargin
//        val code = client.scriptLoad(lua).get
//        val ret = client.evalSHA(code, List(redisKeyPrefix + lockKey),List(requestId,expireTime))
//        val result = ret.get.asInstanceOf[Object].toString
//        var flag = false
//        if(result == RELEASE_SUCCESS){
//          flag = true
//        }
//        flag
//      }
//    )
  }

}
