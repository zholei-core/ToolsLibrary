package com.zho.sparkutils.sparkcore.sparkrepartition

import org.apache.spark.Partitioner

/**
 * Author:zholei
 * Date:2021-01-19
 * Description: 通过继承 Partitioner 类 ，根据自身需要 对数据 根据（hashCode）进行重新分区
 *
 * @param numPartition 源算子分区数 或 自定义分区数
 */
class MyPartition(numPartition: Int) extends Partitioner {

  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ =>
      val rawMod = key.hashCode() % numPartitions
      rawMod + (if (rawMod < 0) numPartitions else 0)
  }

}
