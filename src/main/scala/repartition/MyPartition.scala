package repartition

import org.apache.spark.Partitioner

class MyPartition(numPartition: Int) extends Partitioner{

  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ =>
      val rawMod = key.hashCode()%numPartitions
      rawMod + (if(rawMod<0) numPartitions else 0)
  }

}
