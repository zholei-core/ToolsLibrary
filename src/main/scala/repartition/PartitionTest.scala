package repartition

import org.apache.spark.{Partition, SparkConf, SparkContext}


object PartitionTest {

  //自定义分区
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val rdd = sc.makeRDD(List(("AAA", 1), ("BBB", 2), ("BBB", 2), ("AAA", 2), ("EEE", 2)))
    val partitionsLen = rdd.partitions.length
    val parRdd = rdd.partitionBy(new MyPartition(partitionsLen))

    parRdd.partitions.foreach (elem => {
      val index: Int = elem.index
      val me = parRdd.mapPartitionsWithIndex ((pid, iter) => {
      if (index == pid) iter else Iterator ()
      })
      println ("分区索引ID : " + index + ",elem : " + me.collect ().toList)
      })

      //    parRdd.saveAsTextFile("output")


      }

}
