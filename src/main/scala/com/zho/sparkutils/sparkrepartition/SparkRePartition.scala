package com.zho.sparkutils.sparkrepartition

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Author:zholei
 * Date:2021-01-19
 *
 * 需求：根据自定义 RDDKEY  调用自定义重分区类，进行 数据重分区操作
 * 效果：相同数据类型，在同一分区中
 */
object SparkRePartition {

  def main(args: Array[String]): Unit = {
    // 初始化 SparkContext 对象
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local[2]"))
    // 设置 SparkContext 日志输出 级别
    sc.setLogLevel("ERROR")
    // 通过 SparkContext 的 makeRDD 方法，创建 RDD
    val rdd = sc.makeRDD(List(("AAA", 1), ("BBB", 2), ("BBB", 2), ("AAA", 2), ("EEE", 2)))
    // 获取算子 默认的分区数
    val partitionsLen = rdd.partitions.length
    // 根据自定义类，对RDD进行重分区 ，并传入分区数量
    val parRdd = rdd.partitionBy(new MyPartition(partitionsLen))
    // 将数据按照分区粒度进行重分区操作
    parRdd.partitions.foreach(elem => {
      // 获取每个分区的索引 ID
      val index: Int = elem.index
      // 返回对应分区的数据
      val partitionData = parRdd.mapPartitionsWithIndex((pid, iter) => {
        if (index == pid) iter else Iterator()
      })
      println("分区索引ID : " + index + ",elem : " + partitionData.collect().toList)
    })
    // RDD 数据写入到文件
    //    parRdd.saveAsTextFile("output")


  }

}
