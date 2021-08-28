package com.zho.sparkutils.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataFrameCreate {

    def main(args: Array[String]): Unit = {
      val session = SparkSession
        .builder()
        .appName(this.getClass.getSimpleName)
        .master("local[2]")
        .getOrCreate()
      val nameRDD: RDD[String] = session.sparkContext.makeRDD(Array(
        """
          |{"name":"zhangsan","age":18}
      """.stripMargin
        ,
        """
          |{"name":"lisi","age":11}
      """.stripMargin
      ))
      val scoreRDD: RDD[String] = session.sparkContext.makeRDD(Array(
        """
          |{"name":"zhangsan","score":100}
      """.stripMargin
        ,
        """
          |{"name":"lisi","score":200}
      """.stripMargin
      ))
      val nameDF: DataFrame = session.read.json(nameRDD)
      val scoreDF: DataFrame = session.read.json(scoreRDD)
      nameDF.createOrReplaceTempView("name")
      scoreDF.createOrReplaceTempView("score")
      val result = session.sql("select name.name,name.age,score.score from name,score where name.name = score.name")
      result.show()
    }

}
