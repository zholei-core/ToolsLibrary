package com.zho.sparkutils.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions

case class Average(var sum: Long, var count: Long)

object MyAverage extends Aggregator[Long, Average, Double] {
  override def zero: Average = Average(0L, 0L)

  override def reduce(b: Average, a: Long): Average = {
    b.sum += a
    b.count += 1
    b
  }

  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  override def bufferEncoder: Encoder[Average] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object UDTFCreate {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[2]").getOrCreate()

    session.udf.register("myAverage", functions.udaf(MyAverage))

    readTextFileTransRDDAddSchemaToDataFrame(session).createOrReplaceTempView("employees")
    session.sql("select myAverage(age) as avg_age from employees").show(false)


  }

  /**
   * 自定义 Schema 创建 DataFrame  -> StructType
   *
   * @param session SparkSession
   */
  def readTextFileTransRDDAddSchemaToDataFrame(session: SparkSession): DataFrame = {

    val peopleRDD: RDD[String] = session.sparkContext.textFile("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/data.txt")

    // 创建 Schema 结构信息
    val schemaString = "no name age"
    val fields: Array[StructField] = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema: StructType = StructType(fields)

    // 创建 RDD【Row】 数据
    val rowRDD: RDD[Row] = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1), attributes(2)))

    session.createDataFrame(rowRDD, schema)

  }
}
