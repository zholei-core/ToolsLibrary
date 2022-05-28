package com.zho.sparkutils.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataFrameCreate {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[2]").getOrCreate()

    val instan = udf((name: String) => {
       val result = "DB"+name.replaceAll("[a-zA-Z@_]+"," ").trim.replace(" ","-")
      result
    })
    //    readRddJsonToDataset(session)
    readTextFileTransRDDAddSchemaToDataFrame(session)
      .withColumn("name",instan(col("name")).cast("String"))
      .withColumnRenamed("name","instanceid")
      .show(false)

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

  /**
   * 用户样例类
   *
   * @param name 用户名
   * @param age  年龄
   */
  case class Person(name: String, age: String)

  /**
   * 样例类 通过隐式转换 转换为Dataset 进行数据处理
   *
   * @param session SparkSession
   */
  def readCaseClassToTransDataFrame(session: SparkSession): Unit = {

    import session.implicits._

    val objFrame: Dataset[Person] = Seq(Person("wangwu", "22")).toDS()
    objFrame.show(false)

    val seqFrame: Dataset[Int] = Seq(1, 2, 3).toDS()
    seqFrame.show(false)
  }

  /**
   * 读取json 文件 字符串路径  转换为DataFrame 进行数据操作  (path:String)  or  (path:String*)
   *
   * @param session SparkSession
   */
  def readJsonFileToDataFrame(session: SparkSession): Unit = {

    val jsonFrame: DataFrame = session.read.json(
      "/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/json1data.txt",
      "/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/json2data.txt"
    )

    jsonFrame.show(false)
  }

  /**
   * SparkContext makeRDD 将json 数组  转化为 RDD[String] , 隐士转化为 DataSet 通过 read.json 转化DF 进行数据操作
   *
   * @param session SparkSession
   */
  def readRddJsonToDataset(session: SparkSession): Unit = {

    import session.implicits._
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
    val nameDF: DataFrame = session.read.json(nameRDD.toDS())
    val scoreDF: DataFrame = session.read.json(scoreRDD.toDS())
    nameDF.createOrReplaceTempView("name")
    scoreDF.createOrReplaceTempView("score")
    val result = session.sql("select name.name,name.age,score.score from name,score where name.name = score.name")
    result.show()
  }

  /**
   * 创建 Dataset[String] JSON 数据  ， 通过 read.json  转化为DataFrame
   *
   * @param session SparkSession
   */
  def createDatasetToDataFrame(session: SparkSession): Unit = {

    import session.implicits._
    //    +----------------------------------+
    //    |value                             |
    //    +----------------------------------+
    //    |{"name":"张三","county":"beijing"}|
    //    |{"name":"李四","county":"shanghai"}|
    //    +----------------------------------+
    val datasetString: Dataset[String] = session.createDataset(s"""{"name":"张三","county":"beijing"}""" :: """{"name":"李四","county":"shanghai"}""" :: Nil)
    //    +-------+----+
    //    |county |name|
    //    +-------+----+
    //    |beijing |张三|
    //    |shanghai|李四|
    //    +-------+----+
    val result = session.read.json(datasetString)
    result.show(false)
  }

  /**
   * 读取 csv 文件 ， 创建 DataFrame
   *
   * @param session SparkSession
   */
  def readCSVToDataFrame(session: SparkSession): Unit = {

    val path = "/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/data.csv"

    // 单独 option 参数
    session.read.option("delimiter", ",").option("header", "true").csv(path).show(false)

    // 通过 options 传入map 参数
    session.read.options(Map("delimiter" -> ",", "header" -> "true")).csv(path).show(false)
  }

}
