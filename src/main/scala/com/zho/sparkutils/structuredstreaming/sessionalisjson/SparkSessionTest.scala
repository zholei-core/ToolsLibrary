package com.zho.sparkutils.structuredstreaming.sessionalisjson

import com.google.gson.Gson
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

object SparkSessionTest {

  def main(args: Array[String]): Unit = {
    val json = new SparkSessionTest()
    val j = json.parse("{'name':'caocao','age':'32','sex':'male'}", "com.zho.Person")
    val tuple = list2Tuple7(j)
    println(j)
    println(tuple)
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("appName")
      .getOrCreate()
    val df = spark.sql(
      """
        |select "{'name':'caocao','age':'32','sex':'male'}" as value
        |""".stripMargin)
    import spark.implicits._
    val values: Dataset[String] = df.selectExpr("cast(value as string)").as[String]
    val res: Dataset[(String, String, String, String, String, String, String)] = values.map(elemValue => {
      val list = parseJson(elemValue, "com.zho.Person")
      list2Tuple7(list)
    })
    res.createOrReplaceTempView("temp")
    spark.sql("select _1 as name,_2 as age,_3 as sex from temp").show(false)

  }

  def parseJson(json: String, className: String): List[String] = {
    val tools = new SparkSessionTest()
    tools.parse(json, className)
  }

  // 将List转成Tuple7元组类，这里仅仅是定义7个字段，可以定义更多字段。（ps：这种处理方式很不雅，一时也没想到好办法）
  def list2Tuple7(list: List[String]): (String, String, String, String, String, String, String) = {
    val t = list match {
      case List(a) => (a, "", "", "", "", "", "")
      case List(a, b) => (a, b, "", "", "", "", "")
      case List(a, b, c) => (a, b, c, "", "", "", "")
      case List(a, b, c, d) => (a, b, c, d, "", "", "")
      case List(a, b, c, d, e) => (a, b, c, d, e, "", "")
      case List(a, b, c, d, e, f) => (a, b, c, d, e, f, "")
      case List(a, b, c, d, e, f, g) => (a, b, c, d, e, f, g)
      case _ => ("", "", "", "", "", "", "")
    }
    t
  }

}

class SparkSessionTest{
  // 通过传进来的Bean的全类名，进行反射，解析json，返回一个List()
  def parse(json: String, className: String): List[String] = {
    val list = mutable.ListBuffer[String]()
    val gson = new Gson()
    val clazz = Class.forName(className)
    val obj = gson.fromJson(json, clazz)
    val aClass = obj.getClass
    val fields = aClass.getDeclaredFields
    fields.foreach { f =>
      val fName = f.getName
      val m = aClass.getDeclaredMethod(fName)
      val value = m.invoke(obj).toString
      list.append(value)
    }
    list.toList
  }
}