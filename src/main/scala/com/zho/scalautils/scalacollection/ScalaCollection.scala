package com.zho.scalautils.scalacollection

import java.util
import java.util.Collections

object ScalaCollection extends App {

  new ScalaCollection().javaCollectionMaxAndMin()
  new ScalaCollection().scalaCollectionMaxAndMin()

}

class ScalaCollection {

  /**
   * java Collections 方法获取集合的最大值 与最小值
   */
  def javaCollectionMaxAndMin(): Unit = {
    val arrayList = new util.ArrayList[Integer]()
    arrayList.add(11)
    arrayList.add(66)
    arrayList.add(33)
    val arrMax = Collections.max(arrayList)
    val arrMin = Collections.min(arrayList)
    println(s"Java List -> MAX : $arrMax ; MIN: $arrMin")
  }

  /**
   * scala Collections 方法获取集合的最大值 与最小值
   */
  def scalaCollectionMaxAndMin(): Unit = {
    val listData = List[Integer](11, 66, 33, 99)
    println(s"Scala List -> MAX : ${listData.max} ; MIN: ${listData.min}")
  }
}
