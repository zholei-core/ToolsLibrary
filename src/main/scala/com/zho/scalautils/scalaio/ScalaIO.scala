package com.zho.scalautils.scalaio

import java.net.URL

import scala.io.{BufferedSource, Source}

object ScalaIO {
  def main(args: Array[String]): Unit = {
    // 在 resources 目录中 获取数据文件路径
    val dataUrl: URL = this.getClass.getClassLoader.getResource("data.txt")
    // 根据路径加载 文件数据
    val data: BufferedSource = Source.fromURL(dataUrl)
    // 获取文件数据里的 所有行 返回数据集合
    val lindata: Iterator[String] = data.getLines()

    // 将文件获取的【数据集合】，直接打印输出
    lindata.foreach(println(_))

    // 将文件中获取的数据，通过调用 zipWithIndex 转换为 带有行号的数据类型
    //    val dataAndIndex: Iterator[(String, Int)] = lindata.zipWithIndex
    // 将数据 按照 key -> value 类型输出
    //    dataAndIndex.foreach(line => println(line._2 + " -> " + line._1))
  }
}
