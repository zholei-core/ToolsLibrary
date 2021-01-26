package com.zho.dynamicloadutils

import java.io.FileInputStream
import java.net.URL
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

object DynamicPropsFileUtil {

  // ES 请求参数
  var ES_NODES = ""
  var ES_PORT = 0
  var ES_SCHEME = ""


  /**
   * 获取配置文件，读取其中的配置
   */
  def getProperties(filePathDir: String): Unit = {

    val props = new Properties()
    // 根据参数 ， 判断读取何地的配置文件的数据
    val configFilePathInputStream = filePathDir.toUpperCase match {
      case "PROJECT" =>
        // 读取项目中配置文件
        val configFilePath: String = this.getClass.getClassLoader.getResource("config.properties").getPath
        new FileInputStream(configFilePath)
      case "HDFS" =>
        // 读取HDFS 上配置文件
        val fileSystem = FileSystem.get(new Configuration())
        fileSystem.open(new Path("HDFS_PATH"))
    }
    // 加载配置文件
    props.load(configFilePathInputStream)


    // 获取 ES 相关配置参数
    ES_NODES = props.getProperty("es.nodes")
    ES_PORT = props.getProperty("es.port").toInt
    ES_SCHEME = props.getProperty("es.scheme")

  }


  def main(args: Array[String]): Unit = {
    getProperties("PROJECT")
    println(DynamicPropsFileUtil.ES_SCHEME)
  }
}
