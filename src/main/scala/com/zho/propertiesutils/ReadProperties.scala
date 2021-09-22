package com.zho.propertiesutils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.util._

trait ReadProperties {
  /**
   * 获取配置文件
   *
   * @param propertiesName 文件名带有后缀
   * @return
   */
  def getPropertiesResourceStream(propertiesName: String): Properties = {
    val props = new Properties()
    // Mehtod 一
    props.load(this.getClass.getClassLoader.getResourceAsStream(s"$propertiesName"))
    // 读取项目中配置文件 Method 二
    //    props.load(new FileInputStream(this.getClass.getClassLoader.getResource("config.properties").getPath))
    props
  }

  /**
   * 获取配置文件 字符串需要外部变量传入，不能直接使用
   *
   * @param propertiesName 文件名无需添加后缀
   * @return ResourceBundle
   */
  def getPropertiesResourceBundle(propertiesName: String): ResourceBundle = {
    ResourceBundle.getBundle(s"$propertiesName")
  }

  /**
   *
   * @param readPropMode   读取配置文件模式 (LOCAL,HDFS)
   * @param propertiesName 配置文件名称
   * @return 配置文件信息
   */
  def getPropertiesDynamic(readPropMode: String, propertiesName: String): Properties = {

    val props: Properties = new Properties()
    // 根据参数 ， 判断读取何地的配置文件的数据
    val configFilePathInputStream = readPropMode.toUpperCase match {
      case "LOCAL" =>
        // 读取项目中配置文件
        this.getClass.getClassLoader.getResourceAsStream(s"$propertiesName")
      case "HDFS" =>
        // 读取HDFS 上配置文件
        FileSystem.get(new Configuration()).open(new Path(s"HDFS_PATH/$propertiesName"))
    }
    // 加载配置文件
    props.load(configFilePathInputStream)
    props
  }
}

object ReadProperties extends ReadProperties with App {

  val properties = getPropertiesResourceStream("config.properties")
  println("stream=>" + properties.getProperty("es.scheme"))

  val reader = getPropertiesResourceBundle("config")
  val str = "es.scheme"
  println("bundle=>" + reader.getString(str))

  val dynamicProp = getPropertiesDynamic("local", "config.properties")
  println("dynamic=>" + dynamicProp.getProperty("es.scheme"))

}
