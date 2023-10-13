package com.zho.greenplumutils

import java.util.Properties

object JDBCSourceGPTest {

  import java.sql.{Connection, DriverManager, SQLException}

  def main(args: Array[String]): Unit = {
    try {
      getGreenplumConnection.prepareStatement("")
      println("核心数据区连接成功.....")
    }
  }


  @throws[ClassNotFoundException]
  @throws[SQLException]
  def getGreenplumConnection: Connection = {
//    val prop = new Properties()
//    prop.load(this.getClass.getResourceAsStream("config.properties"))
//    val greenplum_driver = prop.getProperty("greenplum_driver")
//    val greenplum_url = prop.getProperty("greenplum_url")
//    val greenplum_user = prop.getProperty("greenplum_user")
//    val greenplum_password = prop.getProperty("greenplum_password")
//println("212" +greenplum_driver+greenplum_url+greenplum_user+greenplum_password)
//    Class.forName(greenplum_driver)
    Class.forName("com.pivotal.jdbc.GreenplumDriver")
    println("测试加载数据库成功");
//    val con = DriverManager.getConnection(greenplum_url, greenplum_user, greenplum_password)
        val con = DriverManager.getConnection("jdbc:pivotal:greenplum://host:5432;DatabaseName=ads_dev", "username", "pwd")
    println("测试数据库链接成功");
    con
  }
}
