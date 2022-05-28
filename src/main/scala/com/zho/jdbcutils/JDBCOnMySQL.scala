package com.zho.jdbcutils

import com.zho.propertiesutils.ReadProperties
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

class JDBCOnMySQL extends ReadProperties {


  private val props: Properties = getPropertiesResourceStream("config.properites")

  private val url = props.getProperty("")
  private val driver = props.getProperty("")
  private val user = props.getProperty("")
  private val pwd = props.getProperty("")

  def getConnection(): Connection = {
    Class.forName(driver)
    DriverManager.getConnection(url, user, pwd)

  }
}

object JDBCOnMySQL {
  def main(args: Array[String]): Unit = {
    try {

      Class.forName("com.mysql.cj.jdbc.Driver")
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://server1.zholei.com/cdc_test", "root", "12345678")
      val stat: Statement = connection.createStatement()
      val set = stat.executeQuery("select * from user_info")
      while (set.next()) {
        val id = set.getString("id")
        val name = set.getString("name")
        val sex = set.getString("sex")
        println(s"ID:$id,NAME:$name,SEX:$sex")
      }

    } catch {
      case e: Exception =>LoggerFactory.getLogger(this.getClass.getSimpleName).error("error",e)
    }
  }
}