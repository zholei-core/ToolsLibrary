package com.zho.jdbcutils

import com.zho.propertiesutils.ReadProperties

import java.sql.{Connection, DriverManager}
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

  }
}