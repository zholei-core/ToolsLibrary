package com.zho.impalautils

import java.sql.{Connection, DriverManager, SQLException}
//import java.util.ResourceBundle

class ImaplaFactorySimple {

  private val connections = new ThreadLocal[Connection]
//  private val rb = ResourceBundle.getBundle("jdbc")
  private val url:String = null //rb.getString("impala.url")
  private val driver:String = null //rb.getString("impala.drive")

  /**
   * 创建连接
   *
   * @return
   * @throws SQLException SQLException
   */
  @throws[SQLException]
  private def getConn:Connection = {
    var conn:Connection = null
    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url)
    } catch {
      case e: ClassNotFoundException =>
        // 记日志
        e.printStackTrace()
      case e: SQLException =>
        e.printStackTrace()
        throw e
    }
    conn
  }

  /**
   * 获取连接
   *
   * @return
   * @throws SQLException SQLException
   */
  @throws[SQLException]
  def getConnection: Connection = {
    var conn: Connection = connections.get
    if (conn == null) {
      conn = getConn
      connections.set(conn)
    }
    conn
  }
}
