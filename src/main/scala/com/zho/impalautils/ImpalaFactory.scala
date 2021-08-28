package com.zho.impalautils

import org.apache.hadoop.security.UserGroupInformation

import java.io.IOException
import java.security.PrivilegedExceptionAction
import java.sql.{Connection, DriverManager, SQLException}

class ImpalaFactory {
  private val connections: ThreadLocal[Connection] = new ThreadLocal[Connection]
  private var url: String = _
  private var driver: String = _

  def this(url: String, driver: String) {
    this()
    this.url = url
    this.driver = driver
  }

  /**
   * 创建链接
   * @param loginUser 用户信息
   * @throws SQLException 异常捕获
   * @return
   */
  @throws[SQLException]
  private def getConn(loginUser: UserGroupInformation): Connection = {
    var conn: Connection = null
    try {
      Class.forName(driver)
      if (loginUser != null) {
        conn = loginUser.doAs(new PrivilegedExceptionAction[AnyRef]() {
          override def run: Any = {
            var tcon: Connection = null
            try tcon = DriverManager.getConnection(url)
            catch {
              case e: SQLException =>
                e.printStackTrace()
            }
             tcon
          }
        }).asInstanceOf[Connection]
      }
      else {
        conn = DriverManager.getConnection(url)
      }
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
      case e: InterruptedException =>
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
      case e: Exception =>
        e.printStackTrace()
    }
     conn
  }

  /**
   * 获取链接
   * @param loginUser 用户登录信息
   * @throws SQLException 异常捕获
   * @return 返回链接
   */
  @throws[SQLException]
  def getConnection(loginUser: UserGroupInformation): Connection = {
    var conn: Connection = connections.get
    if (conn == null) {
      conn = getConn(loginUser)
      connections.set(conn)
    }
    conn
  }
}
