package com.zho.greenplumutils

import java.sql.{Connection, DriverManager, SQLException}

object JDBCSourceePGTest {
  def main(args: Array[String]): Unit = {

  }
    @throws[ClassNotFoundException]
    @throws[SQLException]
    def getPostgresqlConnection: Connection = { // 加载数据库驱动
      Class.forName("org.postgresql.Driver")
      // System.out.println("测试加载数据库成功");
//      val con = DriverManager.getConnection(postgresql_url, postgresql_user, postgresql_password)
      val con = DriverManager.getConnection("postgresql_url", "postgresql_user", "postgresql_password")
      // System.out.println("测试数据库链接成功");
      con
    }
}
