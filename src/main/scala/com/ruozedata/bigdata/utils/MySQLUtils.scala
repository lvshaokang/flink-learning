package com.ruozedata.bigdata.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * author：若泽数据-PK哥   
 * 交流群：545916944
 */
object MySQLUtils {

  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://hadoop001:3306/ruozedata_flink","root","ruozedata")
  }

  def close(connection:Connection, pstmt:PreparedStatement): Unit = {
    if(null != pstmt) pstmt.close()
    if(null != connection) connection.close()
  }
}
