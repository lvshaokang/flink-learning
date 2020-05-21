package com.ruozedata.bigdata.base

import java.sql.{Connection, PreparedStatement}

import com.ruozedata.bigdata.utils.MySQLUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * author：若泽数据-PK哥   
 * 交流群：545916944
 */
class RuozedataMySQLSource extends RichParallelSourceFunction[Student]{
  var connection:Connection = _
  var pstmt:PreparedStatement = _

  /**
   * 初始化操作：IO Connection
   */
  override def open(parameters: Configuration): Unit = {
    connection = MySQLUtils.getConnection()
    pstmt = connection.prepareStatement("select * from student")
  }

  override def close(): Unit = {
    MySQLUtils.close(connection, pstmt)
  }

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    val rs = pstmt.executeQuery()
    while(rs.next()) {
      val id = rs.getInt("id")
      val name = rs.getString("name")
      val age = rs.getInt("age")
      ctx.collect(Student(id, name, age))
    }
  }

  override def cancel(): Unit = ???
}
