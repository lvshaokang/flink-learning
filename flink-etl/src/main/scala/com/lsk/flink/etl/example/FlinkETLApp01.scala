package com.lsk.flink.etl.example

import java.sql.Connection

import com.lsk.flink.etl.utils.MySQLUtil
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * TODO: 
 *
 * @author red
 * @class_name FlinkETLApp01
 * @date 2020-07-19
 */
object FlinkETLApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("localhost", 9999)

    // TODO... map 操作去关联MySQL数据

    stream.map(new RichMapFunction[String, Access] {

      var connection: Connection = _

      override def open(parameters: Configuration): Unit = {
        connection = MySQLUtil.getConnection()
      }

      override def close(): Unit = {
        MySQLUtil.close(connection)
      }

      override def map(value: String): Access = {
        val splits = value.split(",")

        val time = splits(0)
        val domain = splits(1)
        val province = splits(2)
        val traffic = splits(3).toLong

        val pstmt = connection.prepareStatement("select user_id from users_mapping where domain=?")
        pstmt.setString(1, domain)

        var userId = ""
        val rs = pstmt.executeQuery()
        if (rs.next()) {
          userId = rs.getString(1)
        }

        pstmt.close()

        Access(time,domain,province,traffic,userId)
      }
    }).print()


    env.execute(this.getClass.getSimpleName)
  }

  case class Access(time: String,
                    domain: String,
                    province: String,
                    traffic: Long,
                    userId: String = "")
}
