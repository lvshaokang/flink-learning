package com.lsk.bigdata.flink.join.dimjoin

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._

/**
 * 热存储(维表数据存储在Redis/HBase/MySQL等)
 * 模拟数据从socket加载
 *
 * 2. 异步IO
 *  异步IO涉及问题
 *  1. 超时: 如果查询超时,那么就认为是读写失败,需要按失败处理
 *  2. 并发数量: 如果并发数量太多,就要触发Flink的反压机制来抑制上游的写入
 *  3. 返回顺序错乱: 顺序错乱了要根据实际情况来处理,Flink支持两种方式: 允许乱序、保证顺序
 *
 * 优点: 维度数据量不受内存限制,可以存储很大的数据量
 *
 * 缺点: 因为维表在外部存储中,读取速度受限于外部存储的读取速度;另外维表的同步也有延迟
 *
 * User
 * userName String,
 * cityId Int,
 * timestamp Long
 *
 * City
 * cityId Int,
 * cityName String,
 * timestamp Long
 */
object DimJoinApp02C {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
  
    val baseStream = env.socketTextStream("localhost", 9000, '\n')
      .map(line => {
        val splits = line.split(",")
        // userName, cityId
        (splits(0), splits(1).toInt)
      })

    val orderedResult = AsyncDataStream
      // 保证顺序: 异步返回的结果保证顺序,超时时间1s,最大容量2,超出容量触发反压
      // IN: <userName, userId> OUT: <userName, userId, cityName>
      .orderedWait(baseStream, new AsyncFunction, 1L, TimeUnit.SECONDS, 2)
//      .setParallelism(1)

    val unorderedResult = AsyncDataStream
      // 保证顺序: 异步返回的结果保证顺序,超时时间1s,最大容量2,超出容量触发反压
      // IN: <userName, userId> OUT: <userName, userId, cityName>
      .unorderedWait(baseStream, new AsyncFunction, 1L, TimeUnit.SECONDS, 2)
    //      .setParallelism(1)

    orderedResult.print()
    unorderedResult.print()
    
    env.execute(this.getClass.getSimpleName)
  }

  class AsyncFunction extends RichAsyncFunction[(String, Int), (String, Int, String)] {

    var conn: Connection = _
    var pstmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)

      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306?useSSL=false")
      pstmt = conn.prepareStatement("select city_name from city_info where id = ?")
    }

    override def close(): Unit = {
      super.close()
      conn.close()
    }

    // 异步查询存储在MySQL中的维表
    override def asyncInvoke(input: (String, Int), resultFuture: ResultFuture[(String, Int, String)]): Unit = {
      pstmt.setInt(1, input._2)
      val rs = pstmt.executeQuery()
      var cityName = ""
      if (rs.next()) {
        cityName = rs.getString(1)
      }
      resultFuture.complete(List((input._1, input._2, cityName)))
    }

    // 超时处理(这里给""值)
    override def timeout(input: (String, Int), resultFuture: ResultFuture[(String, Int, String)]): Unit = {
      resultFuture.complete(List((input._1, input._2, "")))
    }
  }
}