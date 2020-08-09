package com.lsk.flink.etl.async

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.concurrent.{Callable, ExecutorService, Executors, TimeUnit}

import com.alibaba.druid.pool.DruidDataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

import org.apache.flink.streaming.api.scala._

/**
 * 使用 Async I/O 的前提:
 * 1.需要一个支持异步请求的客户端
 * 2.或者,没有异步请求客户端,也可以将同步客户端丢到线程池中执行作为异步客户端
 *
 * @author red
 * @class_name MySQLAsyncQueryApp01
 * @date 2020-07-04
 */
object MySQLAsyncQueryApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("localhost", 9999)

    // 添加一个 async I/O 的转换
    // 无序
    val result = AsyncDataStream.unorderedWait(stream, new MySQLAsyncQuery(), 1000, TimeUnit.MILLISECONDS, 10)

    result.print()

    env.execute(this.getClass.getSimpleName)
  }

  class MySQLAsyncQuery extends RichAsyncFunction[String, String] {

    // 创建线程池上下文 executorContext
    implicit lazy val executorContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(10))

    var dataSource: DruidDataSource = _

    override def open(parameters: Configuration): Unit = {


      // 创建连接池
      dataSource = new DruidDataSource()
      dataSource.setDriverClassName("com.mysql.jdbc.Driver")
      dataSource.setUsername("root")
      dataSource.setPassword("ruozedata")
      dataSource.setUrl("jdbc:mysql://ruozedata001:3306/ruozedata_flink")
      dataSource.setInitialSize(5)
      dataSource.setMinIdle(10)
      dataSource.setMaxActive(20)
    }

    override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
      // future
      val future = executorContext.submit(new Callable[String] {
        override def call(): String = {
          query(input)
        }
      })

      val eventualString = Future {
        future.get()
      }

      eventualString.onSuccess {
        case result: String =>
          resultFuture.complete(Iterable(result))
      }


    }

    override def close(): Unit = {
      executorContext.shutdown()
    }

    def query(domain: String): String = {
      var result = ""

      val sql = "select user_id from users_mapping where domain=?"

      var connection:Connection = null
      var pstmt : PreparedStatement = null
      var rs:ResultSet = null

      try {
        connection = dataSource.getConnection
        pstmt = connection.prepareStatement(sql)
        pstmt.setString(1,domain)
        rs = pstmt.executeQuery()
        while(rs.next()) {
          result = rs.getString("user_id")
        }
      } catch {
        case e:Exception => e.printStackTrace()
      } finally {
        // TODO...
      }
      result


    }

  }


}
