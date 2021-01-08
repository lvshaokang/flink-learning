package com.lsk.bigdata.flink.join.dimjoin

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

import java.util.concurrent.TimeUnit



/**
 * Temporal table function join (Temporal table 是持续变化表上某一时刻的视图,
 *  Temporal table function 是一个表函数,传递一个时间参数, 返回Temporal table 这一指定时刻的视图)
 *  可以将维度数据流映射为Temporal table,主流与这个Temporal table进行关联,可以关联到某一版本(历史上某一个时刻)的维度数据
 *
 * 模拟数据从socket加载
 *  EventTime
 *
 * 优点: 维度数据量可以很大,维度数据更新及时,不依赖外部存储,可以关联不同版本的维度数据
 *
 * 缺点: 仅支持Flink SQL API
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
object DimJoinApp04B {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)
    
//    env.setParallelism(1)

    // userName,city_id,ts
    val baseStream = env.fromCollection(Seq(
      ("user1", 1001, 1L),
      ("user1", 1001, 10L),
      ("user2", 1002, 2L),
      ("user2", 1002, 15L)
    )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Int, Long)](Time.seconds(10)) {
      override def extractTimestamp(element: (String, Int, Long)): Long = {
        element._3
      }
    })
    
    val cityStream = env.fromCollection(Seq(
      (1001, "beijing", 1L),
      (1001, "beijing2", 10L),
      (1002, "shanghai", 1L),
      (1002, "shanghai2", 5L)
    )).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, String, Long)](Time.seconds(10)) {
      override def extractTimestamp(element: (Int, String, Long)): Long = {
        element._3
      }
    })
    
    val userTable = tableEnv.fromDataStream(baseStream, 'user_name ,'city_id, 'ts.rowtime)
    val cityTable = tableEnv.fromDataStream(cityStream, 'city_id, 'city_name, 'ts.rowtime)
    
    tableEnv.createTemporaryView("userTable", userTable)
    tableEnv.createTemporaryView("cityView", cityTable)

    // 定义一个TemporalTableFunction
    val dimCity = cityTable.createTemporalTableFunction('ts, 'city_id)
    // 注册表函数
    tableEnv.registerFunction("dimCity", dimCity)

    val result = tableEnv.sqlQuery(
      s"""
         | select u.user_name, u.city_id, d.city_name, u.ts
         | from ${userTable} as u,
         | Lateral table (dimCity(u.ts)) d
         | where u.city_id = d.city_id
         | """.stripMargin)

    val resultDs = tableEnv.toAppendStream[Row](result)

    resultDs.print()

    env.execute(this.getClass.getSimpleName)
  }
}
