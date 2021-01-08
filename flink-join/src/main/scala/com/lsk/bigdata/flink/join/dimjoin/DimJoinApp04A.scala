package com.lsk.bigdata.flink.join.dimjoin

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


/**
 * Temporal table function join (Temporal table 是持续变化表上某一时刻的视图,
 *  Temporal table function 是一个表函数,传递一个时间参数, 返回Temporal table 这一指定时刻的视图)
 *  可以将维度数据流映射为Temporal table,主流与这个Temporal table进行关联,可以关联到某一版本(历史上某一个时刻)的维度数据
 *
 * 模拟数据从socket加载
 *  ProcessingTime
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
object DimJoinApp04A {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)

    val baseStream = env.socketTextStream("localhost", 9000, '\n')
      .map(line => {
        val splits = line.split(",")
        // userName, cityId
        (splits(0), splits(1).toInt)
      })

    val cityStream = env.socketTextStream("localhost", 9001, '\n')
      .map(line => {
        val splits = line.split(",")
        // cityId, cityName
        (splits(0).toInt, splits(1))
      })

    val userTable = tableEnv.fromDataStream(baseStream, 'user_name ,'city_id, 'ps.proctime)
    val cityTable = tableEnv.fromDataStream(cityStream, 'city_id, 'city_name, 'ps.proctime)

    // 定义一个TemporalTableFunction
    val dimCity = cityTable.createTemporalTableFunction('ps, 'city_id)
    // 注册表函数
    tableEnv.registerFunction("dimCity", dimCity)

    val result = tableEnv.sqlQuery(
      s"""
         | select u.user_name, u.city_id, d.city_name
         | from ${userTable} as u,
         | Lateral table (dimCity(u.ps)) d
         | where u.city_id = d.city_id
         | """.stripMargin)

    val resultDs = tableEnv.toAppendStream[Row](result)

    resultDs.print()

    env.execute(this.getClass.getSimpleName)
  }
}
