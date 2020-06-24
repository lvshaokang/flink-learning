package com.lsk.flink.sql.stream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Slide
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

object DatasetStreamWindowSQLApp02 {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    // 时间,用户,商品,价格
    val input = env.fromElements(
      "1000,pk,Spark,75",
      "2000,pk,Flink,80",
      "2000,j,HBase,50",
      "3000,pk,CDH,100",
      "9999,j,ES,90",
      "19999,xingxing,Hive,60"
    ).map(x => {
      val splits = x.split(",")
      val time = splits(0).toLong
      val user = splits(1)
      val product = splits(2)
      val money = splits(3).toDouble
  
      (time, user, product, money)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, String, String, Double)](Time
      .seconds(0)) {
      override def extractTimestamp(element: (Long, String, String, Double)): Long = {
        element._1
      }
    })
  
    val tableEnv = StreamTableEnvironment.create(env)
    tableEnv.createTemporaryView("access",input,
      'time,'user,'product,'money,'rowtime.rowtime)
  
    // api Slide
//    val resultTable = tableEnv.from("access")
//      .window(Slide.over("10.seconds").every("2.seconds")
//      .on("rowtime").as("win"))
//      .groupBy("user,win")
//      .select("user,win.start,win.end,win.rowtime,money.sum as totals")
    
    // sql Slide
//    val sql =
//      """
//        |select
//        |user,sum(money),
//        |hop_start(rowtime,interval '2' second,interval '10' second) as win_start,
//        |hop_end(rowtime,interval '2' second,interval '10' second) as win_end
//        |from
//        |access
//        |group by user,hop(rowtime,interval '2' second,interval '10' second)
//        |""".stripMargin
  
    val sql =
      """
        |select
        |user,sum(money)
        |from
        |access
        |group by user,hop(rowtime,interval '2' second,interval '10' second)
        |""".stripMargin
    
    val resultTable = tableEnv.sqlQuery(sql)
    
    tableEnv.toRetractStream[Row](resultTable).print()
    
    env.execute(this.getClass.getSimpleName)
  }
}
