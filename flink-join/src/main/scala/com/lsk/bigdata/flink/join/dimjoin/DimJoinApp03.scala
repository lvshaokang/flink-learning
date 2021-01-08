package com.lsk.bigdata.flink.join.dimjoin

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 广播维表
 * 模拟数据从socket加载
 *
 * 优点: 维度数据变更后可以即时更新到结果中
 *
 * 缺点: 数据保存在内存中,支持的维度数据量比较少
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
object DimJoinApp03 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
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
  
    val cityBroad = new MapStateDescriptor[Int, String]("cityBroad", classOf[Int], classOf[String])
    // 广播流
    val broadcastStream = cityStream.broadcast(cityBroad)
    
    // 使用connect连接流
    val result = baseStream.connect(broadcastStream)
      .process(new BroadcastProcessFunction[(String, Int), (Int, String), (String, Int, String)] {
        // 处理非广播流,关联维度
        override def processElement(value: (String, Int), ctx: BroadcastProcessFunction[(String, Int), (Int, String), (String, Int, String)]#ReadOnlyContext, out: Collector[(String, Int, String)]): Unit = {
          val state = ctx.getBroadcastState(cityBroad)
          var cityName = ""
          if (state.contains(value._2)) {
            cityName = state.get(value._2)
          }
          // elem
          out.collect(value._1, value._2, cityName)
        }
  
        // 处理广播流
        override def processBroadcastElement(value: (Int, String), ctx: BroadcastProcessFunction[(String, Int), (Int, String), (String, Int, String)]#Context, out: Collector[(String, Int, String)]): Unit = {
          println("收到广播数据: " + value)
          ctx.getBroadcastState(cityBroad).put(value._1, value._2)
        }
      })
  
    result.print()
    
    env.execute(this.getClass.getSimpleName)
  }
}
