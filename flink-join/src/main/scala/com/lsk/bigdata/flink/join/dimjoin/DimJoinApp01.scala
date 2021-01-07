package com.lsk.bigdata.flink.join.dimjoin

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.RichProcessWindowFunction
import org.apache.flink.util.Collector

import java.lang

/**
 * 预加载维表
 * 模拟数据从socket加载
 *
 * 优点:
 *  实现简单
 * 缺点:
 *  数据存储于内存,仅适合小数量且维表数据更新频率不高的情况,虽然可以在open中定义一个定时器(这里定时器是什么? Timer),定时更新维表,
 *  但是还是存在维表更新不及时的情况
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
object DimJoinApp01 {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
  
    val baseStream = env.socketTextStream("localhost", 9000, '\n')
      .map(line => {
        val splits = line.split(",")
        // userName, cityId
        (splits(0), splits(1).toInt)
      })
  
    val joinStream = baseStream.map(new RichMapFunction[(String,Int), (String, Int, String)] {
      // <cityId, cityName>
      var dim: collection.mutable.Map[Int, String] = _
  
      override def open(parameters: Configuration): Unit = {
        // open 方法中读取维表数据,数据/文件/接口等
        dim += (1001 -> "beijing")
        dim += (1002 -> "shanghai")
        dim += (1003 -> "chengdu")
        dim += (1004 -> "wuhan")
      }
  
      override def map(value: (String, Int)): (String, Int, String) = {
        // map 中进行主表和维表的关联
        var cityName = ""
        if (dim.contains(value._2)) {
          cityName = dim(value._2)
        }
        (value._1, value._2, cityName)
      }
    })
    
    joinStream.print()
    
    env.execute(this.getClass.getSimpleName)
  }
  
}
