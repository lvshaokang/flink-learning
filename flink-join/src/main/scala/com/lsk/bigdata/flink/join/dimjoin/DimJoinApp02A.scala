/*
package com.lsk.bigdata.flink.join.dimjoin

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.guava18.com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache, RemovalListener, RemovalNotification}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import java.util.concurrent.TimeUnit

/**
 * 热存储(维表数据存储在Redis/HBase/MySQL等)
 * 模拟数据从socket加载
 *
 * 1. Cache减轻访问压力
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
object DimJoinApp02A {
  
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
      var cache: LoadingCache[Int, String] = _
  
      override def open(parameters: Configuration): Unit = {
        cache = CacheBuilder
          .newBuilder()
          // 最多缓存个数,超过了就根据最近最少使用算法来移除缓存 LRU
          .maximumSize(1000)
          // 在更新后的指定时间后就回收
          .expireAfterWrite(10, TimeUnit.MINUTES)
          // 指定移除通知
          .removalListener[Int, String](new RemovalListener[Int, String] {
            override def onRemoval(removalNotification: RemovalNotification[Int, String]): Unit = {
              println(removalNotification.getKey + "被移除了,值为: " + removalNotification.getValue)
            }
          })
          .build[Int, String](new CacheLoader[Int, String] {
            override def load(k: Int): String = {
              val cityName = readFromHBase(k)
              cityName
            }
          })
      }
  
      override def map(value: (String, Int)): (String, Int, String) = {
        // map 中进行主表和维表的关联
        var cityName = ""
        if (cache.get(value._2) != null) {
          cityName = cache(value._2)
        }
        (value._1, value._2, cityName)
      }
    })
    
    joinStream.print()
    
    env.execute(this.getClass.getSimpleName)
  }

  def readFromHBase(key: Int) : String = {
    //读取hbase

    // 这里写死,模拟从HBase读数据
    var map: collection.mutable.Map[Int, String] = Map(){}
    map += (1001 -> "beijing")
    map += (1002 -> "shanghai")
    map += (1003 -> "chengdu")
    map += (1004 -> "wuhan")

    var cityName = ""
    if (map.contains(key)) {
      cityName = map(key)
    }

    cityName
  }
  
}
*/
