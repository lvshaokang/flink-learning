package com.lsk.flink.etl.analysis

import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * TODO: 
 *
 * @author red
 * @class_name FlinkAnalysisApp01
 * @date 2020-07-19
 */
object FlinkAnalysisApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.setParallelism(1)

    //    val stream = env.socketTextStream("localhost", 9999)

//    byUV(env)

    env.execute(this.getClass.getSimpleName)
  }

  /**
   * 在掌握使用Flink进行统计分析的基本功能的基础上，记住编程模板
   */
  def byProvince(env: StreamExecutionEnvironment): Unit = {
    env.readTextFile("data/log.txt")
      .map(x => {
        val splits = x.split(",")

        AccessPage(splits(0), splits(1), splits(2), splits(3).trim.toLong)
      }).assignAscendingTimestamps(_.ts)
      .keyBy(x => x.province)
      .timeWindow(Time.hours(1), Time.minutes(10))
      // 增量聚合
      .aggregate(new AggregateFunction[AccessPage, Long, Long] {
        override def createAccumulator(): Long = 0L

        override def add(value: AccessPage, accumulator: Long): Long = accumulator + 1

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
      }, new WindowFunction[Long, CountByProvince, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
          out.collect(CountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next()))
        }
      }).print()
  }

 /* def byUV(env: StreamExecutionEnvironment): Unit = {
    env.setParallelism(4)
    env.readTextFile("data/log.txt")
      .map(x => {
        val splits = x.split(",")
        AccessPage(splits(0), splits(1), splits(2), splits(3).trim.toLong)
      }).assignAscendingTimestamps(_.ts)
      .timeWindowAll(Time.hours(1))
      .apply(new AllWindowFunction[AccessPage,UVCount, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[AccessPage], out: Collector[UVCount]): Unit = {
          val userIds = scala.collection.mutable.Set[String]()
          for(ele <- input) {
            userIds.add(ele.userId)
          }
          out.collect(UVCount(new Timestamp(window.getEnd).toString,userIds.size))
        }
      }).print()
  }*/


  case class AccessPage(userId: String, province: String, domain: String, ts: Long)

  case class CountByProvince(end: String, province: String, cnts: Long)

  case class PVCount(end: String, cnts: Long)

  case class UVCount(end: String, cnts: Long)

}
