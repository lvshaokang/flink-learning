package com.lsk.flink.watermark

import java.sql.Timestamp

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 延迟数据处理 outputTag
 *
 * nc -lp 9999
 * nc -l 9999
 *
 * 每个2s,滑6s
 * SlidingEventTimeWindows(6,2)
 *
 * 第一个窗口不完整
 * [-4000,2000) -> [0,2000)
 * [-2000,4000) -> [0,4000)
 * [0,6000) -> [0,6000)
 *
 * 1000,a,1
 * 1999,a,1
 * -- 触发 [0,2000) a->2
 *
 * 2222,b,1
 * 2999,a,1
 * 4000,a,1
 * -- 触发 [0,4000) a->3 b->1
 *
 * @author red
 * @class_name WatermarkApp04
 * @date 2020-06-23
 */
object WatermarkApp07 {
  
  def main(args: Array[String]): Unit = {
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    env.setParallelism(1)
    
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    val stream = env.socketTextStream("node11", 9999)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        override def extractTimestamp(element: String): Long = {
          element.split(",")(0).toLong
        }
      })
      .map(x => {
        val splits = x.split(",")
        (splits(1).trim, splits(2).trim.toInt)
      })
      .keyBy(0)
      .window(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(2)))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2 + value2._2)
        }
      }, new ProcessWindowFunction[(String, Int), String, Tuple, TimeWindow] {
        override def process(key: Tuple, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
          for (ele <- elements) {
            out.collect(new Timestamp(context.currentWatermark)
              + "=>" + new Timestamp(context.window.getStart)
              + "=>" + new Timestamp(context.window.getEnd)
              + "=>" + ele._1
              + "=>" + ele._2)
          }
        }
      })
    
    stream.print()
    
    env.execute(this.getClass.getSimpleName)
  }
}



