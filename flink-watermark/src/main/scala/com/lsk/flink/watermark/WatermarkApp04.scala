package com.lsk.flink.watermark

import java.sql.Timestamp

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 延迟数据处理 outputTag
 *
 * nc -lp 9999
 * nc -l 9999
 *
 * 顺序数据
 *
 * // apply
 *
 * // process
 *
 * // agg
 *
 * [00,03)
 * [03,06)
 * [06,09)
 * [09,12)
 *
 * 1,36.8,a,1590157928
 * 1,37.8,a,1590157929
 * 1,38.8,a,1590157930
 *
 * 1,39.8,a,1590157938
 * 1,38.8,a,1590157939
 *
 * 1,38.8,a,1590157942
 *
 * @author red
 * @class_name WatermarkApp04
 * @date 2020-06-23
 */
object WatermarkApp04 {
  
  def main(args: Array[String]): Unit = {
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    env.setParallelism(1)
    
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    
    val MAX_ALLOWED_UNBOUNDED_TIME = 10 * 1000
    
    env.socketTextStream("node11", 9999)
      .map(x => {
        val splits = x.split(",")
        Log(splits(0).trim, splits(1).trim.toDouble, splits(2).trim, splits(3).trim.toLong)
      }).assignTimestampsAndWatermarks(new DefaultAssignerWithPeriodicWatermarks(MAX_ALLOWED_UNBOUNDED_TIME))
      .keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new RichWindowFunction[Log, String, Tuple, TimeWindow] {
        // 求平均温度
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[Log], out: Collector[String]): Unit = {
          val total = input.size
          var totalTemperature = 0.0
          input.foreach(x => {
            totalTemperature = totalTemperature + x.temperature
          })
          
          val avg = totalTemperature / total
          out.collect(s"${avg}, ${new Timestamp(window.getStart)}, ${new Timestamp(window.getEnd)}")
          
        }
      }).print()
    
    env.execute(this.getClass.getSimpleName)
  }
  
  case class Log(id: String, temperature: Double, name: String, time: Long)
  
  class DefaultAssignerWithPeriodicWatermarks(maxAllowUnboundedTime: Long) extends AssignerWithPeriodicWatermarks[Log] {
  
    var maxTimestamp: Long = 0
  
    override def getCurrentWatermark: Watermark = {
      new Watermark(maxTimestamp - maxAllowUnboundedTime)
    }
  
    override def extractTimestamp(element: Log, previousElementTimestamp: Long): Long = {
      val currTime = element.time * 1000
    
      maxTimestamp = maxTimestamp.max(currTime)
      println(new Timestamp(currTime) + "," + new Timestamp(maxTimestamp) + "," + new Timestamp(getCurrentWatermark.getTimestamp))
    
      currTime
    }
  }
}


