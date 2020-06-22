package com.lsk.flink.watermark

import java.sql.Timestamp

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 延迟数据处理 outputTag
 *
 * nc -lp 9999
 *
 * 1000,a,1
 * 2000,a,1
 * 3000,a,1
 * 4999,c,1
 * 4000,a,1
 * 4444,b,1
 * 8888,a,1
 * 9999,b,1
 *
 * @author red
 * @class_name WatermarkApp01
 * @date 2020-06-23
 */
object WatermarkApp03 {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val outputTag = new OutputTag[(String, Int)]("later-date")

    val stream = env.socketTextStream("localhost",9999)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        override def extractTimestamp(element: String): Long = {
          element.split(",")(0).toLong
        }
      })
        .map(x => {
          val splits = x.split(",")
          (splits(1).trim, splits(2).trim.toInt)
        }).keyBy(0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .sideOutputLateData(outputTag)
//        .sum(1)
        .reduce(new ReduceFunction[(String, Int)] {
          override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
            (value1._1, value1._2 + value2._2)
          }
        },new ProcessWindowFunction[(String,Int),String,Tuple,TimeWindow] {
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
    stream.getSideOutput(outputTag).print("--------")

    env.execute(this.getClass.getSimpleName)
  }

}
