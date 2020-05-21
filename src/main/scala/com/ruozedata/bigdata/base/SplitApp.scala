package com.ruozedata.bigdata.base

import java.{lang, util}

import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

object SplitApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    test1(env)
    test2(env)

    env.execute(this.getClass.getSimpleName)

  }

  private def test2(env: StreamExecutionEnvironment) = {
    val stream = env.readTextFile("data/split.txt")
      .map(x => {
        val splits = x.split(",")
        SplitAcess(splits(0).trim, splits(1).trim, splits(2).trim.toLong)
      })

    val guangdongTag = new OutputTag[SplitAcess]("guangdong")
    val fujianTag = new OutputTag[SplitAcess]("fujian")

    val splitStream = stream.process(new ProcessFunction[SplitAcess, SplitAcess] {
      override def processElement(value: SplitAcess, ctx: ProcessFunction[SplitAcess, SplitAcess]#Context, out: Collector[SplitAcess]): Unit = {
        if ("广东省" == value.province) {
          ctx.output(guangdongTag, value)
        } else if ("福建省" == value.province) {
          ctx.output(fujianTag, value)
        }
      }
    })

    val guangdong: DataStream[SplitAcess] = splitStream.getSideOutput(guangdongTag)
    val fujian: DataStream[SplitAcess] = splitStream.getSideOutput(fujianTag)


    val shenzhenTag = new OutputTag[SplitAcess]("shenzhen")
    val guangzhouTag = new OutputTag[SplitAcess]("guangzhou")

    val cityStream = guangdong.process(new ProcessFunction[SplitAcess, SplitAcess] {
      override def processElement(value: SplitAcess, ctx: ProcessFunction[SplitAcess, SplitAcess]#Context, out: Collector[SplitAcess]): Unit = {
        if ("深圳市" == value.city) {
          ctx.output(shenzhenTag, value)
        } else if ("广州市" == value.city) {
          ctx.output(guangzhouTag, value)
        }
      }
    })

    cityStream.getSideOutput(shenzhenTag).print("深圳：====")
    cityStream.getSideOutput(guangzhouTag).print("广州：====")



  }

  private def test1(env: StreamExecutionEnvironment) = {
    val splitsStream = env.readTextFile("data/split.txt")
      .map(x => {
        val splits = x.split(",")
        SplitAcess(splits(0).trim, splits(1).trim, splits(2).trim.toLong)
      }).split(new OutputSelector[SplitAcess] {
      override def select(value: SplitAcess): lang.Iterable[String] = {
        val splits = new util.ArrayList[String]()
        if (value.province == "广东省") {
          splits.add("广东省")
        } else if (value.province == "福建省") {
          splits.add("福建省")
        }
        splits
      }
    })

    splitsStream.select("福建省").print("福建省分流")
    splitsStream.select("广东省").print("广东省分流")
  }


}
