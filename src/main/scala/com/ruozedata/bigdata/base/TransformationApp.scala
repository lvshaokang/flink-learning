package com.ruozedata.bigdata.base

import org.apache.flink.api.common.functions.{MapFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{ConnectedStreams, SplitStream, StreamExecutionEnvironment}

/**
 * author：若泽数据-PK哥
 * 交流群：545916944
 */
object TransformationApp {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//        map(env)
    //    keyBy(env)
//    split(env)
//    connectUnion(env)
    partitionCustom(env)
    env.execute("Flink Streaming Scala API Skeleton")
  }

  def partitionCustom(env: StreamExecutionEnvironment): Unit = {
    env.setParallelism(3)
    env.addSource(new AccessSource)
      .map(x=>(x.domain,x)) // 按照业务需求进行自定义分区
      .partitionCustom(new RuozedataPartitioner,0)
      .map(x => {
        println("....." + Thread.currentThread().getId + "==> 值:" + x)
        x._2
      }).print()

  }

  /**
   * connect仅支持两个流，数据类型可以不同
   * union支持多流，数据类型需要一致
   * @param env
   */
  def connectUnion(env: StreamExecutionEnvironment): Unit = {
    val stream = env.readTextFile("data/access.log")
    val accessStream = stream.map(x => {
      val splits = x.split(",")
      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    }).keyBy("domain").sum("traffics")

    val splitStream = accessStream.split(x => {
      if (x.traffics <= 6000) {
        Seq("非大客户")
      } else {
        Seq("大客户")
      }
    })

    val s1 = splitStream.select("大客户")//.map(x => (x.time, x.traffics))
    val s2 = splitStream.select("非大客户")


    s1.print("大客户")
    s2.print("fei大客户")


    val connectedStream: ConnectedStreams[(Long, Long), Access] = s1.map(x => (x.time, x.traffics)).connect(s2)
    connectedStream.map(x=>{
      (x._1,x._2,"rz")
    },y=>{
      (y.time,y.domain,y.traffics,"--")
    }).print()

//    s1.union(s2)
  }

  def split(env: StreamExecutionEnvironment): Unit = {
    val stream = env.readTextFile("data/access.log")
    val accessStream = stream.map(x => {
      val splits = x.split(",")
      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    }).keyBy("domain").sum("traffics")

    // side output  侧

    /**
     * split : 根据业务条件把一个DataStream拆分（并不是真正的拆，而是打了一个标签而已）成N个SplitStream
     * select：从一个SplitStream获取业务需要的DataStream
     */
    val splitStream = accessStream.split(x => {
      if (x.traffics <= 6000) {
        Seq("非大客户")
      } else {
        Seq("大客户")
      }
    })

    val s1 = splitStream.select("大客户")
      s1.print("大客户")
    val s2 = splitStream.select("非大客户")
      s2.print("非大客户")

  }


  def keyBy(env: StreamExecutionEnvironment): Unit = {
    //    val stream = env.readTextFile("data/access.log")
    //    val accessStream = stream.map(x => {
    //      val splits = x.split(",")
    //      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    //    })
    //    accessStream.keyBy("domain")//.sum("traffics").print()
    //      .reduce((x,y)=>Access(x.time,x.domain,x.traffics+y.traffics))
    //      .print()

    val stream = env.socketTextStream("ruozedata001", 9999)
    stream.flatMap(_.split(",")).map((_, 1))
      .keyBy(0)
      .reduce((x, y) => (x._1, x._2 + y._2)) // sum
      .print
  }

  def filter(env: StreamExecutionEnvironment): Unit = {
    val stream = env.readTextFile("data/access.log")
    val accessStream = stream.map(x => {
      val splits = x.split(",")
      Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
    })

    //accessStream.filter(x => x.traffics > 4000).print()

    //    accessStream.filter(new RichFilterFunction[Access] {
    //      override def filter(value: Access): Boolean = value.traffics > 4000
    //    }).print()

    accessStream.filter(new RuozedataFilter(4000)).print()
  }


  def map(env: StreamExecutionEnvironment): Unit = {
    val stream = env.readTextFile("data/access.log")
    //    stream.map(x => {
    //      val splits = x.split(",")
    //      Access(splits(0).trim.toLong, splits(1).trim,splits(2).trim.toLong)
    //    }).print()

    stream.map(new RichMapFunction[String, Access] {

      override def open(parameters: Configuration): Unit = {
        println("........open.invoked..........")
      }

      override def close(): Unit = super.close()

      override def map(value: String): Access = {
        val splits = value.split(",")
        Access(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
      }
    }).print()

  }

}
