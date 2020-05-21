package com.ruozedata.bigdata.base

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.util.NumberSequenceIterator

/**
 * fromCollection/socketTextStream/fromElements 是单并行度
 * fromParallelCollection/readTextFile 是多并行度
 * 自定义Source：SourceFunction/RichParallelSourceFunction/ParallelSourceFunction
 *
 */
object SourceApp {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val stream = env.addSource(new AccessSource03).setParallelism(3)
//    val stream = env.addSource(new RuozedataMySQLSource) //.setParallelism(3)
    val stream = env.addSource(new RuozedataMySQLScalikeJDBCSource) //.setParallelism(3)
    println("....." + stream.parallelism) //?
    stream.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }

  def fromTextFile(env: StreamExecutionEnvironment): Unit = {

    val stream = env.readTextFile("data/wc.txt")
    println("....." + stream.parallelism) //?

    val mapStream = stream.map(x => x + ".ruozedata").setParallelism(2)
    println("....." + mapStream.parallelism) // ?
    mapStream.print()

  }

  def fromParallelCollection(env: StreamExecutionEnvironment): Unit = {
    val stream = env.fromParallelCollection(new NumberSequenceIterator(1, 10))
    println("....." + stream.parallelism) //?

    val filterStream = stream.filter(_ > 5)
    println("....." + filterStream.parallelism) // ?
    filterStream.print()
  }

  def fromElements(env: StreamExecutionEnvironment): Unit = {
    val stream = env.fromElements(1, "2", 3L, 4D, 5F)
    println("....." + stream.parallelism) //?

    val mapStream = stream.map(x => x)
    println("....." + mapStream.parallelism) // ? 4
    mapStream.print()
  }

  def fromSocket(env: StreamExecutionEnvironment): Unit = {
    val stream = env.socketTextStream("ruozedata001", 9999)
    println("....." + stream.parallelism) //? 4

    val filterStream = stream.flatMap(_.split(",")).filter(_.trim != "b")
    println("....." + filterStream.parallelism) // ? 4

    filterStream.print()

  }

  def fromCollection(env: StreamExecutionEnvironment): Unit = {
    val stream = env.fromCollection(List(
      Access(202010080010L, "ruozedata.com", 2000),
      Access(202010080010L, "ruoze.ke.qq.com", 6000),
      Access(202010080010L, "google.com", 5000),
      Access(202010080010L, "ruozedata.com", 4000),
      Access(202010080010L, "ruoze.ke.qq.com", 1000)
    ))

    println("....." + stream.parallelism) // 1

    val filterStream = stream.filter(_.traffics > 4000)
    println("....." + filterStream.parallelism) //4

    filterStream.print()
  }
}
