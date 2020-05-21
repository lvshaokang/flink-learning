package com.lsk.flink.streaming.base

import org.apache.flink.streaming.api.scala._

/**
 * 测试 nc 命令测试 nc -l -p 9999
 *
 */
object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.socketTextStream("localhost", 9999)
    println("...." + text.parallelism)

    import org.apache.flink.api.scala._

    text.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print() // cpu core
      .setParallelism(2)


    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
