
package org.myorg.quickstart

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._


object BatchJob {

  def main(args: Array[String]) {
    // 获取批处理上下文环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 数据转正DataSet
    val text: DataSet[String] = env.readTextFile("data/wc.txt")
    // 业务逻辑  transformation
    text.flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()

    // execute program
//    env.execute("Flink Batch Scala API Skeleton")
  }
}
