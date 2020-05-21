package org.myorg.quickstart

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._


/**
 * author：若泽数据-PK哥   
 * 交流群：545916944
 */
object SpecifyingKeysApp {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("ruozedata001", 9999)
    text.flatMap(_.split(","))
      .filter(_.nonEmpty)
      .map(WC(_,1))  // tuple caseclass class
      .keyBy(_.word)
      .sum("count")
      .print()

    env.execute(this.getClass.getSimpleName)
  }

  case class WC(word:String, count:Int)
}
