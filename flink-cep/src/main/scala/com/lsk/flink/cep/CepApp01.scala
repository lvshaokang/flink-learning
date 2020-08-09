package com.lsk.flink.cep

import java.util

import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * TODO: 
 *
 * @author red
 * @class_name CepApp01
 * @date 2020-07-19
 */
object CepApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    val stream = env.socketTextStream("localhost", 9999)

    val stream = env.fromCollection(Seq(
      ("1,bob,click"),
      ("1,bob,buy"),
      ("2,tom,click"),
      ("3,lsk,click"),
      ("3,lsk,buy")
    ))


    val etlStream = stream.map(x => {
      val splits = x.split(",")

      Access(splits(0), splits(1), splits(2))
    }).keyBy(0)

    // 定义规则
    // 用户click后马上进行buy操作
    val pattern = Pattern.begin[Access]("start").where(x => {
      x.operation == "click"
    }).next("middle").where(x => {
      x.operation == "buy"
    })


    val pStream = CEP.pattern(etlStream, pattern)

    pStream.process(new PatternProcessFunction[Access,String] {
      override def processMatch(`match`: util.Map[String, util.List[Access]], ctx: PatternProcessFunction.Context, out: Collector[String]): Unit = {
        val click = `match`.get("start").iterator().next()
        val buy = `match`.get("middle").iterator().next()

        val value = s"click => ${click.uid}, buy => ${buy.uid}"
        out.collect(value)
      }
    }).print()

    env.execute(this.getClass.getSimpleName)
  }

  case class Access(uid: String,
                    name: String,
                    operation: String)

}
