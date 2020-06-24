package com.lsk.flink.sql.stream

import com.lsk.flink.sql.stream.DatasetStreamSQLApp02.Word
import com.lsk.flink.utils.IpUtil
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.types.Row

object DatasetStreamSQLApp02 {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
  
    val wc = env.fromElements("ruoze,ruoze,ruoze", "pk,pk", "xingxing,xingxing")
      .flatMap(_.split(",")).map(Word(_))
  
    // **api
    // toRetractStream
    //    tableEnv.fromDataStream(wc, 'word)
    //      .groupBy('word)
    //      .select('word,'word.count)
    //      .toRetractStream[Row]
    //      .filter(_._1)
    //      .print()
  
    // **sql
    val table = tableEnv.fromDataStream(wc)
    //    tableEnv.createTemporaryView("access",table)
  
    tableEnv.sqlQuery(
      s"""
         |select word,count(1) from ${table} group by word
         |""".stripMargin)
      .toRetractStream[Row]
      .filter(_._1)
      .print()
  
    env.execute(this.getClass.getSimpleName)
  }
  
  case class Word(word: String)
}
