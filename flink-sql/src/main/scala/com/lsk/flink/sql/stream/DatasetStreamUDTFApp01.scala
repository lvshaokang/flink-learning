package com.lsk.flink.sql.stream

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
import org.apache.flink.types.Row

object DatasetStreamUDTFApp01 {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    
    val input = env.socketTextStream("node11",9999)
    
    tableEnv.createTemporaryView("input",input, 'line)
    
    tableEnv.registerFunction("split", new Split())
    
    val resultTable = tableEnv.sqlQuery(
      """
        |select
        |word
        |from
        |input, lateral table(split(line)) as t(word)
        |""".stripMargin)
    
    tableEnv.toAppendStream[Row](resultTable).print()
    
    
    env.execute(this.getClass.getSimpleName)
  }
  
  // [String] 传进来的参数类型
  class Split(separator: String) extends TableFunction[String] {
    
    def this() {
      this(",")
    }
    
    def eval(input: String) = {
      input.split(separator)
        .foreach(x=>collect(x))
    }
  
    override def getResultType: TypeInformation[String] = {
      Types.STRING
    }
  }
}
