package com.lsk.flink.sql.stream

import java.lang

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.sinks.{TableSink, UpsertStreamTableSink}
import org.apache.flink.types.Row

/**
 * Upsert stream
 *
 */
object DatasetStreamSQLApp03 {
  
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
  
    val resultTable = tableEnv.sqlQuery(
      s"""
         |select word,count(1) from ${table} group by word
         |""".stripMargin)
  
//    resultTable.toRetractStream.
  
//    tableEnv.registerTableSink("upsert", new WordCountUpsertStreamTableSink)
//
//
//
//        .toRetractStream[Row]
//        .filter(_._1)
//        .print()
  
    env.execute(this.getClass.getSimpleName)
  }
  
  /*class WordCountUpsertStreamTableSink extends UpsertStreamTableSink[(String, Int)] {
    
    var keys: Array[String] = _
    
    var isAppendOnly: lang.Boolean = _
    
    override def setKeyFields(keys: Array[String]): Unit = {
      this.keys = keys
    }
  
    override def setIsAppendOnly(isAppendOnly: lang.Boolean): Unit = {
      this.isAppendOnly = isAppendOnly
    }
  
    override def getRecordType: TypeInformation[(String, Int)] = {
    
    }
  
    override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, (String, Int)]]): Unit = {
      dataStream.print()
    }
  
    override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[tuple.Tuple2[lang.Boolean, (String, Int)]] = ???
  }*/
  
  case class Word(word:String)
}
