package com.lsk.flink.sql.stream

import java.lang

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, TableSchema}
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
    
    val table = tableEnv.fromDataStream(wc)
//    tableEnv.createTemporaryView("access",table)

//    val fields[].var
//    val fieldNames = Array("word","cnt")
//    val fieldTypes = Array(Types.STRING,Types.LONG)
  
    tableEnv.registerTableSink("upsert",new WordCountUpsertStreamTableSink)
  
    val resultTable = tableEnv.sqlQuery(
      s"""
         |select word,count(1) as cnt from ${table} group by word
         |""".stripMargin)
  
    resultTable.toRetractStream[Row].print()
    
    env.execute(this.getClass.getSimpleName)
  }
  
  class WordCountUpsertStreamTableSink() extends UpsertStreamTableSink[Row] {
    
    var keys: Array[String] = _
    
    var isAppendOnly: lang.Boolean = _
  
    var fieldNames:Array[String]=_
  
    var fieldTypes:Array[TypeInformation[_]]=_
  
    override def getFieldNames: Array[String] = Array("word","cnt")
  
    override def getFieldTypes: Array[TypeInformation[_]] = Array(Types.STRING,Types.LONG)
  
    override def setKeyFields(keys: Array[String]): Unit = {
      this.keys = keys
    }
  
    override def setIsAppendOnly(isAppendOnly: lang.Boolean): Unit = {
      this.isAppendOnly = isAppendOnly
    }
  
  
    override def getRecordType: TypeInformation[Row] = {
      new RowTypeInfo(fieldTypes,fieldNames)
    }
  
    override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit = {
      dataStream.print()
    }
  
    override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[tuple.Tuple2[lang.Boolean, Row]] = {
      this.fieldNames=fieldNames
  
      this.fieldTypes=fieldTypes
  
      this
    }
  }
  
  case class Word(word:String)
}
