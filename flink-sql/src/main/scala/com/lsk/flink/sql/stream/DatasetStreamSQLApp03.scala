package com.lsk.flink.sql.stream

import java.lang

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.table.sinks.{TableSink, UpsertStreamTableSink}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.TypeConversions._
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

    val fieldNames: Array[String] = Array("word", "cnt")
    val fieldTypes = Array(DataTypes.STRING, DataTypes.BIGINT)
    // 注册输出一个 TableSink
    tableEnv.registerTableSink("upsertStreamTable", new WordCountUpsertStreamTableSink(fieldNames, fieldTypes))

    // 业务处理
    val resultTable = tableEnv.sqlQuery(
      s"""
         |select word,count(1) as cnt from ${table} group by word
         |""".stripMargin)

    // 将表结果写入到 TableSink
    resultTable.insertInto("upsertStreamTable")

    env.execute(this.getClass.getSimpleName)
  }

  class WordCountUpsertStreamTableSink(fieldNames: Array[String], fieldTypes: Array[DataType]) extends UpsertStreamTableSink[Row] {

    override def setKeyFields(keys: Array[String]): Unit = {
      println("keys->" + keys.mkString(","))
    }

    override def setIsAppendOnly(isAppendOnly: lang.Boolean): Unit = {
      println("isAppendOnly->" + isAppendOnly)
    }

    override def getRecordType: TypeInformation[Row] = {
      val types = fromDataTypeToLegacyInfo(fieldTypes)
      new RowTypeInfo(types, fieldNames)
    }

    override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit = {
      consumeDataStream(dataStream)
    }

    override def consumeDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): DataStreamSink[_] = {
      // TODO: 业务处理
      dataStream.print()
    }

    override def getTableSchema: TableSchema = {
      TableSchema.builder().fields(fieldNames, fieldTypes).build()
    }

    override def configure(copyFieldNames: Array[String], copyFieldTypes: Array[TypeInformation[_]]): TableSink[tuple.Tuple2[lang.Boolean, Row]] = {
      new WordCountUpsertStreamTableSink(copyFieldNames, fromLegacyInfoToDataType(copyFieldTypes))
    }
  }

  case class Word(word: String)

}
