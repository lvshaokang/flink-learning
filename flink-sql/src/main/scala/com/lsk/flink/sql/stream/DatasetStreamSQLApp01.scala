package com.lsk.flink.sql.stream

import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object DatasetStreamSQLApp01 {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
  
    val stream = env.readTextFile("data/access.log")
      .map(x => {
        val splits = x.split(",")
      
        AccessLog(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
      })
  
    // toAppendStream
//    val table = tableEnv.fromDataStream(stream)
//    tableEnv.createTemporaryView("access", table)
//    val resultTable = tableEnv.sqlQuery("select * from access")
//    tableEnv.toAppendStream[Row](resultTable).print()

//    val table = tableEnv.fromDataStream(stream)
//    val resultTable = table.select('time,'domain,'traffics)
//    tableEnv.toAppendStream[Row](resultTable).print()

    // alias
//    val table = tableEnv.fromDataStream(stream, 'a,'b,'c)
//    table.printSchema()
  
    // agg -> toRetractStream
    val table = tableEnv.fromDataStream(stream)
    tableEnv.createTemporaryView("access", table)
  
    val resultTable = tableEnv.sqlQuery("select domain,sum(traffics) as traffics from access group by domain")
    tableEnv.toRetractStream[Row](resultTable).print()
    
    /*
      2> (true,ruozedata.com,4000)
      1> (true,ruoze.ke.com,4000)
      1> (false,ruoze.ke.com,4000)
      1> (true,ruoze.ke.com,10000)
      2> (false,ruozedata.com,4000)
      2> (true,ruozedata.com,6000)
      1> (true,google.com,2000)
      
      结果中 true 代表 insert, false 代表 delete
     */
    
    env.execute(this.getClass.getSimpleName)
  }
  
  case class AccessLog(time:Long, domain:String, traffics:Long)
}
