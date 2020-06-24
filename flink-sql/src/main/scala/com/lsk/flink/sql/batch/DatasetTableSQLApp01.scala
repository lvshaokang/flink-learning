package com.lsk.flink.sql.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

object DatasetTableSQLApp01 {
  
  def main(args: Array[String]): Unit = {
  
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)
  
    val dataset = env.readTextFile("data/access.log")
      .map(x => {
        val splits = x.split(",")
  
        AccessLog(splits(0).trim.toLong, splits(1).trim, splits(2).trim.toLong)
      })
  
    val table = tableEnv.fromDataSet(dataset)
    
    tableEnv.createTemporaryView("batch_access",table)
    
//    val sql = "select * from batch_access"
//    val resultTable = tableEnv.sqlQuery(sql)
//    resultTable.printSchema()
  
    // SQL API
//    val sql = "select domain,sum(traffics) as traffics from batch_access group by domain"
//    val resultTable = tableEnv.sqlQuery(sql)
//    tableEnv.toDataSet[Row](resultTable).print()
  
    val resultTable = table.groupBy("domain")
      .aggregate("sum(traffics) as traffics")
      .select("domain, traffics")
      .orderBy("traffics.desc")
  
    tableEnv.toDataSet[Row](resultTable).print()
  }
  
  case class AccessLog(time:Long, domain:String, traffics:Long)
  
}
