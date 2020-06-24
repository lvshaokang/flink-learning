package com.lsk.flink.sql.stream

import com.lsk.flink.utils.IpUtil
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object DatasetStreamUDFApp01 {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    
    val input = env.socketTextStream("node11",9999)
    
    tableEnv.createTemporaryView("input",input, 'ip)
    
    tableEnv.registerFunction("ip_parse", new IPParse)
    
    val resultTable = tableEnv.sqlQuery("select ip, ip_parse(ip) from input")
    
    tableEnv.toAppendStream[Row](resultTable).print()
    
    
    env.execute(this.getClass.getSimpleName)
  }
  
  class IPParse extends ScalarFunction {
    
    def eval(ip: String) = {
      val str = IpUtil.getRegionByIp(ip)
      val splits = str.split("\\|")
      
      splits(2) + "-" +splits(3)
    }
    
    override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
      Types.STRING
    }
  }
}
