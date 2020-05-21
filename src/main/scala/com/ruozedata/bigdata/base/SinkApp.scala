package com.ruozedata.bigdata.base

import java.sql.{Connection, PreparedStatement}

import com.ruozedata.bigdata.utils.MySQLUtils
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

object SinkApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.readTextFile("data/access.log")
        .map(x=>{
          val splits = x.split(",")
          (splits(1).trim,splits(2).trim.toDouble)
        }).keyBy(0).sum(1)

    stream.addSink(new RuozedataMySQLSink)

//    class RedisExampleMapper extends RedisMapper[(String,Long)]{
//      override def getCommandDescription: RedisCommandDescription = {
//        new RedisCommandDescription(RedisCommand.HSET,"HASH_NAME")
//      }
//
//      override def getKeyFromData(data: (String, Long)): String = data._1
//
//      override def getValueFromData(data: (String, Long)): String = data._2+""
//    }
//
//    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop001").build()
//    stream.addSink(new RedisSink[(String,Long)](conf,new RedisExampleMapper))

    env.execute("FLINK")
  }
}

class RuozedataMySQLSink extends RichSinkFunction[(String,Double)]{

  var connecton:Connection = _
  var insertPsmt:PreparedStatement = _
  var updatePsmt:PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    connecton = MySQLUtils.getConnection()
    insertPsmt  =connecton.prepareStatement("insert into ruozedata_traffic(domain,traffic) values(?,?)")
    updatePsmt  =connecton.prepareStatement("update ruozedata_traffic set traffic = ? where domain = ?")
  }

  /**
   * 一个批次执行一次  基于Window
   */
  override def invoke(value: (String, Double), context: SinkFunction.Context[_]): Unit = {
    updatePsmt.setDouble(1,value._2)
    updatePsmt.setString(2,value._1)
    updatePsmt.execute()

    if(updatePsmt.getUpdateCount==0){
      insertPsmt.setString(1,value._1)
      insertPsmt.setDouble(2,value._2)
      insertPsmt.execute()
    }
  }

  override def close(): Unit = {
    insertPsmt.close()
    updatePsmt.close()
    connecton.close()
  }
}

