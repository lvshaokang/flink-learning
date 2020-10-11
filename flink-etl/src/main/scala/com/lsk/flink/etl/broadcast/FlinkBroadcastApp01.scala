package com.lsk.flink.etl.broadcast

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConversions._

/**
 * 利用广播变量动态更新告警规则
 * <p>
 * DataSet
 *
 * @author red
 * @class_name FlinkBroadcastApp01
 * @date 2020-10-11
 */
object FlinkBroadcastApp01 {
  def main(args: Array[String]): Unit = {
    val parameterTool = ParameterTool.fromArgs(args)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val toBroadcast = env.fromElements(1, 2, 3)
    env.fromElements("a", "b")
      .map(new RichMapFunction[String,String] {

        var broadcastData: List[Int] = _

        override def open(parameters: Configuration): Unit = {
          broadcastData = getRuntimeContext.getBroadcastVariable("zhisheng").toList
        }

        override def map(value: String): String = {
          broadcastData.get(1) + value
        }
      }).withBroadcastSet(toBroadcast, "zhisheng")
      .print()

  }

}
