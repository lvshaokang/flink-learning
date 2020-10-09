package com.lsk.flink.example

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * TODO: 
 *
 * @author red
 * @class_name FlinkJobSettingExampleApp01
 * @date 2020-10-09
 */
object FlinkJobSettingExampleApp01 {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val config = new Configuration()
    config.setString("name", "lsk")

    env.fromElements(
      "lsk", "lj", "tom", "pk"
    ).flatMap(new RichFlatMapFunction[String, (String,Int)] {
      var name:String = _

      override def open(parameters: Configuration): Unit = {
        name = parameters.getString("name", "defaultName")
      }

      override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
        val splits = value.toLowerCase().split("\\W+")

        for (split <- splits) {
          if (split.length > 0) {
            out.collect((split + name, 1))
          }
        }
      }
    }).withParameters(config) // 仅能在批程序中使用
      .print()


//    env.execute(this.getClass.getSimpleName)
  }


}
