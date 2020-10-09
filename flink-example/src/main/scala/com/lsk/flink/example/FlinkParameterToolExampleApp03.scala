package com.lsk.flink.example

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

import scala.collection.JavaConversions._
import scala.collection.immutable


/**
 * 利用广播变量动态更新配置
 *
 * @author red
 * @class_name FlinkParameterToolExampleApp01
 * @date 2020-10-10
 */
object FlinkParameterToolExampleApp03 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 注册全局参数
    // --os.name win10 --user.home home1
    env.getConfig.setGlobalJobParameters(ParameterTool.fromArgs(args))

    env.addSource(new RichSourceFunction[String] {
      override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
        while (true) {
          val parameterTool = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
          sourceContext.collect(System.currentTimeMillis + "\t" +  parameterTool.get("os.name") + "\t" + parameterTool.get("user.home"))
        }
      }

      override def cancel(): Unit = {}
    }).print()

    env.execute(this.getClass.getSimpleName)
  }

  def createParameterTool(args:Array[String]): ParameterTool = {
    ParameterTool.fromPropertiesFile(classOf[Nothing].getResourceAsStream("/application.properties"))
      .mergeWith(ParameterTool.fromArgs(args))
      .mergeWith(ParameterTool.fromSystemProperties)
      .mergeWith(ParameterTool.fromMap(getEnv())) // mergeWith 会使用最新的配置
  }

  def getEnv() = {
    immutable.Map[String,String](
      "setting1" -> "value1",
    "setting2" -> "value2")
  }
}
