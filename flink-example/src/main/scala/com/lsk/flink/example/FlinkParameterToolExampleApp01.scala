package com.lsk.flink.example

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.java.utils.ParameterTool


/**
 * TODO: 
 *
 * @author red
 * @class_name FlinkParameterToolExampleApp01
 * @date 2020-10-10
 */
object FlinkParameterToolExampleApp01 {

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

}
