package com.lsk.flink.state.chk

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * TODO: 
 *
 * @author red
 * @class_name ChkApp01
 * @date 2020-05-22
 */
object ChkApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 开启chk
    env.enableCheckpointing(10000)


    env.execute(this.getClass.getSimpleName)

  }

}
