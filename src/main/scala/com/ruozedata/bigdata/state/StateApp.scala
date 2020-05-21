package com.ruozedata.bigdata.state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object StateApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //重启策略：重启三次 每次间隔5秒 超过指定次数后作业退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.of(5,TimeUnit.SECONDS)))

    val stream = env.socketTextStream("hadoop001",9999)

    stream.print()

    env.execute(this.getClass.getSimpleName)
  }


}