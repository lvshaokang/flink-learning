package com.lsk.flink.state.chk

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

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

    // 开启chk,只需要这一步
    env.enableCheckpointing(5000)

    // fixedDelayRestart 每5秒重启3次，跑4次才会挂
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)))

    // 未配置重启策略，如果socket中断，Job直接异常
    val stream = env.socketTextStream("localhost", 9999)

    stream.map(x => {
      if (x.contains("pk")) {
        throw new RuntimeException("trigger error...")
      } else {
        x.toLowerCase()
      }
    }).flatMap(_.split(","))
    .map((_,1))
    .keyBy(0)
    .sum(1)
    .print()

    // chk 默认 memory，程序抛出运行时异常时，value会累加，但是如果程序挂了，原来的历史状态会丢失

    env.execute(this.getClass.getSimpleName)

  }

}
