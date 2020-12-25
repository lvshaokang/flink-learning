package com.lsk.flink.state.chk

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Checkpoint定位:
 *  保证Flink作业在意外崩溃重启的时候,不影响Exactly Once一致性的保障,
 *  通常情况下,是配合作业重启的策略去使用的
 */
object ChkApp02 {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    setCheckpointConfig(env)

    val port = 9000
    val hostname = "localhost"
    val socket = env.socketTextStream(hostname, port, '\n')

    val windowCounts  = socket.flatMap(new FlatMapFunction[String, WordWithCount] {
      override def flatMap(value: String, out: Collector[WordWithCount]): Unit = {
        val splits = value.split("\\s")
        for (word <- splits)
          out.collect(WordWithCount(word, 1L))
      }
    }).keyBy(_.word)
      .timeWindow(Time.seconds(2), Time.seconds(1))
      .sum("count")

    windowCounts.print().setParallelism(1)

    env.execute(this.getClass.getSimpleName)
    
    // System.getProperties().get("user.dir") D:\lamp\SourceCode\bigdata\flink-learning
  }
  
  def setCheckpointConfig(env: StreamExecutionEnvironment): Unit = {
    // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(1000)
  
    // 设置模式为exactly-once （这是默认值）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
  
    // 表示一旦flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
  
    //设置statebackend
    env.setStateBackend(new FsStateBackend("file:///" + System.getProperty("user.dir") + "/flink-state/checkpoints/"))
  
  }
  
  case class WordWithCount(word: String, count: Long)
  
}
