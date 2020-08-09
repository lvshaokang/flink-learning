package com.lsk.flink.state.chk

import java.util
import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._



/**
 * TODO: 
 *
 * @author red
 * @class_name StateApp01
 * @date 2020-06-26
 */
object OperatorStateApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    avgKeyedStateValueState(env)

    userBehaviorOperatorStateListCheckpointedState(env)

    env.execute(this.getClass.getSimpleName)
  }

  /**
   * 使用 ListCheckpointedState 来实现 buy 的次数
   */
  def userBehaviorOperatorStateListCheckpointedState(env: StreamExecutionEnvironment): Unit = {
    // userid,behavior
    env.fromCollection(List(
      (1,"buy"),
      (1,"cart"),
      (1,"buy"),
      (1,"fav"),
      (2,"buy"),
      (2,"fav"),
      (2,"fav")
    )).keyBy(x => x._1)
      .flatMap(new UserBehaviorCnt)
      .print()
  }

  // ListCheckpointed[Int] : 存储的数据类型
  class UserBehaviorCnt extends RichFlatMapFunction[(Int,String),(Int, String,Int)] with ListCheckpointed[Integer]{

    var userBuyBehaviorCnt = 0

    override def flatMap(value: (Int, String), out: Collector[(Int ,String, Int)]): Unit = {
      if (value._2 == "buy") {
        userBuyBehaviorCnt += 1
      }

      out.collect(value._1, "buy", userBuyBehaviorCnt)
    }

    override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Integer] = {
      Collections.singletonList(userBuyBehaviorCnt)
    }

    override def restoreState(state: util.List[Integer]): Unit = {
      for (ele <- state) {
        userBuyBehaviorCnt += 1
      }
    }
  }
}
