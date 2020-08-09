package com.lsk.flink.state.chk

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * TODO: 
 *
 * @author red
 * @class_name StateApp01
 * @date 2020-06-26
 */
object KeyedStateApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    avgKeyedStateValueState(env)

    userBehaviorKeyedMapState(env)

    env.execute(this.getClass.getSimpleName)
  }

  /**
   * 1.根据输入元素求 avg
   * 2.只要到达两个元素就开始算
   *
   */
  def avgKeyedStateValueState(env: StreamExecutionEnvironment): Unit = {
    env.fromCollection(List(
      (1,3),
      (1,5),
      (1,7),
      (1,4),
      (1,2)
    )).keyBy(x => x._1)
      .flatMap(new CountWindowAvg)
      .print()


  }

  /**
   * 统计每个用户某种行为的操作次数
   */
  def userBehaviorKeyedMapState(env: StreamExecutionEnvironment): Unit = {
    // userid,behavior
    env.fromCollection(List(
      (1,"buy"),
      (1,"cart"),
      (1,"buy"),
      (1,"fav"),
      (2,"buy"),
      (2,"buy"),
      (2,"fav")
    )).keyBy(x => x._1)
      .flatMap(new UserBehaviorCnt)
      .print()
  }

  class UserBehaviorCnt extends RichFlatMapFunction[(Int,String),(Int,String,Int)] {

    /**
     * MapState[String,Int] （行为,次数）
     */
    private var behavior: MapState[String,Int] = _

    // TypeInformation.of(new TypeHint[String] {})
    override def open(parameters: Configuration): Unit = {
      val mapStateDescriptor = new MapStateDescriptor[String,Int](
        "behavior-state",
        TypeInformation.of(new TypeHint[String] {}),
        TypeInformation.of(new TypeHint[Int] {}))

      behavior = getRuntimeContext.getMapState(mapStateDescriptor)
    }

    override def flatMap(value: (Int, String), out: Collector[(Int, String, Int)]): Unit = {
      var behaviorCnt = 1

      if (behavior.contains(value._2)) {
        behaviorCnt = behavior.get(value._2) + 1
      }

      behavior.put(value._2, behaviorCnt)
      out.collect((value._1, value._2, behaviorCnt))
    }
  }


  class CountWindowAvg extends RichFlatMapFunction[(Int,Int),(Int,Int)] {

    /**
     * ValueState[(Int,Int)] （次数,和）
     */
    private var sum: ValueState[(Int,Int)] = _

    // createTypeInformation[(Int, Int)]
    override def open(parameters: Configuration): Unit = {
      val valueState = new ValueStateDescriptor[(Int, Int)]("avg-state", createTypeInformation[(Int, Int)])
      sum = getRuntimeContext.getState(valueState)
    }

    override def flatMap(value: (Int, Int), out: Collector[(Int, Int)]): Unit = {
      // 获取当前state
      val tmpState = sum.value()

      val curState = if (tmpState != null) {
        tmpState
      } else {
        // initial
        (0, 0)  // (次数,和)
      }

      val newState = (curState._1 + 1, curState._2 + value._2)
      // 更新state
      sum.update(newState)


      if (newState._1 >= 2) {
        out.collect((value._1, newState._2 / newState._1))
        sum.clear()
      }
    }
  }

}
