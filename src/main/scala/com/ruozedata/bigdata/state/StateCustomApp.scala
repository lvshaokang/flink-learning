//package com.ruozedata.bigdata.state
//
//import org.apache.flink.api.common.functions.RichFlatMapFunction
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.api.scala._
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.util.Collector
//
//object StateCustomApp {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//
//    averageKeyedValueState(env)
//
//    env.execute(this.getClass.getSimpleName)
//  }
//
//  /**
//   * 根据输入元素求平均数
//   * 只要到达两个元素就开始算
//   */
//
//  def averageKeyedValueState(executionEnvironment: StreamExecutionEnvironment): Unit ={
//    executionEnvironment.fromCollection(List(
//      (1,3),
//      (1,5),
//      (1,7),
//      (1,4),
//      (1,2)
//    )).keyBy(x=>{
//      x._1
//    }).flatMap(new CountWindowAverage).print()+
//
//  }
//
//
//}
//
//class CountWindowAverage extends RichFlatMapFunction[(Int,Int),(Int,Int)]{
//  private var sum:ValueState[Int,Int] = _
//
//  override def open(parameters: Configuration): Unit = {
//    sum = getRuntimeContext.getState(new ValueStateDescriptor[Int,Int]("average",createTypeInformation[Int,Int]))
//  }
//
//  override def flatMap(value: (Int, Int), out: Collector[(Int, Int)]): Unit = {
//    val tmpState = sum.value()
//
//
//    val currentState = if(null!=tmpState){
//      tmpState
//    }else{
//      (0,0)
//    }
//
//    val newState = (currentState._1+1,currentState._2+value._2)
//    //更新state的值
//    sum.update(newState)
//
//    if(newState._1>=2){
//      out.collect((value._1),newState._2/newState._1)
//      sum.clear()//更新后清空
//    }
//
//  }
//}