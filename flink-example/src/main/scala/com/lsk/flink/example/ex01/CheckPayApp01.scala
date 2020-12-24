package com.lsk.flink.example.ex01

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}

import org.apache.flink.streaming.api.scala._

/**
 * TODO: 
 *
 * @author red
 * @class_name CheckPayApp01
 * @date 2020-11-29
 */
object CheckPayApp01 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val payEventTag = new OutputTag[String]("payEventTag-side") {}

    env.socketTextStream("localhost", 8888)


  }

  case class OrderEvent(userId: Long, action: String, orId:String, timestamp: Long)

  case class ReceiptEvent(orId: String, payChannel: String, timestamp: Long)





}


