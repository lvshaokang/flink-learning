package com.lsk.flink.state.chk

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * TODO: 
 *
 * @author red
 * @class_name KeyedStateApp02
 * @date 2020-12-24
 */
object KeyedStateApp02 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val ds = env.fromCollection(Seq(
      ("pk", "pk"),
      ("j", "j"),
      ("xingxing", "xingxing"),
      ("pk", "pk")
    ))

    val ds1 = ds.keyBy(0).map(new MapWithCounter())

    ds1.print()

    env.execute(this.getClass.getSimpleName)
  }

  class MapWithCounter extends RichMapFunction[(String, String), Long] {
    private var totalLengthByKey: ValueState[Long] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)

      val stateDescriptor = new ValueStateDescriptor[Long]("sum of length", TypeInformation.of(new TypeHint[Long] {}))
      totalLengthByKey = getRuntimeContext.getState(stateDescriptor)

    }

    override def map(value: (String, String)): Long = {
      val length = totalLengthByKey.value()
      val newTotalLength = length + value._1.length
      totalLengthByKey.update(newTotalLength)
      newTotalLength
    }
  }

}
