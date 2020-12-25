package com.lsk.flink.state.chk

import com.lsk.flink.state.chk.KeyedStateApp02.MapWithCounter
import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ReducingState, ReducingStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._


/**
 * TODO: 
 *
 * @author red
 * @class_name OperatorStateApp02
 * @date 2020-12-24
 */
object OperatorStateApp02 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    env.execute(this.getClass.getSimpleName)
  }

  class MyFunction[T] extends MapFunction[T,T] with CheckpointedFunction {

    private var countPerKey:ReducingState[Long] = _

    private var countPerPartition:ListState[Long] = _

    private var localCount:Long = _

    override def initializeState(context: FunctionInitializationContext): Unit = {
      countPerKey = context.getKeyedStateStore().getReducingState(
        new ReducingStateDescriptor[Long]("perKeyCount", new AddFunction(), createTypeInformation[Long])
      )
      countPerPartition = context.getOperatorStateStore().getListState(
        new ListStateDescriptor[Long]("perPartitionCount", createTypeInformation[Long])
      )
      for (l <- countPerPartition.get().asScala) {
        localCount += l
      }
    }

    override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
      countPerPartition.clear()
      countPerPartition.add(localCount)
    }

    override def map(value: T): T = {
      countPerKey.add(1L)
      localCount += 1
      value
    }
  }
  
  class AddFunction extends ReduceFunction[Long] {
    override def reduce(value1: Long, value2: Long): Long = null
  }

}
