package com.lsk.flink.state.chk

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaConsumerBase, KafkaTableSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.nio.charset.{Charset, StandardCharsets}
import java.util.Properties

/**
 * Operator 可以含有多个不同名称的广播状态
 * 1.动态规则
 * 2.数据丰富
 *
 * actions (1,下单) (2,登录) (1,支付) (2,登出) (3,加入购物车)
 * patterns (登录,登出) (下单,支付)
 * emit 匹配到规则的 userId
 */
object BroadcastStateApp01 {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
  
    // 用户行为
    val actions = env.addSource(new FlinkKafkaConsumer[Action](
      "action-topic", new ActionEventSchema(), new Properties()
    ))
  
    // 规则
    val patterns = env.addSource(new FlinkKafkaConsumer[Pattern](
      "pattern-topic", new PatternEventSchema(), new Properties()))
    
    val actionsByUser = actions.keyBy(_.userId)
  
    val bcStateDescriptor = new MapStateDescriptor[Void, Pattern]("patterns", createTypeInformation[Void], createTypeInformation[Pattern])
  
    // 广播出去
    val bcedPatterns = patterns.broadcast(bcStateDescriptor)
  
    val matches = actionsByUser.connect(bcedPatterns)
      .process(new PatternEvaluator())
  
    matches.print()
    
    env.execute(this.getClass.getSimpleName)
  }
  
  case class Action(userId: Long, action: String)
  
  case class Pattern(firstAction: String, secondAction: String)
  
  class ActionEventSchema extends DeserializationSchema[Action] with SerializationSchema[Action] {
  
    override def serialize(element: Action): Array[Byte] = element.toString.getBytes(StandardCharsets.UTF_8)
    
    override def deserialize(message: Array[Byte]): Action = JSON.parseObject(message, classOf[Action])
  
    override def isEndOfStream(nextElement: Action): Boolean = false
  
    override def getProducedType: TypeInformation[Action] = createTypeInformation[Action]
  }
  
  class PatternEventSchema extends DeserializationSchema[Pattern] with SerializationSchema[Pattern] {
    
    override def serialize(element: Pattern): Array[Byte] = element.toString.getBytes(StandardCharsets.UTF_8)
    
    override def deserialize(message: Array[Byte]): Action = JSON.parseObject(message, classOf[Pattern])
    
    override def isEndOfStream(nextElement: Pattern): Boolean = false
    
    override def getProducedType: TypeInformation[Pattern] = createTypeInformation[Pattern]
  }
  
  class PatternEvaluator extends KeyedBroadcastProcessFunction[Long, Action, Pattern, (Long, Pattern)] {
    
    private var preActionState: ValueState[String] = _
    
    private var patternDesc: MapStateDescriptor[Void, Pattern] = _
  
    override def open(parameters: Configuration): Unit = {
      preActionState = getRuntimeContext.getState(
        new ValueStateDescriptor[String]("lastActions", createTypeInformation[String]))
      
      patternDesc = new MapStateDescriptor[Void, Pattern]("patterns", createTypeInformation[Void], createTypeInformation[Pattern])
    }
  
    override def processElement(value: Action, ctx: KeyedBroadcastProcessFunction[Long, Action, Pattern, (Long, Pattern)]#ReadOnlyContext, out: Collector[(Long, Pattern)]): Unit = {
      val pattern = ctx.getBroadcastState(patternDesc).get(null)
      val preAction = preActionState.value()
      if (pattern != null && preAction != null) {
        if (pattern.firstAction.equals(preAction) && pattern.secondAction.equals(value.action)) {
          // match
          out.collect((ctx.getCurrentKey, pattern))
        }
      }
      preActionState.update(value.action)
    }
  
    override def processBroadcastElement(value: Pattern, ctx: KeyedBroadcastProcessFunction[Long, Action, Pattern, (Long, Pattern)]#Context, out: Collector[(Long, Pattern)]): Unit = {
      val bcState = ctx.getBroadcastState(patternDesc)
      bcState.put(null, value)
    }
  }
}
