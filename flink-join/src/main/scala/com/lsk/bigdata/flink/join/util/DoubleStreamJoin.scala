/*
package com.lsk.bigdata.flink.join.util

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
 * TODO: 
 *
 * @author red
 * @class_name DoubleStreamJoin
 * @date 2021-01-05
 */
object DoubleStreamJoin {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val consumerBigOrder = new FlinkKafkaConsumer[String]("big_order_topic_name",
      new SimpleStringSchema(),
      KafkaConfigUtil.buildConsumerProperties("KAFKA_CONSUMER_GROUP_ID"))
      .setStartFromGroupOffsets()

    val bigOrderStream = env.addSource(consumerBigOrder)
      .uid("topic_1") // 有状态的算子一定要配置uid
      .filter(x => x != null)
      .map(line => JSON.parseObject(line, classOf[Order]))
      // 提取 EventTime Watermarks
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(60)) {
          override def extractTimestamp(order: Order): Long =
            order.time
        })
      .keyBy(_.orderId)

    // 小订单处理逻辑与大订单完全一样
    val consumerSmallOrder = new FlinkKafkaConsumer[String]("small_order_topic_name",
      new SimpleStringSchema(),
      KafkaConfigUtil.buildConsumerProperties("KAFKA_CONSUMER_GROUP_ID"))
      .setStartFromGroupOffsets()

    val smallOrderStream = env.addSource(consumerSmallOrder)
      .uid("topic_1") // 有状态的算子一定要配置uid
      .filter(x => x != null)
      .map(line => JSON.parseObject(line, classOf[Order]))
      // 提取 EventTime Watermarks
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(60)) {
          override def extractTimestamp(order: Order): Long =
            order.time
        })
      .keyBy(_.orderId)

    val bigOrderTag: OutputTag[Order] = new OutputTag[Order]("bigOrder")
    val smallOrderTag: OutputTag[Order] = new OutputTag[Order]("smallOrder")

    val resStream = bigOrderStream
      .connect(smallOrderStream)
      .process(new CoProcessFunction[Order, Order, (Order, Order)] {
        // 大订单数据先来了，将大订单数据保存在 bigState 中
        var bigState: ValueState[Order] = _
        // 小订单数据先来了，将小订单数据保存在 smallState 中
        var smallState: ValueState[Order] = _

        var timerState: ValueState[Long] = _

        // 处理大订单
        override def processElement1(bigOrder: Order, ctx: CoProcessFunction[Order, Order, (Order, Order)]#Context, out: Collector[(Order, Order)]): Unit = {
          val smallOrder = smallState.value()
          // smallOrder 不为空表示小订单先来了,直接将大小订单拼接发送到下游
          if (smallOrder != null) {
            out.collect((smallOrder, bigOrder))
            // 清空小订单对应的 State 信息
            smallState.clear()
            // 这里可以将 Timer 清除,因为两个流都到了,没必要再触发 onTimer 了
            ctx.timerService().deleteEventTimeTimer(timerState.value())
            timerState.clear()
          } else {
            // 小订单还没来,将大订单放到状态中,并注册1分钟触发的 timer
            bigState.update(bigOrder)
            // 1 分钟后触发定时器,当前的 eventTime + 60s
            val time = bigOrder.time + 60000
            timerState.update(time)
            ctx.timerService().registerEventTimeTimer(time)
          }
        }


        // 处理小订单,同逻辑
        override def processElement2(smallOrder: Order, ctx: CoProcessFunction[Order, Order, (Order, Order)]#Context, out: Collector[(Order, Order)]): Unit = {
          val bigOrder = bigState.value()
          // bigOrder 不为空表示大订单先来了,直接将大小订单拼接发送到下游
          if (bigOrder != null) {
            out.collect((smallOrder, bigOrder))
            bigState.clear()
            // 这里可以将 Timer 清除,因为两个流都到了,没必要再触发 onTimer 了
            ctx.timerService().deleteEventTimeTimer(timerState.value())
            timerState.clear()
          } else {
            // 大订单还没来,将小订单放到状态中,并注册1分钟触发的 timer
            smallState.update(smallOrder)
            // 1 分钟后触发定时器,当前的 eventTime + 60s
            val time = smallOrder.time + 60000
            timerState.update(time)
            ctx.timerService().registerEventTimeTimer(time)
          }
        }

        override def onTimer(timestamp: Long, ctx: CoProcessFunction[Order, Order, (Order, Order)]#OnTimerContext, out: Collector[(Order, Order)]): Unit = {
          // 定时器触发了,即 1 分钟内没有接收到两个流
          if (bigState.value() != null) {
            ctx.output(bigOrderTag, bigState.value())
          }

          if (smallState.value() != null) {
            ctx.output(smallOrderTag, smallState.value())
          }
          bigState.clear()
          smallState.clear()
        }

        override def open(parameters: Configuration): Unit = {
          super.open(parameters)

          bigState = getRuntimeContext.getState(new ValueStateDescriptor[Order]("bigState", classOf[Order]))
          smallState = getRuntimeContext.getState(new ValueStateDescriptor[Order]("smallState", classOf[Order]))
        }
      })

    // 生产环境sink出去
    resStream.print()

    // 只有大订单时,没匹配到 小订单,属于异常数据,需要保存到外部系统,进行特殊处理
    resStream.getSideOutput(bigOrderTag).print()

    // 只有小订单时,没匹配到 大订单,属于异常数据,需要保存到外部系统,进行特殊处理
    resStream.getSideOutput(smallOrderTag).print()

    env.execute(this.getClass.getSimpleName)
  }


  case class Order(time: Long, orderId: String, userId: String, goodsId: Int, price: Int, cityId: Int)

}
*/
