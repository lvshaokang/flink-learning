package com.lsk.bigdata.flink.join.dimjoin

import com.alibaba.fastjson.JSON
import com.lsk.bigdata.flink.join.util.KafkaConfigUtil
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.types.Row

import java.nio.charset.StandardCharsets


/**
 * Temporal table function join (Temporal table 是持续变化表上某一时刻的视图,
 *  Temporal table function 是一个表函数,传递一个时间参数, 返回Temporal table 这一指定时刻的视图)
 *  可以将维度数据流映射为Temporal table,主流与这个Temporal table进行关联,可以关联到某一版本(历史上某一个时刻)的维度数据
 *
 * 模拟数据从Kafka加载
 *  EventTime
 *
 * 优点: 维度数据量可以很大,维度数据更新及时,不依赖外部存储,可以关联不同版本的维度数据
 *
 * 缺点: 仅支持Flink SQL API
 *
 * User
 * userName String,
 * cityId Int,
 * timestamp Long
 *
 * City
 * cityId Int,
 * cityName String,
 * timestamp Long
 */
object DimJoinApp04C {
  
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
    val tableEnv = StreamTableEnvironment.create(env, bsSettings)
    
//    env.setParallelism(1)

    val userConsumer = new FlinkKafkaConsumer[User]("user_topic", new UserDeserializationSchema, KafkaConfigUtil.buildConsumerProperties("user_group"))
    userConsumer.setStartFromEarliest()
    userConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[User](Time.seconds(10)) {
      override def extractTimestamp(element: User): Long = element.ts
    })
    val userStream = env.addSource(userConsumer)

    val cityConsumer = new FlinkKafkaConsumer[City]("city_topic", new CityDeserializationSchema, KafkaConfigUtil.buildConsumerProperties("city_group"))
    userConsumer.setStartFromEarliest()
    cityConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[City](Time.seconds(10)) {
      override def extractTimestamp(element: City): Long = element.ts
    })
    val cityStream = env.addSource(cityConsumer)


    val userTable = tableEnv.fromDataStream(userStream, 'user_name ,'city_id, 'ts.rowtime)
    // cityTable是维表
    val cityTable = tableEnv.fromDataStream(cityStream, 'city_id, 'city_name, 'ts.rowtime)
    
    tableEnv.createTemporaryView("userTable", userTable)
    tableEnv.createTemporaryView("cityTable", cityTable)

    // 定义一个TemporalTableFunction
    val dimCity = cityTable.createTemporalTableFunction('ts, 'city_id)
    // 注册表函数
    tableEnv.registerFunction("dimCity", dimCity)

    val selectUser = tableEnv.sqlQuery("select * from userTable")
    selectUser.printSchema()
    tableEnv.toAppendStream[Row](selectUser)
      .print("用户流接受到: ")

    val selectCity = tableEnv.sqlQuery("select * from cityTable")
    selectCity.printSchema()
    tableEnv.toAppendStream[Row](selectCity)
      .print("城市流接受到: ")

    val result = tableEnv.sqlQuery(
      s"""
         | select u.user_name, u.city_id, d.city_name, u.ts
         | from ${userTable} as u,
         | Lateral table (dimCity(u.ts)) d
         | where u.city_id = d.city_id
         | """.stripMargin)

    val resultDs = tableEnv.toAppendStream[Row](result)

    resultDs.print("关联输出: ")

    env.execute(this.getClass.getSimpleName)
  }

  case class User(userName: String, cityId: Int, ts: Long)

  case class City(cityId: Int, cityName: String, ts: Long)

  class CityDeserializationSchema extends DeserializationSchema[City] {
    override def deserialize(message: Array[Byte]): City = {
      val jsonStr = new String(message, StandardCharsets.UTF_8)
      val user = JSON.parseObject(jsonStr, classOf[City])
      user
    }

    override def isEndOfStream(nextElement: City): Boolean = false

    override def getProducedType: TypeInformation[City] = createTypeInformation[City]
  }

  class UserDeserializationSchema extends DeserializationSchema[User] {
    override def deserialize(message: Array[Byte]): User = {
      val jsonStr = new String(message, StandardCharsets.UTF_8)
      val user = JSON.parseObject(jsonStr, classOf[User])
      user
    }

    override def isEndOfStream(nextElement: User): Boolean = false

    override def getProducedType: TypeInformation[User] = createTypeInformation[User]
  }
}
