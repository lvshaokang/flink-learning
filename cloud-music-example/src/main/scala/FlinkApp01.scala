import org.apache.flink.api.common.functions.MapFunction

import java.util.{Date, Properties}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.flink.streaming.api.scala._

import java.text.SimpleDateFormat
import scala.collection.JavaConversions._

/**
 * 业务背景
 * 1.binLog日志来自两张日志表,用户使用历史日推/用户使用人气榜 (增,log型)
 *
 * @author red
 * @class_name FlinkApp01
 * @date 2020-11-29
 */
object FlinkApp01 {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", classOf[StringSerializer])
    prop.put("value.serializer", classOf[StringSerializer])

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val binLogConsumer = new FlinkKafkaConsumer[BinLogData]("BinLog", new BinLogDeserializer, prop)
    val userActionConsumer = new FlinkKafkaConsumer[UserActionData]("UserAction", new UserActionDeserializer, prop)

    val binLogStream = env.addSource(binLogConsumer)
    val userActionStream = env.addSource(userActionConsumer)

    // 用户ID,权限信息,附加信息
    val binLogTupleStream: DataStream[(Long, String, String)] = binLogStream.map(new MapFunction[BinLogData, (Long, String, String)] {

      val format = new SimpleDateFormat("yyyy-MM-dd")

      override def map(binLogData: BinLogData): (Long, String, String) = {
        if (binLogData.changeType != "INSERT") {
          null
        }
        var userId: Long = null
        var targetData: String = null

        if (binLogData.tableName == "Music_FansContributionRecord") {
          for (changeColumn <- binLogData.changeList) {
            if (changeColumn.columnName == "Id") {
              userId = changeColumn.newValue.toLong
            } else if (changeColumn.columnName == "Type") {
              targetData = changeColumn.newValue
            }
          }
        } else if (binLogData.tableName == "Music_HistorySongRecommend") {
          for (changeColumn <- binLogData.changeList) {
            if (changeColumn.columnName == "UserId") {
              userId = changeColumn.newValue.toLong
            } else if (changeColumn.columnName == "RecommendTime") {
              val timestamp = changeColumn.newValue.toLong
              targetData = format.format(new Date(timestamp))
            }
          }
        } else {
          return null
        }

        (userId, binLogData.tableName, targetData)
      }
    }).filter(x => x != null)

    binLogTupleStream.print()

    env.execute(this.getClass.getSimpleName)
  }

}
