import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.flink.streaming.api.scala._

/**
 * TODO: 
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

    binLogStream.print()

    env.execute(this.getClass.getSimpleName)
  }

}
