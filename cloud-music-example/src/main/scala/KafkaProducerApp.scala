import java.io.{BufferedReader, File, FileReader}
import java.util.Properties
import java.util.concurrent.Executors

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
 * TODO: 
 *
 * @author red
 * @class_name KafkaProducerApp
 * @date 2020-10-11
 */
object KafkaProducerApp {

  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", classOf[StringSerializer])
    prop.put("value.serializer", classOf[StringSerializer])

    val executor = Executors.newFixedThreadPool(2)

    executor.submit {
      val runnable: Runnable = () => {
        val userAction = new File("file0")
        val producer = new KafkaProducer[String, String](prop)
        val reader = new BufferedReader(new FileReader(userAction))
        var line = reader.readLine()
        while (line != null) {
          producer.send(new ProducerRecord("UserAction", line))
          line = reader.readLine()
          Thread.sleep(10000)
        }
        reader.close()
        producer.close()
      }
      runnable
    }

    executor.submit {
      val runnable: Runnable = () => {
        val binlogFile = new File("file1")
        val producer = new KafkaProducer[String, String](prop)
        val reader = new BufferedReader(new FileReader(binlogFile))
        var line = reader.readLine()
        while (line != null) {
          producer.send(new ProducerRecord("BinLog", line))
          line = reader.readLine()
          Thread.sleep(10000)
        }
        reader.close()
        producer.close()
      }
      runnable
    }

    executor.shutdown()
  }

}
