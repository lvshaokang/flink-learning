package com.lsk.bigdata.fink.kafka

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.google.gson.JsonParser
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala._

import scala.collection.JavaConversions._

/**
 * TODO: 
 *
 * @author red
 * @class_name HdfsSink
 * @date 2020-09-15
 */
object HdfsSink {

  def main(args: Array[String]): Unit = {
    val fieldDelimiter = ","

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 开启chk,只需要这一步
    env.enableCheckpointing(5000)

    // statebackend
    env.setStateBackend(new FsStateBackend("hdfs://kms-1:8020/checkpoint").asInstanceOf[StateBackend])

    // ???
    val config = env.getCheckpointConfig
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    // source
    val props = new Properties()
    props.setProperty("bootstrap.servers", "kms-2:9092,kms-3:9092,kms-4:9092")
    // only required for Kafka 0.8
    props.setProperty("zookeeper.connect", "kms-2:2181,kms-3:2181,kms-4:2181")
    props.setProperty("group.id", "test123")

    val consumer = new FlinkKafkaConsumer[String]("qfbap_ods.code_city", new SimpleStringSchema(), props)
    // 最早开始消费
    consumer.setStartFromEarliest()
    val stream = env.addSource(consumer)

    // transform
    val cityDS = stream.filter(new FilterFunction[String] {
      override def filter(value: String): Boolean = {
        val record = JsonParser.parseString(value).getAsJsonObject
        // 过滤掉DDL操作
        !record.get("isDdl").getAsBoolean
      }
    }).map(new MapFunction[String, String] {
      override def map(value: String): String = {
        val builder = new StringBuilder
        val record = JsonParser.parseString(value).getAsJsonObject
        val data = record.getAsJsonArray("data")

        for (i <- 0 until data.size) {
          val obj = data.get(i).getAsJsonObject

          if (obj != null) {
            builder.append(record.get("id").getAsLong) // 序号id
            builder.append(fieldDelimiter) // 字段分隔符
            builder.append(record.get("es").getAsLong) //业务时间戳
            builder.append(fieldDelimiter)
            builder.append(record.get("ts").getAsLong) // 日志时间戳
            builder.append(fieldDelimiter)
            builder.append(record.get("type").getAsString) // 操作类型
            for (entry <- obj.entrySet) {
              builder.append(fieldDelimiter)
              builder.append(entry.getValue) // 表字段数据
            }
          }
        }

        builder.toString
      }
    })

    // sink
    // 以下条件满足其中之一就会滚动生成新的文件
    val rollingPolicy = DefaultRollingPolicy.builder()
      .withRolloverInterval(60L * 1000L) // 滚动写入新文件的时间，默认60s。根据具体情况调节
      .withMaxPartSize(1024 * 1024 * 128L) // 设置每个文件的最大大小，默认是128M，这里设置为128M
      .withInactivityInterval(60L * 1000L) // 默认60秒，未写入数据处于不活跃状态超时会滚动新文件
      .build[String, String]()

    val sink = StreamingFileSink.forRowFormat[String](new Path("hdfs://kms-1:8020/binlog_db/code_city_delta"), new SimpleStringEncoder[String]())
      .withBucketAssigner(new EventTimeBucketAssigner())
      .withRollingPolicy(rollingPolicy)
      .withBucketCheckInterval(1000) // 桶检查间隔，这里设置1S
      .build()

    cityDS.addSink(sink)

    env.execute()
  }

  class EventTimeBucketAssigner extends BucketAssigner[String, String] {
    override def getBucketId(element: String, context: BucketAssigner.Context): String = {
      var partitionValue = ""
      partitionValue = getPartitionValue(element)
      "dt=" + partitionValue // 分区目录名称
    }

    override def getSerializer: SimpleVersionedSerializer[String] = {
      SimpleVersionedStringSerializer.INSTANCE
    }

    def getPartitionValue(element: String): String = {
      val eventTime = element.split(",")(1).toLong
      val date = new Date(eventTime)
      new SimpleDateFormat("yyyyMMdd").format(date)
    }
  }



}
