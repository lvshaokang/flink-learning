package com.lsk.bigdata.flink.join.util

import org.apache.kafka.clients.consumer.ConsumerConfig

import java.util.Properties

object KafkaConfigUtil {
  
  def buildConsumerProperties(groupId: String): Properties = {
    val kafkaIps = "192.168.0.1"
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIps)
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    prop
  }
}
