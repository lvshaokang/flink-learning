package com.ruozedata.bigdata.base

import org.apache.flink.api.common.functions.Partitioner

/**
 * author：若泽数据-PK哥   
 * 交流群：545916944
 */
class RuozedataPartitioner extends Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
    println("numPartitions......" + numPartitions)
    if(key == "ruozedata.com") {
      0
    } else if(key == "ruoze.ke.qq.com") {
      1
    } else {
      2
    }
  }
}
