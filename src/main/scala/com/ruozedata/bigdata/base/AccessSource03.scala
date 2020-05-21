package com.ruozedata.bigdata.base

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, RichParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
 * author：若泽数据-PK哥   
 * 交流群：545916944
 */
class AccessSource03 extends RichParallelSourceFunction[Access]{
  var running = true

  val random = new Random()

  var domains = Array("ruozedata.com","ruoze.ke.qq.com","google.com")

  // 初始化操作
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    println("----open.invoked---- ")
  }

  // 释放资源
  override def close(): Unit = super.close()

  override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {

    while(running) {
      val time = System.currentTimeMillis()
      1.to(10).map(x => {
        ctx.collect(Access(time,
          domains(random.nextInt(domains.length)),
          random.nextInt(1000)+x
        ))
      })

      Thread.sleep(5000)
    }

  }

  override def cancel(): Unit = {
    running = false
  }
}
