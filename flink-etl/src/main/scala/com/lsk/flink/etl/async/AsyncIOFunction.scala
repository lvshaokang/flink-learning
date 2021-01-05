package com.lsk.flink.etl.async

import io.vertx.core.{Vertx, VertxOptions}
import io.vertx.redis.client.RedisOptions
import io.vertx.redis.client.impl.RedisClient
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

class AsyncIOFunction extends RichAsyncFunction[String, String] {
  
  @transient private var redisClient: RedisClient = _
  
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  
    val redisOptions = new RedisOptions()
    redisOptions.setConnectionString("redis://localhost:6379")
    
    val vo = new VertxOptions()
    vo.setEventLoopPoolSize(10)
    vo.setWorkerPoolSize(20)
  
    val vertx = Vertx.vertx(vo)
  
    redisClient = new RedisClient(vertx, redisOptions)
  }
  
  override def close(): Unit = {
    super.close()
    if (redisClient != null) {
      redisClient.close()
    }
  }
  
  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    val value = redisClient.connect()
  
  }
  
  
}
