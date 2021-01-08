package com.lsk.flink.etl.utils

import redis.clients.jedis.{HostAndPort, Jedis}

object RedisFactory {
  
  private lazy val jedisCluster: Jedis = {
    val jedis = new Jedis("localhost", 6379)
    jedis
  }
  
}
