package com.lsk.flink.etl.utils

import redis.clients.jedis.{HostAndPort, Jedis}

object RedisFactory {
  
  private lazy val jedisCluster: Jedis = {
    new Jedis(new HostAndPort("localhost", 6379))
  }
  
}
