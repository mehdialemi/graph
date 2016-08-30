package ir.ac.sbu.redispark

import java.util.concurrent.ConcurrentHashMap

import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis._

import scala.collection.JavaConversions._


object ConnectionPool {

    @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, ShardedJedisPool] =
        new ConcurrentHashMap[RedisEndpoint, ShardedJedisPool]()

    def connect(re: RedisEndpoint): ShardedJedis = {
        val pool = pools.getOrElseUpdate(re, {
            val shardInfo = List(new JedisShardInfo(re.host, re.port))

            val poolConfig: JedisPoolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(10000)
            poolConfig.setMaxIdle(10)
            poolConfig.setTestOnBorrow(true)
            poolConfig.setTestOnReturn(true)
            poolConfig.setTestWhileIdle(true)
            poolConfig.setMinEvictableIdleTimeMillis(60000)
            poolConfig.setTimeBetweenEvictionRunsMillis(30000)
            poolConfig.setNumTestsPerEvictionRun(3)
            new ShardedJedisPool(poolConfig, shardInfo)
        }
        )
        var sleepTime: Int = 4
        var conn: ShardedJedis = null
        while (conn == null) {
            try {
                conn = pool.getResource
            }
            catch {
                case e: JedisConnectionException if e.getCause.toString.
                  contains("ERR max number of clients reached") => {
                    if (sleepTime < 500) sleepTime *= 2
                    Thread.sleep(sleepTime)
                }
                case e: Exception => throw e
            }
        }
        conn
    }
}

