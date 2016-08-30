package ir.ac.sbu.redispark

import java.util.concurrent.ConcurrentHashMap

import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis._

import scala.collection.JavaConversions._


object ConnectionPool {

    @transient private val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
        new ConcurrentHashMap[RedisEndpoint, JedisPool]()

    def connect(re: RedisEndpoint): Jedis = {
        val pool = pools.getOrElseUpdate(re, {
            val poolConfig: JedisPoolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(1000)
            poolConfig.setMaxIdle(1)
            poolConfig.setTestOnBorrow(true)
            poolConfig.setTestOnReturn(true)
            poolConfig.setTestWhileIdle(true)
            poolConfig.setMinEvictableIdleTimeMillis(60000)
            poolConfig.setTimeBetweenEvictionRunsMillis(3000)
            poolConfig.setNumTestsPerEvictionRun(3)
            new JedisPool(poolConfig, re.host, re.port, 60000)
        }
        )
        var sleepTime: Int = 4
        var conn: Jedis = null
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

