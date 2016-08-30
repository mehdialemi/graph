package ir.ac.sbu.redispark;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.io.Serializable;
/**
 *
 */
public class ConnectionPool implements Serializable {

    private JedisPool jedisPool;

    public ConnectionPool(String host, int port) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(1000);
            poolConfig.setMaxIdle(1);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setMinEvictableIdleTimeMillis(60000);
            poolConfig.setTimeBetweenEvictionRunsMillis(3000);
            poolConfig.setNumTestsPerEvictionRun(3);
            jedisPool = new JedisPool(poolConfig, host, port, 60000);
    }

    public Jedis getJedis() {
        return jedisPool.getResource();
    }

    public void close() {
        jedisPool.close();
    }
}
