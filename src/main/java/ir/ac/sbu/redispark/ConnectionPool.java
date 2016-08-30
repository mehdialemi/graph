package ir.ac.sbu.redispark;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by mehdi on 8/30/16.
 */
public class ConnectionPool {

    private static JedisPool instance;

    private static synchronized JedisPool getInstance(RedisEndpoint re) {
        if (instance == null) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(1000);
            poolConfig.setMaxIdle(1);
            poolConfig.setTestOnBorrow(true);
            poolConfig.setTestOnReturn(true);
            poolConfig.setTestWhileIdle(true);
            poolConfig.setMinEvictableIdleTimeMillis(60000);
            poolConfig.setTimeBetweenEvictionRunsMillis(3000);
            poolConfig.setNumTestsPerEvictionRun(3);
            instance = new JedisPool(poolConfig, re.host(), re.port(), 60000);
        }
        return instance;
    }

    public static Jedis connect(RedisEndpoint re) {
        return getInstance(re).getResource();
    }
}
