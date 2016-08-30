package ir.ac.sbu.graph;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;

/**
 *
 */
public class RedisTest {

    public static void main(String[] args) {
        RedisURI redisUrl = RedisURI.create("redis://localhost:6379/0");
        RedisClient redisClient = RedisClient.create(redisUrl);
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        connection.async().incr("10:10");

        connection.close();
        redisClient.shutdown();
    }
}
