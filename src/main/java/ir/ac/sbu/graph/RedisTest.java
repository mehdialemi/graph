package ir.ac.sbu.graph;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.api.StatefulRedisConnection;

/**
 *
 */
public class RedisTest {

    public static void main(String[] args) {
        RedisClient redisClient = RedisClient.create(args[0]);
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        connection.async().incr("10:10");

        connection.close();
        redisClient.shutdown();
    }
}
