package ir.ac.sbu.redispark

import java.net.URI

import org.apache.spark.SparkConf

/**
  * RedisEndpoint represents a redis connection endpoint info: host, port, auth password
  * db number, and timeout
  *
  * @param host the redis host or ip
  * @param port the redis port
  */
case class RedisEndpoint(host: String = "127.0.0.1", port: Int = 6379)
  extends Serializable {

    /**
      * Constructor from spark config. set params with redis.host, redis.port, redis.auth and redis.db
      *
      * @param conf spark context config
      */
    def this(conf: SparkConf) {
        this(
            conf.get("redis.host"),
            conf.getInt("redis.port", 6379)
        )
    }

    /**
      * Constructor with Jedis URI
      *
      * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
      */
    def this(uri: URI) {
        this(uri.getHost, uri.getPort)
    }

    /**
      * Constructor with Jedis URI from String
      *
      * @param uri connection URI in the form of redis://:$password@$host:$port/[dbnum]
      */
    def this(uri: String) {
        this(URI.create(uri))
    }

    /**
      * Connect tries to open a connection to the redis endpoint,
      * optionally authenticating and selecting a db
      *
      * @return a new Jedis instance
      */
    def connect() = {
        ConnectionPool.connect(this)
    }


}
