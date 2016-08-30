package ir.ac.sbu.redispark

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * RedisContext extends sparkContext's functionality with redis functions
  *
  * @param sc a spark context
  */
class RedisContext(@transient val sc: SparkContext) extends Serializable {

    def incr(kvs: RDD[(Long, Long)])
            (implicit redisEndpoint: RedisEndpoint = new RedisEndpoint(sc.getConf)): JavaRedisRDD = {
        new JavaRedisRDD(sc, redisEndpoint.host, redisEndpoint.port, kvs.dependencies)
    }
}

trait RedisFunctions {
    implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}

