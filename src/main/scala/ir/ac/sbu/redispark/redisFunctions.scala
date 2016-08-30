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
            (implicit redisEndpoint: RedisEndpoint = new RedisEndpoint(sc.getConf)): Unit = {
        val redis = redisEndpoint.connect()

        kvs.foreachPartition((partition: Iterator[(Long, Long)]) => partition.foreach {
            x => {
                redis.incr(x._1.toString)
                redis.incr(x._2.toString)
            }
        })
    }
}

trait RedisFunctions {
    implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}

