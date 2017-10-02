//package ir.ac.sbu.redispark
//
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//
///**
//  * RedisContext extends sparkContext's functionality with redis functions
//  *
//  * @param sc a spark context
//  */
//class RedisContext(@transient val sc: SparkContext) extends Serializable {
//
//    def incr(kvs: RDD[(Long, Long)])
//            (implicit redisEndpoint: RedisEndpoint = new RedisEndpoint(sc.getConf)): RDD[(Long, Long)] = {
//        new RedisRDD(kvs, redisEndpoint)
//    }
//}
//
//trait RedisFunctions {
//    implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
//}
//
