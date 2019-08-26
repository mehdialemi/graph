//package ir.ac.sbu.redispark
//
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//
///**
//  * RedisContext extends sparkContext's functionality with redis functions
//  *
//  * @param javaSparkContext a spark context
//  */
//class RedisContext(@transient val javaSparkContext: SparkContext) extends Serializable {
//
//    def incr(kvs: RDD[(Long, Long)])
//            (implicit redisEndpoint: RedisEndpoint = new RedisEndpoint(javaSparkContext.getConf)): RDD[(Long, Long)] = {
//        new RedisRDD(kvs, redisEndpoint)
//    }
//}
//
//trait RedisFunctions {
//    implicit def toRedisContext(javaSparkContext: SparkContext): RedisContext = new RedisContext(javaSparkContext)
//}
//
