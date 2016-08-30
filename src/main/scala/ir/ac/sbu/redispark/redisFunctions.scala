package ir.ac.sbu.redispark

import ir.ac.sbu.redispark.rdd.RedisKeysRDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * RedisContext extends sparkContext's functionality with redis functions
  *
  * @param sc a spark context
  */
class RedisContext(@transient val sc: SparkContext) extends Serializable {

    def incr(kvs: RDD[(Long, Long)])
            (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))): Unit = {
        kvs.foreachPartition((partition: Iterator[(Long, Long)]) => partition.map(kv => (redisConfig.getHost(kv._1.toString),
          kv)).toArray.groupBy(_._1).
          mapValues(a => a.map(p => p._2)).foreach {
            x => {
                val conn = x._1.endpoint.connect()
                val pipeline = conn.pipelined
                x._2.foreach(input => {
                    conn.incr(input._1.toString)
                    conn.incr(input._2.toString)
                })
                pipeline.sync
                conn.close
            }
        })
    }
    /**
      * @param keyPattern   a key pattern to match, or a single key
      * @param partitionNum number of partitions
      * @return RedisKeysRDD of simple Keys stored in redis server
      */
    def fromRedisKeyPattern(keyPattern: String = "*",
                            partitionNum: Int = 3)
                           (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RedisKeysRDD = {
        new RedisKeysRDD(sc, redisConfig, keyPattern, partitionNum, null)
    }

    /**
      * @param keys         an array of keys
      * @param partitionNum number of partitions
      * @return RedisKeysRDD of simple Keys stored in redis server
      */
    def fromRedisKeys(keys: Array[String],
                      partitionNum: Int = 3)
                     (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RedisKeysRDD = {
        new RedisKeysRDD(sc, redisConfig, "", partitionNum, keys)
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param partitionNum     number of partitions
      * @return RedisKVRDD of simple Key-Values stored in redis server
      */
    def fromRedisKV[T](keysOrKeyPattern: T,
                       partitionNum: Int = 3)
                      (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[(String, String)] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getKV
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getKV
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param partitionNum     number of partitions
      * @return RedisListRDD of related values stored in redis server
      */
    def fromRedisList[T](keysOrKeyPattern: T,
                         partitionNum: Int = 3)
                        (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[String] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getList
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getList
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param partitionNum     number of partitions
      * @return RedisZSetRDD of Keys in related ZSets stored in redis server
      */
    def fromRedisSet[T](keysOrKeyPattern: T,
                        partitionNum: Int = 3)
                       (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[String] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getSet
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getSet
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param partitionNum     number of partitions
      * @return RedisHashRDD of related Key-Values stored in redis server
      */
    def fromRedisHash[T](keysOrKeyPattern: T,
                         partitionNum: Int = 3)
                        (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[(String, String)] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getHash
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getHash
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param partitionNum     number of partitions
      * @return RedisZSetRDD of Keys in related ZSets stored in redis server
      */
    def fromRedisZSet[T](keysOrKeyPattern: T,
                         partitionNum: Int = 3)
                        (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[String] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSet
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSet
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param partitionNum     number of partitions
      * @return RedisZSetRDD of related Key-Scores stored in redis server
      */
    def fromRedisZSetWithScore[T](keysOrKeyPattern: T,
                                  partitionNum: Int = 3)
                                 (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[(String, Double)] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetWithScore
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetWithScore
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param start            start position of target zsets
      * @param end              end position of target zsets
      * @param partitionNum     number of partitions
      * @return RedisZSetRDD of Keys in related ZSets stored in redis server
      */
    def fromRedisZRange[T](keysOrKeyPattern: T,
                           start: Int,
                           end: Int,
                           partitionNum: Int = 3)
                          (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[String] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetByRange(start, end)
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetByRange(start, end)
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param start            start position of target zsets
      * @param end              end position of target zsets
      * @param partitionNum     number of partitions
      * @return RedisZSetRDD of related Key-Scores stored in redis server
      */
    def fromRedisZRangeWithScore[T](keysOrKeyPattern: T,
                                    start: Int,
                                    end: Int,
                                    partitionNum: Int = 3)
                                   (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[(String, Double)] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetByRangeWithScore(start, end)
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetByRangeWithScore(start, end)
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param min              min score of target zsets
      * @param max              max score of target zsets
      * @param partitionNum     number of partitions
      * @return RedisZSetRDD of Keys in related ZSets stored in redis server
      */
    def fromRedisZRangeByScore[T](keysOrKeyPattern: T,
                                  min: Double,
                                  max: Double,
                                  partitionNum: Int = 3)
                                 (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[String] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetByScore(min, max)
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetByScore(min, max)
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param keysOrKeyPattern an array of keys or a key pattern
      * @param min              min score of target zsets
      * @param max              max score of target zsets
      * @param partitionNum     number of partitions
      * @return RedisZSetRDD of related Key-Scores stored in redis server
      */
    def fromRedisZRangeByScoreWithScore[T](keysOrKeyPattern: T,
                                           min: Double,
                                           max: Double,
                                           partitionNum: Int = 3)
                                          (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
    RDD[(String, Double)] = {
        keysOrKeyPattern match {
            case keyPattern: String => fromRedisKeyPattern(keyPattern, partitionNum)(redisConfig).getZSetByScoreWithScore(min, max)
            case keys: Array[String] => fromRedisKeys(keys, partitionNum)(redisConfig).getZSetByScoreWithScore(min, max)
            case _ => throw new scala.Exception("KeysOrKeyPattern should be String or Array[String]")
        }
    }

    /**
      * @param kvs Pair RDD of K/V
      * @param ttl time to live
      */
    def toRedisKV(kvs: RDD[(String, String)], ttl: Int = 0)
                 (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
        kvs.foreachPartition(partition => partition.map(kv => (redisConfig.getHost(kv._1), kv)).toArray.groupBy(_._1).
          mapValues(a => a.map(p => p._2)).foreach {
            x => {
                val conn = x._1.endpoint.connect()
                val pipeline = conn.pipelined
                if (ttl <= 0) {
                    x._2.foreach(x => pipeline.set(x._1, x._2))
                }
                else {
                    x._2.foreach(x => pipeline.setex(x._1, ttl, x._2))
                }
                pipeline.sync
                conn.close
            }
        })
    }

    /**
      * @param kvs      Pair RDD of K/V
      * @param hashName target hash's name which hold all the kvs
      * @param ttl      time to live
      */
    def toRedisHASH(kvs: RDD[(String, String)], hashName: String, ttl: Int = 0)
                   (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
        kvs.foreachPartition(partition => {
            val conn = redisConfig.connectionForKey(hashName)
            val pipeline = conn.pipelined
            val p = partition.foreach(x => pipeline.hset(hashName, x._1, x._2))
            if (ttl > 0) pipeline.expire(hashName, ttl)
            pipeline.sync
            conn.close
            p
        })
    }

    /**
      * @param kvs      Pair RDD of K/V
      * @param zsetName target zset's name which hold all the kvs
      * @param ttl      time to live
      */
    def toRedisZSET(kvs: RDD[(String, String)], zsetName: String, ttl: Int = 0)
                   (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
        kvs.foreachPartition(partition => {
            val conn = redisConfig.connectionForKey(zsetName)
            val pipeline = conn.pipelined
            val p = partition.foreach(x => pipeline.zadd(zsetName, x._2.toDouble, x._1))
            if (ttl > 0) pipeline.expire(zsetName, ttl)
            pipeline.sync
            conn.close
            p
        })
    }

    /**
      * @param vs      RDD of values
      * @param setName target set's name which hold all the vs
      * @param ttl     time to live
      */
    def toRedisSET(vs: RDD[String], setName: String, ttl: Int = 0)
                  (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
        vs.foreachPartition(partition => {
            val conn = redisConfig.connectionForKey(setName)
            val pipeline = conn.pipelined
            val p = partition.foreach(pipeline.sadd(setName, _))
            if (ttl > 0) pipeline.expire(setName, ttl)
            pipeline.sync
            conn.close
            p
        })
    }

    /**
      * @param vs       RDD of values
      * @param listName target list's name which hold all the vs
      * @param ttl      time to live
      */
    def toRedisLIST(vs: RDD[String], listName: String, ttl: Int = 0)
                   (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
        vs.foreachPartition(partition => {
            val conn = redisConfig.connectionForKey(listName)
            val pipeline = conn.pipelined
            val p = partition.foreach(pipeline.rpush(listName, _))
            if (ttl > 0) pipeline.expire(listName, ttl)
            pipeline.sync
            conn.close
            p
        })
    }
}

trait RedisFunctions {
    implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}

