package ir.ac.sbu.redispark

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * Created by mehdi on 8/30/16.
  */
class RedisRDD(rdd: RDD[(Long, Long)], redisEndpoint: RedisEndpoint) extends RDD[(Long, Long)] (rdd)  {

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[(Long, Long)] = {

        val y = rdd.iterator(split, context).map(x => {
            val jedis = new Jedis(redisEndpoint.host, redisEndpoint.port, 60000)
            val pipline = jedis.pipelined()
            pipline.incr(x._1.toString)
            pipline.incr(x._2.toString)
            pipline.close()
            x
        })
        y
    }

    override protected def getPartitions: Array[Partition] = firstParent[(Long, Long)].partitions
}
