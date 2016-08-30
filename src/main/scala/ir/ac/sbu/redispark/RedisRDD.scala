package ir.ac.sbu.redispark

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * Created by mehdi on 8/30/16.
  */
class RedisRDD(rdds: RDD[(Long, Long)], redisEndpoint: RedisEndpoint) extends RDD[(Long, Long)] (rdds)  {


    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[(Long, Long)] = {
        val jedis = new Jedis(redisEndpoint.host, redisEndpoint.port, 60000)
        val pipline = jedis.pipelined()

        firstParent[(Long, Long)].iterator(split, context).map(x => {
            pipline.incr(x._1.toString)
            pipline.incr(x._2.toString)
        })
        
        pipline.close()
        Iterator()
    }

    override protected def getPartitions: Array[Partition] = rdds.partitions
}
