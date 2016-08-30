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
        rdds.iterator(split, context).foreach{
            x => {
                jedis.incr(x._1.toString)
                jedis.incr(x._2.toString)
            }
        }
        jedis.close()
        Iterator()
    }

    override protected def getPartitions: Array[Partition] = rdds.partitions
}
