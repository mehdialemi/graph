package ir.ac.sbu.redispark

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * Created by mehdi on 8/30/16.
  */
class RedisRDD(rdds: RDD[(Long, Long)], redisEndpoint: RedisEndpoint) extends RDD[(Long, Long)] (rdds)  {

    object xxx {
        val jedis = new Jedis(redisEndpoint.host, redisEndpoint.port)
    }
    
    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[(Long, Long)] = {
        rdds.iterator(split, context).foreach{
            x => {
                xxx.jedis.incr(x._1.toString)

                xxx.jedis.incr(x._2.toString)
            }
        }
        Iterator()
    }

    override protected def getPartitions: Array[Partition] = rdds.partitions
}
