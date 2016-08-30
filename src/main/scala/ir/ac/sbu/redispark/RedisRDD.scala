package ir.ac.sbu.redispark

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

/**
  * Created by mehdi on 8/30/16.
  */
class RedisRDD(rdds: RDD[(Long, Long)], redisEndpoint: RedisEndpoint) extends RDD[(Long, Long)] (rdds)  {

    def connect() = new Jedis(redisEndpoint.host, redisEndpoint.port, 60000)

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[(Long, Long)] = {
        var jedis = connect()
        var pipline = jedis.pipelined()
        rdds.iterator(split, context).foreach{
            x => {
                if (!jedis.getClient.isConnected) {
                    jedis = connect()
                    pipline = jedis.pipelined()
                }
                pipline.incr(x._1.toString)
                pipline.incr(x._2.toString)
            }
        }
        Iterator()
    }

    override protected def getPartitions: Array[Partition] = rdds.partitions
}
