package ir.ac.sbu.redispark

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD

/**
  * Created by mehdi on 8/30/16.
  */
class RedisRDD(rdds: RDD[(Long, Long)], redisEndpoint: RedisEndpoint) extends RDD[(Long, Long)] (rdds)  {

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[(Long, Long)] = {
        val pool = new ConnectionPool(redisEndpoint.host, redisEndpoint.port);

        rdds.iterator(split, context).foreach{
            x => {
                var jedis = pool.getJedis();
                jedis.incr(x._1.toString)
                jedis.close()

                jedis = pool.getJedis();
                jedis.incr(x._2.toString)
                jedis.close()
            }
        }
        pool.close()
        Iterator()
    }

    override protected def getPartitions: Array[Partition] = rdds.partitions
}
