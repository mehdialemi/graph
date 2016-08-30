package ir.ac.sbu.redispark

import org.apache.spark.Partition

/**
  * Created by mehdi on 8/30/16.
  */
case class RedisPartition (index: Int, redisEndpoint: RedisEndpoint) extends Partition
