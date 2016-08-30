package ir.ac.sbu.redispark.partitioner

import ir.ac.sbu.redispark.RedisConfig
import org.apache.spark.Partition


case class RedisPartition(index: Int,
                          redisConfig: RedisConfig,
                          slots: (Int, Int)) extends Partition
