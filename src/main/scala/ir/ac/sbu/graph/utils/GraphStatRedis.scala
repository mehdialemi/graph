package ir.ac.sbu.graph.utils

import ir.ac.sbu.graph.GraphUtils
import ir.ac.sbu.redispark.{RedisConfig, RedisContext, RedisEndpoint}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mehdi on 8/30/16.
  */
object GraphStatRedis {

    def main(args: Array[String]) {
        var inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt"
        if (args != null && args.length > 0)
            inputPath = args(0);

        var partition = 2
        if (args != null && args.length > 1)
            partition = args(1).toInt;

        val conf = new SparkConf()
        if (args == null || args.length == 0)
            conf.setMaster("local[2]")
        GraphUtils.setAppName(conf, "Graph-Stat-By-Redis", partition, inputPath);
        conf.set("redis.host", "127.0.0.1").set("redis.port", "6379")

        val sc = SparkContext.getOrCreate(conf)
        val edges = sc.textFile(inputPath, partition)
          .filter(t => !t.startsWith("#")).map(t => t.split("\\s+"))
          .map(t => t(0).toLong -> t(1).toLong)
        val rc = new RedisContext(sc)
        val redisConfig = new RedisConfig(new RedisEndpoint("127.0.0.1", 6379))
        rc.incr(edges)(redisConfig)
        sc.stop()
    }

}
