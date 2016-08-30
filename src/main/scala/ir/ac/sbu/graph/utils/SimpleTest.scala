package ir.ac.sbu.graph.utils

import ir.ac.sbu.redispark.RedisEndpoint

/**
  * Created by mehdi on 8/30/16.
  */
class SimpleTest {

    def main(args: Array[String]) {
        val endPoint = new RedisEndpoint("127.0.0.1", 6379)
        endPoint.connect().incr("1111")
    }
}
