package graph.ktruss

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mehdi on 6/10/16.
  */
object TestScala {

    def main(args: Array[String]) {
        val inputPath = "input.txt"
        val outputPath = "output"
        val config = new SparkConf()
        config.setAppName("ktruss-pregel")
        config.setMaster("local[2]")
        val sc = SparkContext.getOrCreate(config)
        val lines = sc.textFile(inputPath)
        val edges = lines.map(line => line.split("\\s+"))
          .filter(s => s.length == 2)
          .map(s => (s(0).toInt, s(1).toInt))
        println("Edges")
        edges.foreach(println(_))

        println()
        println("Group")
        val group = edges.groupByKey()
        group.foreach(println(_))

        println("Reduced")
        val reduced = edges.reduceByKey((a, b) => a + b)
        reduced.foreach(println(_))

        println("Joined")
        val joined = group.join(reduced).mapValues(a => a._1.map(e => e * a._2))
        joined.foreach(println(_))
    }
}
