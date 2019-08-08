package ir.ac.sbu.graph.ktruss

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
/**
  *
  */
object KTrussSpark {

    def main(args: Array[String]) {
        val inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt"
        val outputPath = "/home/mehdi/ir.ac.sbu.graph-data/output-spark"
        val partitions = 2
        val config = new SparkConf()
        config.setAppName("k-truss spark")
        config.setMaster("local[2]")
        val sc = SparkContext.getOrCreate(config)
        val k = 4
        val support = k - 2

        // Step 1: Create fonl
        // Find fonlValue, duplicate links are removed, no direction
        val neighbor = sc.textFile(inputPath, partitions)
          .filter(t => !t.startsWith("#")).map(t => t.split("\\s+"))
          .map(t => t(0).toLong -> t(1).toLong)
          .flatMap((t => Map((t._1 , t._2), (t._2 , t._1)))).groupByKey().map(t => (t._1, t._2.to[Set]))

        // A neighbor list that each neighbor has its degree
        val vdNeighbor = neighbor.flatMap { t =>
            var msg = mutable.Map[Long, (Long, Int)]()
            val degree = t._2.size
            t._2.foreach(e => msg += e -> (t._1, degree))
            msg
        }.groupByKey()

        // Neighbors which have higher degree than key and sorted based on degree
        val fsNeighbors = vdNeighbor
          .map(t => (t._1, t._2.filter(x => x._2 > t._2.size || (x._2 == t._2.size && x._1 > t._1)).map(_._1).toSet[Long])).cache

        // Use sortWith(_._2 < _._2) before map when this sortDegrees means if in the subsequent steps there is a filtering based on it
        // We can send degree along with vertex id to cut based on it

        // Step 2: find triangles
        // Send third completing edge to check
        val edgeMsg = fsNeighbors.flatMap { t =>
            val msg = mutable.Map[Long, (Long, Set[Long])]()
            // we can add filtering based on sortDegrees and not send any more data
            t._2.foreach(e => msg += e -> (t._1, t._2))
            msg
        }

        var triangles = edgeMsg.cogroup(fsNeighbors).flatMap { t =>
            t._2._1.map(e => t._2._2.head.intersect(e._2).map((e._1, t._1, _))).flatten
        }


        //  Count edges
        var finish = false
        var iter = 0
        while(!finish) {
            iter = iter + 1
            println("iteration: " + iter)
            println("triangles: " + triangles.count())

            triangles.repartition(partitions).cache
            val invalidEdges = triangles.flatMap(t =>
                List(((t._1, t._2) -> t._3), ((t._1, t._3) -> t._2), ((t._2, t._3) -> t._1)))
              .groupBy(identity)
              .filter(_._2.size < support)
            println("invalid edges: " + invalidEdges.count())

            val removeTriangles = invalidEdges.map(e => makeTriangle(e._1._1._1, e._1._1._2, e._1._2))
            println("remove triangles: " + removeTriangles.count())

            val remaining = triangles.subtract(removeTriangles)
            val remainingCount = remaining.count()
            println("remaining: " + remainingCount)

            if (remainingCount == 0) {
                triangles = remaining
                finish = true
            } else {
                triangles.unpersist(false) // not-blocking
                triangles = remaining
            }
        }

        val edges = triangles.flatMap(t => List((t._1, t._2), (t._1, t._3), (t._2, t._3))).distinct()

        println("Remaining  edge count: " + edges.count())
        new File(outputPath).delete()
        edges.saveAsTextFile(outputPath)
    }


    case class Edge(v1: Long, v2: Long, x: Long) {
        def _hashCode(x: Edge) = (v1 + v2).toInt
        def _equals(x: Edge) = (v1 == v2)
    }

    case class Triangle(v1: Long, v2: Long, v3: Long)


    def makeTriangle(v1: Long, v2: Long, v3: Long): (Long, Long, Long) = {
        val sorted = Array(v1, v2, v3).sortWith(_<_)
        (sorted(0), sorted(1), sorted(2))
    }
}
