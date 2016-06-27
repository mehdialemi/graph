package graph.ktruss

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
/**
  *
  */
object KTrussSpark {

    def main(args: Array[String]) {
        val inputPath = "input.txt"
        val partitions = 2
        val config = new SparkConf()
        config.setAppName("k-truss spark")
        config.setMaster("local")
        val sc = SparkContext.getOrCreate(config)
        val k = 4
        val sup = sc.broadcast(k - 2)
        // Step 1: Create fonl
        // Find neighbors, duplicate links are removed, no direction
        val neighbor = sc.textFile(inputPath, partitions)
          .filter(t => !t.startsWith("#")).map(t => t.split("\\s+"))
          .map(t => t(0).toLong -> t(1).toLong)
          .flatMap((t => Map((t._1 , t._2), (t._2 , t._1)))).groupByKey().map(t => (t._1, t._2.to[Set]))

        // A neighbor list that each neighbor has its degree
        val vdNeighbor = neighbor.flatMap { t =>
            var msg = mutable.Map[Long, (Long, Int)]()
            val deg = t._2.size
            t._2.foreach(e => msg += e -> (t._1, deg))
            msg
        }.groupByKey()

        // Neighbors which have higher degree than key and sorted based on degree
        val fsNeighbors = vdNeighbor
          .map(t => (t._1, t._2.filter(x => x._2 > t._2.size || (x._2 == t._2.size && x._1 > t._1)).map(_._1).toSet[Long])).cache

        // Use sortWith(_._2 < _._2) before map when this sort means if in the subsequent steps there is a filtering based on it
        // We can send degree along with vertex id to cut based on it
        println("Filtered neighbors")
        fsNeighbors.collect().foreach(println(_))

        // Step 2: find triangles
        // Send third completing edge to check
        val edgeMsg = fsNeighbors.flatMap { t =>
            val msg = mutable.Map[Long, (Long, Set[Long])]()
            // we can add filtering based on sort and not send any more data
            t._2.foreach(e => msg += e -> (t._1, t._2))
            msg
        }

        println("Edge messeges:")
        edgeMsg.collect().foreach(println(_))

        var triangles = edgeMsg.cogroup(fsNeighbors).flatMap { t =>
            val triangles = mutable.ListBuffer[Triangle]()
            val v2 = t._1
            if (t._2._2.size > 0) {
                val neighbors = t._2._2.head
                val msgs = t._2._1
                msgs.foreach { e =>
                    neighbors.intersect(e._2).foreach(v3 => triangles += makeTriangle(e._1, v2, v3))
                }
            }
            triangles
        }
        println("Triangles:")
        triangles.collect().foreach(println(_))

        //  Count edges
        var finish = false
        while(!finish) {
            triangles.cache
            val removeTriangles = triangles
              .flatMap(t => Map(Edge(t.v1, t.v2, t.v3) -> 1, Edge(t.v1, t.v3, t.v2) -> 1, Edge(t.v2, t.v3, t.v1) -> 1))
              .reduceByKey((a, b) => a + b).filter(t => t._2 < sup.value)
              .map(e => makeTriangle(e._1.v1, e._1.v2, e._1.x))

            val remaining = triangles.subtract(removeTriangles)
            if (remaining.count == 0)
                finish = true
            else {
                triangles.unpersist(false) // not-blocking
                triangles = remaining
            }
        }

        println("Final triangles:")
        triangles.collect().foreach(println(_))
        val edges = triangles.flatMap(t => List((t.v1, t.v2), (t.v1, t.v3), (t.v2, t.v3))).distinct()

        println("Edges:")
        edges.collect().foreach(println(_))
    }


    case class Edge(v1: Long, v2: Long, x: Long) {
        def _hashCode(x: Edge) = (v1 + v2).toInt
        def _equals(x: Edge) = (v1 == v2)
    }

    case class Triangle(v1: Long, v2: Long, v3: Long)


    def makeTriangle(v1: Long, v2: Long, v3: Long): Triangle = {
        val sorted = Array(v1, v2, v3).sortWith(_<_)
        Triangle(sorted(0), sorted(1), sorted(2))
    }
}
