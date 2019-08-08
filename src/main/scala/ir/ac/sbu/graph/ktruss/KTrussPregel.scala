package ir.ac.sbu.graph.ktruss

import java.io.File

import ir.ac.sbu.graph.utils.GraphUtils
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ListBuffer, Map}

/**
  * Find ktruss subgraph using pregel like procedure.
  * This implemented pregel like system
  */
object KTrussPregel {

    case class OneNeighborMsg(vId: Long, fonlValue: Array[Long])

    case class NeighborMessage(list: ListBuffer[OneNeighborMsg])


    val P_MULTIPLIER = 5

    def main(args: Array[String]): Unit = {
        var inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt"
        if (args != null && args.length > 0)
            inputPath = args(0);

        var k = 4
        if (args.length > 2)
            k = args(1).toInt
        val support: Int = k - 2

        val conf = new SparkConf()
        if (args == null || args.length == 0)
            conf.setMaster("local[2]")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        conf.setAppName("KTruss-Pregel-" + k + "-(" + new File(inputPath).getName + ")")

        val sc = SparkContext.getOrCreate(conf)

        val start = System.currentTimeMillis()
        // Load int ir.ac.sbu.graph which is as a list of triangleEdges
        val inputGraph = GraphLoader.edgeListFile(sc, inputPath)

        val partition = inputGraph.edges.getNumPartitions * P_MULTIPLIER

        // Change direction from lower degree node to a higher node
        // First find degree of each node
        // Second find correct edge direction
        // Third getOrCreate a new ir.ac.sbu.graph with new triangleEdges and previous vertices

        // Set degree of each vertex in the property.
        val graphVD = inputGraph.outerJoinVertices(inputGraph.degrees)((vid, vertex, degree) => degree)

        // Find new triangleEdges with correct direction. A direction from a lower degree node to a higher degree node.
        val newEdges = graphVD.triplets.map { et =>
            if (et.srcAttr.get <= et.dstAttr.get)
                Edge(et.srcId, et.dstId, 0)
            else
                Edge(et.dstId, et.srcId, 0)
        }.repartition(partition)

        val empty = sc.makeRDD(Array[(Long, Int)]())

        // Create ir.ac.sbu.graph with edge direction from lower degree to higher degree node and edge attribute.
        var graph = Graph(empty, newEdges, defaultVertexAttr = 0,
            edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

        // In a loop we find triangles and then remove triangleEdges lower than specified sup
        var stop = false
        var iteration = 0
        while (!stop) {
            iteration = iteration + 1
            val t1 = System.currentTimeMillis()
            println("iteration: " + iteration)

//            graph.partitionBy(PartitionStrategy.CanonicalRandomVertexCut).persist(StorageLevel.MEMORY_AND_DISK)

            val oldEdgeCount = graph.triangleEdges.count()
            // =======================================================
            // phase 1: Send message about completing the third triangleEdges.
            // =======================================================

            // Find outlink fonlValue ids
            val neighborIds = graph.collectNeighborIds(EdgeDirection.Either)

            // Update each nodes with its outlink fonlValue' id.
            val graphWithOutlinks = graph.outerJoinVertices(neighborIds)((vid, _, nId) => nId.getOrElse(Array[Long]()))

            // Send neighborIds of a node to all other its fonlValue.
            // Send neighborIds of a node to all other its fonlValue.
            val message = graphWithOutlinks.aggregateMessages(
                (ctx: EdgeContext[Array[Long], Int, List[(Long, Array[Long])]]) => {
                    val msg = List((ctx.srcId, ctx.srcAttr))
                    ctx.sendToDst(msg)
                }, (msg1: List[(Long, Array[Long])], msg2: List[(Long, Array[Long])]) => msg1 ::: msg2)

            // =======================================================
            // phase 2: Find triangles
            // =======================================================
            // At first each node receives messages from its neighbor telling their fonlValue' id.
            // Then check that if receiving neighborIds have a common with its fonlValue.
            // If there was any common fonlValue then it report back telling the sender the completing nodes to make
            // a triangle through it.
            val triangleMsg = graphWithOutlinks.vertices.join(message).flatMap { case (vid, (n, msg)) =>
                msg.map(ids => (ids._1, vid -> n.intersect(ids._2).length)).filter(t => t._2._2 > 0)
            }.groupByKey()

            // In this step tgraph have information about the common fonlValue per neighbor as the follow:
            // (neighborId, array of common fonlValue with neighborId)
            val edgeCount = graphWithOutlinks.outerJoinVertices(triangleMsg)((vid, n, msg) => {
                val m = Map[Long, Int]()
                msg.getOrElse(Map[Long, Int]()).map(t => m.put(t._1, t._2 + m.getOrElse(t._1, 0)))
                m
            })

            val edgeUpdated = edgeCount.mapTriplets(t => t.srcAttr.getOrElse(t.dstId, 0)).subgraph(e => e.attr >=
              support, (vid, vertex) => true)

            val newGraph = edgeUpdated.mapVertices((vId, vertex) => 0)
              .partitionBy(PartitionStrategy.CanonicalRandomVertexCut)
              .persist(StorageLevel.MEMORY_AND_DISK)

            // =======================================================
            // phase 3: Collate messages for each edge
            // =======================================================
            val newEdgeCount = newGraph.triangleEdges.count()

            println("KTRUSS) iteration: " + iteration + ", edge count: " + newEdgeCount + ", duration: " +
              (System.currentTimeMillis() - t1) + " ms")

            if (newEdgeCount == 0 || newEdgeCount == oldEdgeCount)
                stop = true

            graph.unpersist()
            graph = newGraph;
        }

        println("KTRUSS final edge count: " + graph.triangleEdges.count() + ", duration: " + (System.currentTimeMillis
        () - start) / 1000 + "s")

        sc.stop()
    }
}
