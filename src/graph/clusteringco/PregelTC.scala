package graph.clusteringco

import java.io.File

import graph.{GraphUtils, OutUtils}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Find ktruss subgraph using pregel like procedure.
  * This implemented pregel like system
  */
object PregelTC {

    case class OneNeighborMsg(vId: Long, neighbors: Array[Long])

    case class NeighborMessage(list: ListBuffer[OneNeighborMsg])


    def main(args: Array[String]): Unit = {
        var inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt"
        if (args != null && args.length > 0)
            inputPath = args(0);

        var partition = 2
        if (args != null && args.length > 1)
            partition = Integer.parseInt(args(1));
        val conf = new SparkConf()
        if (args == null || args.length == 0)
            conf.setMaster("local[2]")
        GraphUtils.setAppName(conf, "GraphX-GCC", partition, inputPath);

        val sc = new SparkContext(conf)

        // Load int graph which is as a list of edges
        val inputGraph = GraphLoader.edgeListFile(sc, inputPath, numEdgePartitions = partition)

        // Change direction from lower degree node to a higher node
        // First find degree of each node
        // Second find correct edge direction
        // Third create a new graph with new edges and previous vertices

        // Set degree of each vertex in the property.
        val graphVD = inputGraph.outerJoinVertices(inputGraph.degrees)((vid, v, deg) => deg)

        // Find new edges with correct direction. A direction from a lower degree node to a higher degree node.
        val newEdges = graphVD.triplets.map { et =>
            if (et.srcAttr.get <= et.dstAttr.get)
                Edge(et.srcId, et.dstId, true)
            else
                Edge(et.dstId, et.srcId, true)
        }

        val empty = sc.makeRDD(Array[(Long, Boolean)]())

        // Create graph with edge direction from lower degree to higher degree node and edge attribute.
        var graph = Graph(empty, newEdges)

        // =======================================================
        // phase 1: Send message about completing the third edges.
        // =======================================================

        // Find outlink neighbors ids
        val neighborIds = graph.collectNeighborIds(EdgeDirection.Out)

        // Update each nodes with its outlink neighbors' id.
        val graphWithOutlinks = graph.outerJoinVertices(neighborIds)((vid, _, nId) => nId.getOrElse(Array[Long]()))

        // Send neighborIds of a node to all other its neighbors.
        val message = graphWithOutlinks.aggregateMessages(
            (ctx: EdgeContext[Array[Long], Boolean, NeighborMessage]) => {
                val msg = new ListBuffer[OneNeighborMsg]()
                msg += OneNeighborMsg(ctx.srcId, ctx.srcAttr)
                ctx.sendToDst(NeighborMessage(msg))
            }, (msg1: NeighborMessage, msg2: NeighborMessage) => {
                msg1.list ++= msg2.list
                msg1
            })

        // =======================================================
        // phase 2: Find triangles
        // =======================================================
        // At first each node receive messages from its neighbor telling their neighbors' id.
        // Then check that if receiving neighborIds have a common with its neighbors.
        // If there was any common neighbors then it report back telling the sender the completing nodes to make
        // a triangle through it.
        val triangleMsg = graphWithOutlinks.vertices.join(message).flatMap { case (vid, (n, msg)) =>
            val map1 = msg.list.map(ids => (ids.vId, n.intersect(ids.neighbors))).filter(_._2.length > 0)
            val map2 = map1.map(vid_ids => (vid_ids._1, Array[Long](vid)))
            map1.union(map2)
        }.groupByKey()

        // In this step tgraph has information about the common neighbors per neighbor as the follow:
        // (neighborId, array of common neighbors with neighborId)
        val tgraph = graphWithOutlinks.outerJoinVertices(triangleMsg)((vid, n, msg) => msg)

        val triangles = tgraph.vertices.map(n => n._2.getOrElse(Iterator()).size).reduce((a, b) => a + b)
        OutUtils.printOutputTC(triangles)

        sc.stop()
    }
}
