package graph.ktruss

import java.io.File

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Find ktruss subgraph using pregel like procedure.
  * This implemented pregel like system
  */
object KTrussPregel {

    case class OneNeighborMsg(vId: Long, neighbors: Array[Long])

    case class NeighborMessage(list: ListBuffer[OneNeighborMsg])


    def main(args: Array[String]): Unit = {
        val inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt"
        val outputPath = "/home/mehdi/graph-data/output-pregel"
        val config = new SparkConf()
        config.setAppName("ktruss-pregel")
        config.setMaster("local[2]")
        val sc = SparkContext.getOrCreate(config)
        val k = 5
        val support = k - 2

        // Load int graph which is as a list of edges
        val inputGraph = GraphLoader.edgeListFile(sc, inputPath)

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

        // In a loop we find triangles and then remove edges lower than specified support
        var stop = false
        while (!stop) {
            graph.persist()
            // =======================================================
            // phase 1: Send message about completing the third edges.
            // =======================================================

            // Find outlink neighbors ids
            val neighborIds = graph.collectNeighborIds(EdgeDirection.Out)

            // Update each nodes with its outlink neighbors' id.
            val graphWithOutlinks = graph.outerJoinVertices(neighborIds)((vid, _, nId) => nId.getOrElse(Array[Long]()))

            // Send neighborIds of a node to all other its neighbors.
            // Send neighborIds of a node to all other its neighbors.
            val message = graphWithOutlinks.aggregateMessages(
                (ctx: EdgeContext[Array[Long], Boolean, List[(Long, Array[Long])]]) => {
                    val msg = List((ctx.srcId, ctx.srcAttr))
                    ctx.sendToDst(msg)
                }, (msg1: List[(Long, Array[Long])], msg2: List[(Long, Array[Long])]) => msg1 ::: msg2)

            // =======================================================
            // phase 2: Find triangles
            // =======================================================
            // At first each node receives messages from its neighbor telling their neighbors' id.
            // Then check that if receiving neighborIds have a common with its neighbors.
            // If there was any common neighbors then it report back telling the sender the completing nodes to make
            // a triangle through it.
            val triangleMsg = graphWithOutlinks.vertices.join(message).flatMap{ case (vid, (n, msg)) =>
                msg.map(ids => (ids._1, n.intersect(ids._2).union(Array[Long](vid)))).filter(_._2.length > 1)
            }.groupByKey()

            // In this step tgraph have information about the common neighbors per neighbor as the follow:
            // (neighborId, array of common neighbors with neighborId)
            val tgraph = graphWithOutlinks.outerJoinVertices(triangleMsg)((vid, n, msg) => msg)

            // =======================================================
            // phase 3: Collate messages for each edge
            // =======================================================

            // here there is a graph which nodes on it contains valid edges
            val validEdgeGraph = tgraph.mapVertices((vid, nMsgs) => nMsgs.getOrElse(Iterable[Array[Long]]()).flatten
              .map((vid, _)).groupBy(identity).mapValues(_.size).filter(_._2 >= support)
            )

            // activeEdgeMsg determine nodes and edges to be held to the next step.
            val activeEdgeMsg = validEdgeGraph.aggregateMessages(
                (ctx: EdgeContext[Map[(Long, Long), Int], Boolean,Boolean]) => {
                if (ctx.srcAttr.contains((ctx.srcId, ctx.dstId)))
                    ctx.sendToDst(true)
                ctx.sendToSrc(true)
            }, (a: Boolean, b: Boolean) => true)

            // this is a graph which boolean values in the nodes and edges indicate valid and invalid ones.
            val egraph = tgraph.outerJoinVertices(activeEdgeMsg)((vid, value, v) => v.getOrElse(false))

            val oldEdgeCount = graph.edges.count()
            graph = egraph.subgraph(e => e.srcAttr && e.dstAttr, (vid, v) => v)
            val newEdgeCount = graph.edges.count()

            if (newEdgeCount == 0 || newEdgeCount == oldEdgeCount)
                stop = true
        }

        new File(outputPath).delete()
        println("Remaining graph edge count: " + graph.edges.count())
        graph.edges.saveAsTextFile(outputPath)
    }
}
