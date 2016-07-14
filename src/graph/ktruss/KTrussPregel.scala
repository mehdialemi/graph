package graph.ktruss

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Find ktruss subgraph using pregel like procedure.
  * This implemented pregel like system
  */
object KTrussPregel {

    case class OneNeighborMsg(vId: Long, neighbors: Array[Long])

    case class NeighborMessage(list: ListBuffer[OneNeighborMsg])


    def main(args: Array[String]): Unit = {
        val input = "input.txt"
        val outputPath = "output.txt"
        val config = new SparkConf()
        config.setAppName("ktruss-pregel")
        config.setMaster("local")
        val sc = SparkContext.getOrCreate(config)
        val support = 1

        // Load int graph which is as a list of edges
        val inputGraph = GraphLoader.edgeListFile(sc, input)

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
        var i = 0
        var stop = false
        while (i < 100 && !stop) {
            graph.persist()
            // =======================================================
            // phase 1: Send message about completing the third edges.
            // =======================================================

            // Find outlink neighbors ids
            val neighborIds = graph.collectNeighborIds(EdgeDirection.Out)

            // Update each nodes with its outlink neighbors' id.
            val graphWithOutlinks = graph.outerJoinVertices(neighborIds)((vid, _, nId) => nId.getOrElse(Array[Long]()))

            // Send neighborIds of a node to all other its neighbors.
            val message = graphWithOutlinks.aggregateMessages(sendNeighborIds, mergeNeighborIds)

            // =======================================================
            // phase 2: Find triangles
            // =======================================================
            // At first each node receive messages from its neighbor telling their neighbors' id.
            // Then check that if receiving neighborIds have a common with its neighbors.
            // If there was any common neighbors then it report back telling the sender the completing nodes to make
            // a triangle through it.
            val triangleMsg = graphWithOutlinks.vertices.join(message).flatMap{ case (vid, (n, msg)) =>
                val map1 = msg.list.map(ids => (ids.vId, n.intersect(ids.neighbors))).filter(_._2.length > 0)
                val map2 = map1.map(vid_ids => (vid_ids._1, Array[Long](vid)))
                map1.union(map2)
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
            i += 1
        }

        graph.edges.saveAsTextFile(outputPath)
    }

    // Send neighborIds to all neighbors
    def sendNeighborIds(ctx: EdgeContext[Array[Long], Boolean, NeighborMessage]): Unit = {
        val msg = new ListBuffer[OneNeighborMsg]()
        msg += OneNeighborMsg(ctx.srcId, ctx.srcAttr)
        ctx.sendToDst(NeighborMessage(msg))
    }

    def mergeNeighborIds(msg1: NeighborMessage, msg2: NeighborMessage): NeighborMessage = {
        msg1.list ++= msg2.list
        msg1
    }

    // Send completing node to the neighbor
    def sendTriangle(ctx: EdgeContext[mutable.Map[Long, Array[Long]], Int, ListBuffer[Long]]): Unit = {
        ctx.dstAttr.get(ctx.srcId) match {
            case Some(s) =>
                val nodes = ListBuffer[Long]()
                nodes ++= s
                ctx.sendToSrc(nodes)
            case None =>
        }
    }

    def mergeTriangle(msg1: ListBuffer[Long], msg2: ListBuffer[Long]): ListBuffer[Long] = {
        msg1 ++= msg2
        msg1
    }

    // Send true to src and dst node of an edge if that edge can be found in the map of the src node.
    def sendEdge(ctx: EdgeContext[(Map[(Long, Long), Int]), Boolean, Boolean]): Unit = {
        if (ctx.srcAttr.contains((ctx.srcId, ctx.dstId)))
            ctx.sendToDst(true)
        ctx.sendToSrc(true)
    }
}
