package graph.ktruss

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer}

/**
  * Find ktruss subgraph using pregel like procedure.
  * This implemented pregel like system
  */
object KTrussPregel {

    case class OneNeighborMsg(vId: Long, neighbors: Array[Long])

    case class NeighborMessage(list: ListBuffer[OneNeighborMsg])

    val support = 4

    def main(args: Array[String]): Unit = {
        val input = "input.txt"
        val config = new SparkConf()
        config.setAppName("ktruss-pregel")
        config.setMaster("local")
        val sc = SparkContext.getOrCreate(config)
        val sup = sc.broadcast(support - 2)

        // Load int graph which is as a list of edges
        val inputGraph = GraphLoader.edgeListFile(sc, input)

        // Change direction from lower degree node to a higher node
        // First find degree of each node
        // Second find correct edge direction
        // Third create a new graph with new edges and previous vertices

        // Set degree of each vertex in the property.
        val graphVD = inputGraph.outerJoinVertices(inputGraph.degrees)((vid, v, deg) => deg)
        graphVD.vertices.foreach(println(_))

        // Find new edges with correct direction
        val newEdges = graphVD.triplets.map { et =>
            if (et.srcAttr.get <= et.dstAttr.get)
                Edge(et.srcId, et.dstId, et.dstAttr.get)
            else
                Edge(et.dstId, et.srcId, et.srcAttr.get)
        }

        // Create graph wit correct edge direction and edge attribute
        var graph = Graph(inputGraph.vertices, newEdges)
//        graph.edges.collect().foreach(println(_))
        graph.persist()

        // In a loop we find triangles and then remove edges lower than specified support
        var i = 0
        var complete = false
        while (i < 100 && !complete) {
            // phase 1: Send message about completing the third edges
            // Find neighbors
            val neighbors = graph.collectNeighborIds(EdgeDirection.Out)
            // Update each nodes with its neighbors
            val graphN = graph.outerJoinVertices(neighbors)((vid, value, n) => n)
            // Send neighborIds to each neighbor
            val message = graphN.aggregateMessages(sendNeighbors, mergeNeighborIds)

            // phase 2: Find triangles
            // Each node create and store messages to be sent to the neighbors about triangle
            val graphT = graphN.outerJoinVertices(message)((vid, n, msg) => {
                // Find triangles and make message
                var thirdEdgesMsg = mutable.Map[Long, Array[Long]]()
                val msgs = msg.orNull
                if (msgs != null) {
                    msgs.list.foreach { n1 =>
                        // common neighbor between current node neighbors and sender node neighbors
                        val commonEdges = n1.neighbors.intersect(n.get)

                        // report itself and other neighbors to the sender node
                        val reportVertices = Array[Long](vid).union(commonEdges)
                        thirdEdgesMsg += n1.vId -> reportVertices

                        // send message to itself about triangle neighbors
                        thirdEdgesMsg += vid -> commonEdges
                    }
                }
                thirdEdgesMsg
            })

            // phase 3: send messages in order to count edges and report them
            val triangleMsg = graphT.aggregateMessages(sendTriangle, mergeTriangle)
            val graphK = graph.outerJoinVertices(triangleMsg)((vid, value, tMsg) => {
                val edges = mutable.Map[Edge[Int], Int]()
                val list = tMsg.orNull
                if (list != null)
                    list.foreach(ne => edges += Edge(vid, ne, 0) -> 1)
                edges
            })

            // count edge occurrence
            val edgeCount = graphK.vertices.flatMap(t => t._2).reduceByKey((a, b) => a + b)

            // find edges which should be removed
            val removeEdges = edgeCount.filter(ec => ec._2 < sup.value).map(ec => (ec._1.srcId, ec._1.dstId))

            // find nodes not received any messages and should be removed
            val removeVertices = graphK.vertices.filter(t => t._2.isEmpty).map(t => (t._1, 0))

            val reCount = removeEdges.count()
            val rvCount = removeVertices.count()
            // If no extra edge as well as no extra vertex is found then exit from the loop
            if (reCount == 0 && rvCount == 0)
                complete = true

            // find remaining vertices
            val remainingVertices = graph.vertices.minus(removeVertices)

            // find remaining edges
            val remainingEdges = graph.edges.map(e => (e.srcId, e.dstId))
              .subtract(removeEdges).map(e => Edge(e._1, e._2, 0))

            // build new graph based on new vertices and edges
            val newGraph = Graph(remainingVertices, remainingEdges)
            graph.unpersist() // unpersist previous graph
            graph = newGraph
            graph.persist()

            i += 1
        }

        graph.edges.collect().foreach(println(_))
    }

    // Send neighborIds to a neighbor
    def sendNeighbors(ctx: EdgeContext[Option[Array[Long]], Int, NeighborMessage]): Unit = {
        val msg = new ListBuffer[OneNeighborMsg]()
        msg += OneNeighborMsg(ctx.srcId, ctx.srcAttr.get)
        ctx.sendToDst(NeighborMessage(msg))
    }

    def mergeNeighborIds(msg1: NeighborMessage, msg2: NeighborMessage): NeighborMessage = {
        msg1.list ++= msg2.list
        msg1
    }

    // Send completing node to the neighbor
    def sendTriangle(ctx: EdgeContext[mutable.Map[Long, Array[Long]], Int, ListBuffer[Long]]): Unit = {
        ctx.srcAttr.get(ctx.dstId) match {
            case Some(s) =>
                val nodes = ListBuffer[Long]()
                nodes ++= s
                ctx.sendToDst(nodes)
            case None =>
        }
    }

    def mergeTriangle(msg1: ListBuffer[Long], msg2: ListBuffer[Long]): ListBuffer[Long] = {
        msg1 ++= msg2
        msg1
    }
}
