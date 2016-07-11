package graph.ktruss

import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

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

        // Find new edges with correct direction. A direction from a lower degree node to a higher degree node.
        val newEdges = graphVD.triplets.map { et =>
            if (et.srcAttr.get <= et.dstAttr.get)
                Edge(et.srcId, et.dstId, et.dstAttr.get)
            else
                Edge(et.dstId, et.srcId, et.srcAttr.get)
        }

        val localNewEdges = newEdges.collect()

        // Create graph with edge direction from lower degree to higher degree node and edge attribute.
        var graph = Graph(inputGraph.vertices, newEdges)
//        graph.edges.collect().foreach(println(_))
        graph.persist()

        // In a loop we find triangles and then remove edges lower than specified support
        var i = 0
        var complete = false
        while (i < 100 && !complete) {
            // phase 1: Send message about completing the third edges
            val idToDstMsg = graph.aggregateMessages(sendId, mergeIds)
            val graphLN = graph.outerJoinVertices(idToDstMsg)((vid, value, n) => n)

            // Find neighbors
            val neighbors = graph.collectNeighborIds(EdgeDirection.Out)
            val localNeighbors = neighbors.collect()

            // Update each nodes with its neighbors
            val graphN = graph.outerJoinVertices(neighbors)((vid, value, n) => n)
            val localGraphN = graphN.vertices.collect()

            // Send neighborIds to each neighbor
            val message = graphN.aggregateMessages(sendNeighbors, mergeNeighborIds)
            val localMsg = message.collect()

            // phase 2: Find triangles
            // Each node create and store messages to be sent to the neighbors about triangle
            val graphT = graphN.outerJoinVertices(message)((vid, n, msg) => {
                // Find triangles and make message
                var thirdEdgesMsg = mutable.Map[Long, Array[Long]]()
                val msgs = msg.orNull
                if (msgs != null) {
                    msgs.list.foreach { n1 =>
                        // common neighbor between current node neighbors and sender node neighbors
                        val commonNeighbors = n1.neighbors.intersect(n.get)

                        // report itself and other neighbors to the sender node
//                        val reportVertices = Array[Long](vid).union(commonNeighbors)
                        thirdEdgesMsg += n1.vId -> commonNeighbors

                        // send message to itself about triangle neighbors
//                        thirdEdgesMsg += vid -> commonNeighbors
                    }
                }
                thirdEdgesMsg
            })
            val localGraphT = graphT.vertices.collect()

            // phase 3: send messages in order to count edges and report them
            val triangleMsg = graphT.aggregateMessages(sendTriangle, mergeTriangle)
            val localTriangleMsg = triangleMsg.collect()

            val graphK = graph.outerJoinVertices(triangleMsg)((vid, value, tMsg) => {
                val edges = mutable.Map[Edge[Int], Int]()
                val list = tMsg.orNull
                if (list != null)
                    list.foreach(ne => edges += Edge(vid, ne, 0) -> 1)
                edges
            })
            val localGraphK = graphK.vertices.collect()

            // count edge occurrence
            val edgeCount = graphK.vertices.flatMap(t => t._2).reduceByKey((a, b) => a + b)
            val lec = edgeCount.collect()

            // find edges which should be removed
            val removeEdges = edgeCount.filter(ec => ec._2.toLong < sup.value).map(ec => (ec._1.srcId, ec._1.dstId))
            val lre = removeEdges.collect()

            // find nodes not received any messages and should be removed
            val graphActiveVertices = graphK.subgraph(vpred = (id, attr) => !attr.isEmpty)
//            val removeVertices = graphK.vertices.filter(t => t._2.isEmpty).map(t => (t._1, 0))
//            val lrv = removeVertices.collect()
            val reCount = removeEdges.count()
            val rvCount = graphActiveVertices.vertices.count()
            // If no extra edge as well as no extra vertex is found then exit from the loop
            if (reCount == 0 && rvCount == 0)
                complete = true

            // find remaining vertices
//            val remainingVertices = graph.vertices.minus(removeVertices)
//            val localRV = remainingVertices.collect()


            // find remaining edges
            val remainingEdges = graph.edges.map(e => (e.srcId, e.dstId))
              .subtract(removeEdges)
              .map(e => Edge(e._1, e._2, 0))
              .intersection(graphActiveVertices.edges)

            val localRE = remainingEdges.collect()

            // build new graph based on new vertices and edges
            val newGraph = Graph(sc.parallelize(Array((-1L, 0))), remainingEdges, (0))
            graph.unpersist() // unpersist previous graph
            graph = newGraph
            graph.persist()

            i += 1
        }

        graph.edges.collect().foreach(println(_))
    }

    def sendId(ctx: EdgeContext[Int, Int, ListBuffer[Long]]): Unit = {
        val msg = new ListBuffer[Long]()
        msg += ctx.srcId
        ctx.sendToDst(msg)
    }

    def mergeIds(msg1: ListBuffer[Long], msg2: ListBuffer[Long]): Unit = {
        msg1 ++= msg2
        msg1
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
}
