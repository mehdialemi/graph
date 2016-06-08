//package graph
//
//import graph.KTrussPregel.{MessageNeighbors, _}
//import org.apache.spark.graphx._
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.mutable.{HashSet, ListBuffer}
//
///**
//  * Created by mehdi on 6/5/16.
//  */
//object KTrussPregelLike {
//    case class VertexValue(phase: Int, degree: Int, vertices: HashSet[Long], edges: HashSet[SimpleEdge] = HashSet.empty[SimpleEdge])
//
//    class Message() {
//    }
//
//    case class MessageNeighbors(neighbors: HashSet[Long]) extends Message
//
//    case class MessageVertexSet(override val phase: Int, vertices: ListBuffer[Long]) extends Message(phase)
//
//    case class SimpleEdge(v1: Long, v2: Long)
//
//    case class MessageEdgeSet(override val phase: Int, edges: HashSet[SimpleEdge]) extends Message(phase)
//
//    val support = 4
//
//    def main(args: Array[String]): Unit = {
//        val input = "input.txt"
//        val config = new SparkConf();
//        config.setAppName("ktruss-pregel")
//        config.setMaster("local")
//        val sc = SparkContext.getOrCreate(config);
//        val supportBroadCast = sc.broadcast(support)
//
//        // Load int graph which is as a list of edges
//        val inputGraph = GraphLoader.edgeListFile(sc, input)
//
//        inputGraph.degrees.foreach(println(_))
//
//        // Set degree of each vertex in the property.
//        val graphVD = inputGraph.outerJoinVertices(inputGraph.degrees)((vid, v, deg) => deg)
//        graphVD.vertices.collect().foreach(println(_))
//
//        // initialize graph with property
//        var graph = graphVD.mapVertices((vId, degree) =>
//            vprog(vId, VertexValue(0, degree.getOrElse(0), HashSet.empty[Long]), new Message(0)))
//
//        graph.vertices.collect().foreach(println(_))
//
//        // Initialize graph
//        var message = graph.aggregateMessages(send, mergeMsg)
//        var oldMsgCount = 0L
//        var removeEdgeCount = 1L
//        var activeMessages = message.count()
//        // Loop until no message remain or maxIterations is achieved
//        var i = 0
//        while (activeMessages > 0 && i < 100 && (oldMsgCount != activeMessages && removeEdgeCount == 0)) {
//            // phase 1
//            graph.aggregateMessages(context =>
//                if (context.srcAttr.degree < context.dstAttr.degree)
//                    context.sendToDst(MessageNeighbors(HashSet(context.srcId)))
//            , (msg1, msg2) =>
//                  msg1.asInstanceOf[MessageNeighbors].neighbors += msg2.asInstanceOf[MessageNeighbors].neighbors.head
//                    msg1)
//
//            // Receive the message and update the vertices.
//
//            graph = graph.joinVertices(message)(vprog).cache()
//            val vertexHaveRemoveEdge = graph.vertices.filter { case (vid, value) => value.phase == 3 && value.edges.size > 0 }
//
//            removeEdgeCount = vertexHaveRemoveEdge.count()
//            if (removeEdgeCount > 0) {
//                val edgesToRemove = vertexHaveRemoveEdge.flatMap(e => {
//                    val list = new ListBuffer[Edge[Int]]()
//                    e._2.edges.foreach { edge => list += Edge[Int](edge.v1, edge.v2, 0) }
//                    list
//                })
//                val remainingEdges = graph.edges.subtract(edgesToRemove)
//                graph = Graph(graph.vertices, remainingEdges)
//            }
//            val oldMessages = message
//            // Send new message, skipping edges where neither side received a message. We must cache
//            // message so it can be materialized on the next line, allowing us to uncache the previous
//            // iteration.
//            message = graph.aggregateMessages(send, mergeMsg);
//            oldMsgCount = activeMessages
//            activeMessages = message.count()
//            i += 1
//        }
//    }
//
//    def send(context: EdgeContext[VertexValue, Int, Message]): Unit = {
//        context.srcAttr.phase match {
//            case 0 => // initial step to send node id from lower degree to higher degree node
//                if (context.srcAttr.degree < context.dstAttr.degree)
//                    context.sendToDst(MessageNeighbors(1, HashSet(context.srcId)))
//            case 1 =>
//                val list = ListBuffer.empty[(VertexId, MessageEdgeSet)]
//                if (context.srcAttr.degree < context.attr) {
//                    context.srcAttr.vertices -= context.dstId
//                    context.srcAttr.vertices.foreach { vId => context.sendToDst(MessageEdgeSet(2, HashSet(SimpleEdge(context.srcId, vId)))) };
//                }
//            case 2 =>
//                val list = ListBuffer.empty[(VertexId, MessageVertexSet)]
//                context.srcAttr.edges.map { edge =>
//                    list += ((edge.v1, MessageVertexSet(3, ListBuffer(context.srcId))))
//                    list += ((edge.v1, MessageVertexSet(3, ListBuffer(edge.v2))))
//                    list += ((context.srcId, MessageVertexSet(3, ListBuffer(edge.v2))))
//                }
//            case 3 =>
//                val list = ListBuffer.empty[(VertexId, MessageNeighbors)]
//                context.srcAttr.vertices.foreach { vId => list += ((vId, MessageNeighbors(1, HashSet(context.srcId)))) }
//                list.iterator
//        }
//    }
//
//    def mergeMsg(msg1: Message, msg2: Message): Message = {
//        msg1.phase match {
//            case 1 => msg1.asInstanceOf[MessageNeighbors].neighbors += msg2.asInstanceOf[MessageNeighbors].neighbors.head
//            case 2 => msg1.asInstanceOf[MessageEdgeSet].edges += msg2.asInstanceOf[MessageEdgeSet].edges.head
//            case 3 => msg1.asInstanceOf[MessageVertexSet].vertices += msg2.asInstanceOf[MessageVertexSet].vertices.head
//        }
//        msg1
//    }
//
//    def vprog(vertexId: VertexId, vertex: VertexValue, message: Message): VertexValue = {
//        message.phase match {
//            case 0 => vertex
//            case 1 =>
//                VertexValue(1, vertex.degree, message.asInstanceOf[MessageNeighbors].neighbors)
//            case 2 =>
//                val msg = message.asInstanceOf[MessageEdgeSet]
//                val triangleVertices = msg.edges.filter { edge => vertex.vertices.contains(edge.v2) }
//                VertexValue(2, vertex.degree, HashSet.empty[Long], triangleVertices)
//
//            case 3 =>
//                val list = HashSet.empty[Long]
//                val listRemove = HashSet.empty[SimpleEdge]
//                message.asInstanceOf[MessageVertexSet].vertices.groupBy(i => i).map(m => {
//                    if (m._2.length >= support)
//                        list += m._1
//                    else
//                        listRemove += SimpleEdge(vertexId, m._1)
//                })
//                VertexValue(3, vertex.degree, list, listRemove)
//        }
//    }
//
//}
