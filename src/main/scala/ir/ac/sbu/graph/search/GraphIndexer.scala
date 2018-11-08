package ir.ac.sbu.graph.search

import org.apache.spark.graphx.{EdgeContext, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by mehdi on 6/6/16.
  */
object GraphIndexer {

    case class NodeValue(label: String, rank: Float, id: Long)
    case class STwig(label: String, rootId: Long, children: ListBuffer[Long])
    case class Message(neighbors: ListBuffer[(NodeValue, Long)]) // rank, degree, nodeId

    def main(args: Array[String]) {
        val inputPath = "input.txt"
        val labelPath = "label.txt"
        val config = new SparkConf()
        config.setAppName("ktruss-pregel")
        config.setMaster("local")
        val sc = SparkContext.getOrCreate(config)

        // Load int ir.ac.sbu.graph which is as a list of edges
        val inputGraph = GraphLoader.edgeListFile(sc, inputPath)
        val labelMap = sc.textFile(labelPath).map(t => {
            val split = t.split("\\s+")
            split(0).toLong -> split(1)
        }).cache()

        val labelCount = labelMap.map(t => t._2).countByValue
        val labelCountMap = sc.broadcast(labelCount)
        val graph = inputGraph.outerJoinVertices(inputGraph.degrees)((vid, vertex, degree) => degree).cache()
        inputGraph.unpersist()

        val labelCountGraph = graph.outerJoinVertices(labelMap)((vid, degree, label) => {
            val popularity = labelCountMap.value.get(label.get).get.toFloat
            val rank = degree.get / popularity
            NodeValue(label.get, rank, vid)
        }).cache()

        val message = labelCountGraph.aggregateMessages(send, mergeMsg)
        val sTwigs = labelCountGraph.outerJoinVertices(message){ (id, value, msg) =>
            val children = new ListBuffer[Long]()
            val list = msg.orNull
            if (list != null)
                list.neighbors.filter(t => {
                t._1.rank < value.rank || (t._1.rank == value.rank && t._1.id < value.id)
            }).map(t => children += t._2)
            STwig(value.label, id, children)
        }.vertices.filter{ case (vId, property) =>
            property.children.nonEmpty
        }.map { case (vId, stwig) => (stwig.label, stwig) }.groupByKey.persist()

        sTwigs.foreach(println(_))
    }

    def send(ctx: EdgeContext[NodeValue, Int, Message]): Unit = {
        ctx.sendToDst(Message(ListBuffer((ctx.srcAttr, ctx.srcId))))
    }

    def mergeMsg(msg1: Message, msg2: Message): Message = {
        msg1.neighbors += msg2.neighbors.head
        msg1
    }
}
