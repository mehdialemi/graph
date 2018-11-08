package ir.ac.sbu.graph.clusteringco

import ir.ac.sbu.graph.utils.{OutUtils, GraphUtils}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.TriangleCount
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Calculate LCC using GraphX framework
  */
object GraphX_LCC {

    def main(args: Array[String]) {
        var inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt"
        if (args != null && args.length > 0)
            inputPath = args(0);

        var partition = 2
        if (args != null && args.length > 1)
            partition = Integer.parseInt(args(1));

        val conf = new SparkConf()
        if (args == null || args.length == 0)
            conf.setMaster("local[2]")
        GraphUtils.setAppName(conf, "GraphX-LCC", partition, inputPath);
        val sc = new SparkContext(conf)

        val graph = GraphLoader.edgeListFile(sc, inputPath, numEdgePartitions=partition)

        val triangleGraph = TriangleCount.run(graph)

        // sum of all local clustering coefficient
        val sumLCC = triangleGraph.vertices.join(triangleGraph.degrees, partition)
            .filter(vertex => vertex._2._2 > 1)
            .map(vertex => 2 * vertex._2._1 / (vertex._2._2 * (vertex._2._2 - 1)).toFloat)  // local clustering coefficient in each node
            .reduce((a, sign) => a + sign) // sum all node's lcc

        val totalNodes = graph.vertices.count()
        val avgLCC = sumLCC / totalNodes

        OutUtils.printOutputLCC(totalNodes, sumLCC, avgLCC)
        sc.stop()
    }
}
