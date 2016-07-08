package graph.clusteringco

import graph.OutputUtils
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.TriangleCount
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Calculate LCC using GraphX framework
  */
object GraphX_LCC {

    def main(args: Array[String]) {
        var inputPath = "input.txt"
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

        val graph = GraphLoader.edgeListFile(sc, inputPath, numEdgePartitions=partition,
            edgeStorageLevel=StorageLevel.MEMORY_ONLY, vertexStorageLevel=StorageLevel.MEMORY_ONLY)

        val triangleGraph = TriangleCount.run(graph)

        // sum of all local clustering coefficient
        val sumLCC = triangleGraph.vertices.join(triangleGraph.degrees, partition)
            .filter(v => v._2._2 > 1)
            .map(v => 2 * v._2._1 / (v._2._2 * (v._2._2 - 1)).toFloat)  // local clustering coefficient in each node
            .reduce((a, b) => a + b) // sum all node's lcc

        val totalNodes = graph.vertices.count()
        val avgLCC = sumLCC / totalNodes

        OutputUtils.printOutputLCC(totalNodes, sumLCC, avgLCC)
        sc.stop()
    }
}
