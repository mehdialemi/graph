package graph.clusteringco

import graph.{GraphUtils, OutputUtils}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.TriangleCount
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object GraphX_GCC {

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
        GraphUtils.setAppName(conf, "GraphX-GCC", partition, inputPath);
        val sc = new SparkContext(conf)

        val graph = GraphLoader.edgeListFile(sc, inputPath, numEdgePartitions=partition)

        val triangleGraph = TriangleCount.run(graph)
        val triangle3 = triangleGraph.vertices.reduce((v1,v2) => (0L , v1._2 + v2._2))
        val totalTriangles = triangle3._2.toInt / 3;

        val nodes = graph.vertices.count();

        val globalCC = totalTriangles / (nodes * (nodes - 1)).toFloat;

        OutputUtils.printOutputGCC(nodes, totalTriangles, globalCC)
        sc.stop()
    }
}
