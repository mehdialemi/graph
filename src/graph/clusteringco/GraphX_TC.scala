package graph.clusteringco

import graph.OutputUtils
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.lib.TriangleCount
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Count total number of triangles in the graph using GraphX framework.
  */
object GraphX_TC {
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

        val graph = GraphLoader.edgeListFile(sc, inputPath, numEdgePartitions=partition,
            edgeStorageLevel=StorageLevel.MEMORY_AND_DISK, vertexStorageLevel=StorageLevel.MEMORY_AND_DISK)

        val triangleGraph = TriangleCount.run(graph)
        val triangle3 = triangleGraph.vertices.reduce((v1,v2) => (0L , v1._2 + v2._2))
        val totalTriangles = triangle3._2.toInt / 3;

        OutputUtils.printOutputTC(totalTriangles)
        sc.stop()
    }
}
