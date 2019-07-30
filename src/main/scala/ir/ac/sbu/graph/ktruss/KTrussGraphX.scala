package ir.ac.sbu.graph.ktruss

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader

/**
  * Created by mehdi on 2/21/18.
  */
object KTrussGraphX {
  def main(args: Array[String]): Unit = {
    var inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt"
    if (args != null && args.length > 0)
      inputPath = args(0);

    var k = 4
    if (args.length > 2)
      k = args(1).toInt
    val support: Int = k - 2

    val conf = new SparkConf()
    if (args == null || args.length == 0)
      conf.setMaster("local[2]")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.setAppName("KTruss-Pregel-" + k + "-(" + new File(inputPath).getName + ")")

    val sc = SparkContext.getOrCreate(conf)

    val start = System.currentTimeMillis()
    // Load int ir.ac.sbu.graph which is as a list of triangleEdges
    val inputGraph = GraphLoader.edgeListFile(sc, inputPath)

  }
}
