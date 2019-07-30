package ir.ac.sbu.graph.clusteringco;

import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import ir.ac.sbu.graph.utils.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Calculate Global Clustering Coefficient (GCC) using fonl structure which is sorted based on degree of nodes. This
 * causes that in all steps of program we could have a balanced workload. In finding GCC we only require total
 * triangle count and we don't need to maintain triangle count per node. So, we could have better performance in
 * comparison with Local Clustering Coefficient (LCC) which we should have number of triangles per node.
 */
public class FonlDegGCC {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "LocalFonl-GCC-Deg", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);

        JavaPairRDD<Long, long[]> fonl = FonlUtils.createWith2ReduceNoSort(edges, partition);

        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of higherIds (high degree) would be allocated besides vertex with
        // low number of higherIds (high degree)
        JavaPairRDD<Long, long[]> candidates = FonlDegTC.generateCandidates(fonl);

        long totalTriangles = FonlDegTC.countTriangles(candidates, fonl, partition);

        long totalNodes = fonl.count();
        float globalCC = totalTriangles / (float) (totalNodes * (totalNodes - 1));
        OutUtils.printOutputGCC(totalNodes, totalTriangles, globalCC);
        sc.close();
    }
}
