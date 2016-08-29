package ir.ac.sbu.graph.clusteringco;

import ir.ac.sbu.graph.GraphLoader;
import ir.ac.sbu.graph.GraphUtils;
import ir.ac.sbu.graph.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Calculate GCC with id based ordering. In this method we have no dealing with degree of nodes and to sort nodes in
 * the fonl just id of the nodes are taken into account.
 */
public class FonlIdGCC {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
        if (args != null && args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args != null && args.length > 1) {
            partition = Integer.parseInt(args[1]);
        }

        final int batchSize = 1000;

        SparkConf conf = new SparkConf();
        if (args == null || args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "Fonl-GCC-Id", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.CandidateState.class, int[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createFonlIdBasedInt(edges, partition);

        long nodes = fonl.count();

        JavaPairRDD<Integer, GraphUtils.CandidateState> candidate = FonlIdTC.generateCandidates(fonl, batchSize);

        long totalTriangles = FonlIdTC.countTriangles(candidate, fonl, partition);

        float globalCC = totalTriangles / (float) (nodes * (nodes - 1));

        OutUtils.printOutputGCC(nodes, totalTriangles, globalCC);
        sc.close();
    }
}
