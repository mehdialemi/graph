//package ir.ac.sbu.redispark;
//
//import ir.ac.sbu.graph.GraphLoader;
//import ir.ac.sbu.graph.GraphUtils;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//
//import java.io.File;
//
///**
// * Created by mehdi on 8/30/16.
// */
//public class GraphStatRedisJava {
//
//    public static void main(String[] args) {
//        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
//        if (args.length > 0)
//            inputPath = args[0];
//
//        String dataset = new File(inputPath).getName();
//        int index = inputPath.indexOf(dataset);
//        String parentOutput = inputPath.substring(0, index);
//
//        String outputPath = parentOutput + "/dist/" + dataset;
//        int partition = 2;
//        if (args.length > 1)
//            partition = Integer.parseInt(args[1]);
//
//        SparkConf conf = new SparkConf();
//        if (args.length == 0)
//            conf.setMaster("local[2]");
//
//        GraphUtils.setAppName(conf, "Dist", partition, inputPath);
//
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        JavaRDD<String> input = sc.textFile(inputPath, partition);
//
//        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);
//
//        JavaRedisRDD rdd = new JavaRedisRDD(sc, "localhost", 6379, edges.de)
//    }
//}
