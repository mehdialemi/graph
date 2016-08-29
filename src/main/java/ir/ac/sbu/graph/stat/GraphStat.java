package ir.ac.sbu.graph.stat;

import ir.ac.sbu.graph.GraphLoader;
import ir.ac.sbu.graph.GraphUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

/**
 * Find statistics about the input ir.ac.sbu.graph.
 */
public class GraphStat {

    public static void main(String[] args) throws IOException {
        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        String dataset = new File(inputPath).getName();
        int index = inputPath.indexOf(dataset);
        String parentOutput = inputPath.substring(0, index);

        String outputPath = parentOutput + "/dist/" + dataset;
        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");

        GraphUtils.setAppName(conf, "Dist", partition, inputPath);

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);

        JavaPairRDD<Integer, Integer> degrees = edges.groupByKey().mapToPair(t -> {
            HashSet<Long> neighborSet = new HashSet<>();
            for (Long neighbor : t._2) {
                neighborSet.add(neighbor);
            }

            return new Tuple2<>(neighborSet.size(), 1);
        }).reduceByKey((a , b) -> a + b, 1).sortByKey();

        FileUtils.deleteDirectory(new File(outputPath));
        degrees.saveAsTextFile(outputPath);
        sc.close();
    }
}
