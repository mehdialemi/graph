package graph.clusteringco;

import graph.GraphLoader;
import graph.GraphUtils;
import graph.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Calculate local clustering coefficient using fonl structure which is sorted based on degree of nodes. This causes
 * that in all steps of program we could have a balanced workload.
 */
public class FonlDegLCC {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "Fonl-LCC-Deg", partition, inputPath);
        conf.registerKryoClasses(new Class[] {GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);

        JavaPairRDD<Long, long[]> fonl = FonlUtils.createWith2Reduce(edges, partition);
        long totalNodes = fonl.count();

        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of higherIds (high deg) would be allocated besides vertex with
        // low number of higherIds (high deg)
        JavaPairRDD<Long, long[]> candidates = fonl
            .filter(t -> t._2.length > 2)
            .flatMapToPair(t -> {
                int size = t._2.length - 1;
                List<Tuple2<Long, long[]>> output = new ArrayList<>(size);
                for (int index = 1; index < size; index++) {
                    int len = size - index;
                    long[] forward = new long[len + 1];
                    forward[0] = t._1; // First vertex in the triangle
                    System.arraycopy(t._2, index + 1, forward, 1, len);
                    Arrays.sort(forward, 1, forward.length); // sort to comfort with fonl
                    output.add(new Tuple2<>(t._2[index], forward));
                }
                return output.iterator();
            });

        JavaPairRDD<Long, Integer> localTriangleCount = candidates.cogroup(fonl, partition)
            .flatMapToPair(t -> {
                Iterator<long[]> iterator = t._2._2.iterator();
                List<Tuple2<Long, Integer>> output = new ArrayList<>();
                if (!iterator.hasNext())
                    return output.iterator();

                long[] hDegs = iterator.next();

                iterator = t._2._1.iterator();
                if (!iterator.hasNext())
                    return output.iterator();

                Arrays.sort(hDegs, 1, hDegs.length);

                int sum = 0;
                do {
                    long[] forward = iterator.next();
                    int count = GraphUtils.sortedIntersectionCount(hDegs, forward, output, 1, 1);
                    if (count > 0) {
                        sum += count;
                        output.add(new Tuple2<>(forward[0], count));
                    }
                } while (iterator.hasNext());

                if (sum > 0) {
                    output.add(new Tuple2<>(t._1, sum));
                }
                return output.iterator();
            })
            .reduceByKey((a, b) -> a + b);

        Float sumLCC = localTriangleCount.filter(t -> t._2 > 0).join(fonl, partition)
            .mapValues(t -> 2 * t._1 / (float) (t._2[0] * (t._2[0] - 1)))
            .map(t -> t._2)
            .reduce((a, b) -> a + b);

        float avgLCC = sumLCC / totalNodes;
        OutUtils.printOutputLCC(totalNodes, sumLCC, avgLCC);

        sc.close();
    }
}
