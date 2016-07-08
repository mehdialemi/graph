package graph.clusteringco;

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
 * Calculate Global Clustering Coefficient (GCC) using fonl structure which is sorted based on degree of nodes. This
 * causes that in all steps of program we could have a balanced workload. In finding GCC we only require total
 * triangle count and we don't need to maintain triangle count per node. So, we could have better performance in
 * comparison with Local Clustering Coefficient (LCC) which we should have number of triangles per node.
 */
public class FonlDegGCC {

    public static void main(String[] args) {
        String inputPath = "input.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "Fonl-GCC-Deg", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Long, Long> edges = GraphUtils.loadUndirectedEdges(input);

        JavaPairRDD<Long, long[]> fonl = FonlUtils.createFonlDegreeBased(edges, partition);

        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of higherIds (high deg) would be allocated besides vertex with
        // low number of higherIds (high deg)
        JavaPairRDD<Long, long[]> candidates = fonl
            .filter(t -> t._2.length > 2)
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, long[]>, Long, long[]>() {
                @Override
                public Iterable<Tuple2<Long, long[]>> call(Tuple2<Long, long[]> t) throws Exception {
                    int size = t._2.length - 1;
                    List<Tuple2<Long, long[]>> output = new ArrayList<>(size - 1);
                    for (int index = 1; index < size; index++) {
                        int len = size - index;
                        long[] forward = new long[len];
                        System.arraycopy(t._2, index + 1, forward, 0, len);
                        output.add(new Tuple2<>(t._2[index], forward));
                    }
                    return output;
                }
            }).mapValues(t -> { // sort candidates
                Arrays.sort(t);
                return t;
            });

        long totalTriangles = candidates.cogroup(fonl, partition).map(t -> {
                Iterator<long[]> iterator = t._2._2.iterator();
                if (!iterator.hasNext())
                    return 0L;
                long[] hDegs = iterator.next();

                iterator = t._2._1.iterator();
                if (!iterator.hasNext())
                    return 0L;

                Arrays.sort(hDegs, 1, hDegs.length);
                long sum = 0;

                do {
                    long[] forward = iterator.next();
                    int count = GraphUtils.sortedIntersectionCount(hDegs, forward, null, 1, 0);
                    sum += count;
                } while (iterator.hasNext());

                return sum;
            }).reduce((a, b) -> a + b);

        long totalNodes = fonl.count();
        float globalCC = totalTriangles / (float) (totalNodes * (totalNodes - 1));
        GraphUtils.printOutputGCC(totalNodes, totalTriangles, globalCC);
        sc.close();
    }
}
