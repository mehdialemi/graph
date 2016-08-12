package graph.clusteringco;

import graph.GraphLoader;
import graph.GraphUtils;
import graph.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Counts total number of triangles in the given graph using degree based fonl. Copied from {@link FonlDegGCC}.
 */
public class FonlDegTC {

//    public static JavaPairRDD<Long, long[]> generateCandidates(JavaPairRDD<Long, long[]> fonl) {
//        return fonl.filter(t -> t._2.length > 2)
//            .flatMapToPair((PairFlatMapFunction<Tuple2<Long, long[]>, Long, long[]>) t -> {
//                int size = t._2.length - 1;
//                List<Tuple2<Long, long[]>> output = new ArrayList<>(size);
//                for (int index = 1; index < size; index++) {
//                    int len = size - index;
//                    long[] forward = new long[len + 1];
//                    forward[0] = t._1; // First vertex in the triangle
//                    System.arraycopy(t._2, index + 1, forward, 1, len);
//                    Arrays.sort(forward, 1, forward.length); // sort to comfort with fonl
//                    output.add(new Tuple2<>(t._2[index], forward));
//                }
//                return output.iterator();
//            });
//    }

//    public static JavaRDD<Tuple3<Long, Long, Long>> listTriangles(JavaSparkContext sc, String inputPath,
//                                                                  int partition) {
//        JavaPairRDD<Long, long[]> fonl = FonlUtils.loadFonl(sc, inputPath, partition);
//
        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of higherIds (high deg) would be allocated besides vertex with
        // low number of higherIds (high deg)

//        JavaPairRDD<Long, long[]> candidates = generateCandidates(fonl);
//        return candidates.cogroup(fonl, partition).flatMap(t -> {
//            List<Tuple3<Long, Long, Long>> triangles = new ArrayList<>();
//            Iterator<long[]> iterator = t._2._2.iterator();
//            if (!iterator.hasNext())
//                return triangles.iterator();
//
//            long[] hDegs = iterator.next();
//
//            iterator = t._2._1.iterator();
//            if (!iterator.hasNext())
//                return triangles.iterator();
//
//            Arrays.sort(hDegs, 1, hDegs.length);
//
//            do {
//                long[] forward = iterator.next();
//                List<Long> common = GraphUtils.sortedIntersection(hDegs, forward, 1, 1);
//                for (long v : common)
//                    triangles.add(GraphUtils.createSorted(forward[0], t._1, v));
//            } while (iterator.hasNext());
//
//            return triangles.iterator();
//        });
//
//    }


    public static void main(String[] args) {
//        String inputPath = "input.txt";
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "Fonl-TC-Deg", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);

        JavaPairRDD<Long, long[]> fonl = FonlUtils.createWith2ReduceNoSort(edges, partition);

        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of higherIds (high deg) would be allocated besides vertex with
        // low number of higherIds (high deg)
        JavaPairRDD<Long, long[]> candidates = generateCandidates(fonl);

        long totalTriangles = countTriangles(candidates, fonl, partition);

        OutUtils.printOutputTC(totalTriangles);

        sc.close();
    }

    public static long countTriangles(JavaPairRDD<Long, long[]> candidates, JavaPairRDD<Long, long[]> fonl,
                                      int partition) {
        return candidates.cogroup(fonl, partition).map(t -> {
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
    }

    public static JavaPairRDD<Long, long[]> generateCandidates(JavaPairRDD<Long, long[]> fonl) {
        return fonl.filter(t -> t._2.length > 2).flatMapToPair(t -> {
                    int size = t._2.length - 1;
                    List<Tuple2<Long, long[]>> output = new ArrayList<>(size - 1);
                    for (int index = 1; index < size; index++) {
                        int len = size - index;
                        long[] forward = new long[len];
                        System.arraycopy(t._2, index + 1, forward, 0, len);
                        output.add(new Tuple2<>(t._2[index], forward));
                    }
                    return output.iterator();
                }).mapValues(t -> { // sort candidates
                    Arrays.sort(t);
                    return t;
                });
    }
}
