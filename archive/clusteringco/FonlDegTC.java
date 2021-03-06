package ir.ac.sbu.graph.clusteringco;

import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import ir.ac.sbu.graph.utils.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 * Counts total number of triangles in the given ir.ac.sbu.graph using degree based fonl. Copied from {@link FonlDegGCC}.
 */
public class FonlDegTC {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        boolean persistOnDisk = false;
        if (args.length > 2)
            persistOnDisk = Boolean.parseBoolean(args[2]);

        boolean repartitionFonl = false;
        if (args.length > 3)
            repartitionFonl = Boolean.parseBoolean(args[3]);

        boolean useSort = true;
        if (args.length > 4)
            useSort = Boolean.parseBoolean(args[4]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "LocalFonl-TC-Deg", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);

        JavaPairRDD<Long, long[]> fonl;
        if (useSort)
            fonl = FonlUtils.createWith2Reduce(edges, partition, persistOnDisk, repartitionFonl);
        else
            fonl = FonlUtils.createWith2ReduceNoSort(edges, partition);

        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of higherIds (high degree) would be allocated besides vertex with
        // low number of higherIds (high degree)
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
        return generateCandidates(fonl, false);
    }

    public static JavaPairRDD<Long, long[]> generateCandidates(JavaPairRDD<Long, long[]> fonl, final boolean addFirst) {
        return generateCandidates(fonl, addFirst, true);
    }

    public static JavaPairRDD<Long, long[]> generateCandidates(JavaPairRDD<Long, long[]> fonl, final boolean addFirst,
                                                               final boolean sortById) {
        return fonl.filter(t -> t._2.length > 2).flatMapToPair(t -> {
            int size = t._2.length - 1; // one is for the first index holding node's degree

            if (size == 1)
                return Collections.emptyIterator();

            List<Tuple2<Long, long[]>> output;
            if (addFirst)
                output = new ArrayList<>(size);
            else
                output = new ArrayList<>(size - 1);

            for (int index = 1; index < size; index++) {
                int len = size - index;
                long[] cvalue;
                if (addFirst) {
                    cvalue = new long[len + 1];
                    cvalue[0] = t._1; // First vertex in the triangle
                    System.arraycopy(t._2, index + 1, cvalue, 1, len);
                    if (sortById)
                        Arrays.sort(cvalue, 1, cvalue.length); // quickSort to comfort with fonl
                } else {
                    cvalue = new long[len];
                    System.arraycopy(t._2, index + 1, cvalue, 0, len);
                    if (sortById)
                        Arrays.sort(cvalue);
                }
                output.add(new Tuple2<>(t._2[index], cvalue));
            }

            return output.iterator();
        });
    }

    public static JavaPairRDD<Integer, int[]> generateCandidatesInteger(JavaPairRDD<Integer, int[]> fonl) {
        return fonl
            .filter(t -> t._2.length > 2) // Select vertices having more than 2 items in their values
            .flatMapToPair(t -> {

                int size = t._2.length - 1; // one is for the first index holding node's degree

                if (size == 1)
                    return Collections.emptyIterator();

                List<Tuple2<Integer, int[]>> output;
                output = new ArrayList<>(size);

                for (int index = 1; index < size; index++) {
                    int len = size - index;
                    int[] cvalue = new int[len + 1];
                    cvalue[0] = t._1; // First vertex in the triangle
                    System.arraycopy(t._2, index + 1, cvalue, 1, len);
                    Arrays.sort(cvalue, 1, cvalue.length); // quickSort to comfort with fonl
                    output.add(new Tuple2<>(t._2[index], cvalue));
                }

                return output.iterator();
            });
    }
}
