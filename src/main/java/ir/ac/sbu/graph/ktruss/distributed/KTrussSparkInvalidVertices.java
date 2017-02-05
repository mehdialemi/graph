package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This class find k-truss sub-graphs from the big graph stored as edge list in the hdfs (Hadoop File System).
 * This program is written by spark framework and would be run on the spark cluster. To extract k-truss sub-graphs,
 * the following tasks is performed:
 * 1- Find triangles and produce key-values as (edge, triangle vertices).
 * 2- Repetitively remove edges which have support lower than the specified value.
 */
public class KTrussSparkInvalidVertices {

    public static final float MIN_THRESHOLD = 0.1F;
    public static final int[] EMPTY_INT_ARRAY = new int[]{};

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 10;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);
        final int minSup = k - 2;

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "KTrussSparkInvalidVertices-" + k + "-MultiSteps", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, int[].class, Iterable.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long tStart = System.currentTimeMillis();

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partition);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl);

        // Generate kv such that key is an edge and value is its triangle vertices.
        Partitioner partitioner = new HashPartitioner(partition);

        JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> edgeVertices = candidates.cogroup(fonl).flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;
            List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>();
            for (int[] cVal : t._2._1) {
                int u = cVal[0];

                // The intersection determines triangles which u and v are two of their vertices.
                IntList wList = GraphUtils.sortedIntersectionTest(fVal, 1, cVal, 1);
                if (wList == null)
                    continue;

                // Always generate and edge (u, v) such that u < v.
                Tuple2<Integer, Integer> uv;
                if (u < v)
                    uv = new Tuple2<>(u, v);
                else
                    uv = new Tuple2<>(v, u);

                IntListIterator wIter = wList.iterator();
                while (wIter.hasNext()) {
                    int w = wIter.nextInt();
                    output.add(new Tuple2<>(uv, w));
                    if (u < w)
                        output.add(new Tuple2<>(new Tuple2<>(u, w), v));
                    else
                        output.add(new Tuple2<>(new Tuple2<>(w, u), v));

                    if (v < w)
                        output.add(new Tuple2<>(new Tuple2<>(v, w), u));
                    else
                        output.add(new Tuple2<>(new Tuple2<>(w, v), u));
                }
            }

            return output.iterator();
        }).groupByKey().partitionBy(partitioner)
//            .mapValues(values -> {
//            // TODO use iterator here instead of creating a set and filling it
//            IntSet set = new IntOpenHashSet();
//            for (int v : values) {
//                set.add(v);
//            }
//            return set;
//        })
            .persist(StorageLevel.MEMORY_ONLY()); // Use disk too if graph is very large

        JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> prevEdgeVertices = edgeVertices;

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, int[]>> edgeSup = edgeVertices
            .mapValues(v -> {
                int count = 0;
                for (Integer integer : v) {
                    count ++;
                }
                return new Tuple2<>(count, EMPTY_INT_ARRAY);
            }).partitionBy(partitioner)
            .persist(StorageLevel.MEMORY_ONLY());

        long tEdgeSup = System.currentTimeMillis();
        log("Create edgeSup ", tStart, tEdgeSup);

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, int[]>> prevEdgeSup = edgeSup;
        int iteration = 0;
        while (true) {
            long t1 = System.currentTimeMillis();
            log("Iteration: " + ++iteration);
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, int[]>> invalids =
                edgeSup.filter(value -> (value._2._1 - value._2._2.length) < minSup);

            long invalidCount = invalids.count();

            long t2 = System.currentTimeMillis();
            long duration = t2 - t1;
            log("Invalid Count: " + invalidCount, duration);

            if (invalidCount == 0)
                break;

            // Join invalids with  edgeVertices
            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> invalidUpdate =
                invalids.leftOuterJoin(edgeVertices) // TODO find a best partition and test the reverse join
                    .flatMapToPair(kv -> {
                        if (!kv._2._2.isPresent())
                            return Collections.emptyIterator();

                        Tuple2<Integer, int[]> invalid = kv._2._1;
                        int[] invalidVertices = invalid._2;
                        List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>((invalid._1 - invalidVertices.length) * 2);

                        int u = kv._1._1;
                        int v = kv._1._2;

                        // Generate the vertices which should be reported as invalid for the edge of the current triangles.
                        for (Integer w : kv._2._2.get()) {

                            // Skip invalids
                            if (Arrays.binarySearch(invalidVertices, w) >= 0)
                                continue;

                            if (u < w)
                                out.add(new Tuple2<>(new Tuple2<>(u, w), v));
                            else
                                out.add(new Tuple2<>(new Tuple2<>(w, u), v));

                            if (v < w)
                                out.add(new Tuple2<>(new Tuple2<>(v, w), u));
                            else
                                out.add(new Tuple2<>(new Tuple2<>(w, v), u));
                        }

                        return out.iterator();
                    }).groupByKey().partitionBy(partitioner); // TODO repartition?

            // Join invalid update with edgeSup to update the current edgeSup.
            // By this join the list in the value part would be updated with those invalid vertices for the current edge
            edgeSup = invalidUpdate.rightOuterJoin(edgeSup) // TODO find a best partition
                .mapValues(joinValue -> {

                    // Calculate previous support
                    int initSup = joinValue._2._1;
                    int prevSup = initSup - joinValue._2._2.length;
                    if (!joinValue._1.isPresent()) {
                        if (prevSup < minSup) { // Skip if this edge is already detected as invalid
                            return null;
                        }
                        return joinValue._2;
                    }

                    // Update the invalid list for the current value.
                    // TODO we can store SortedSet or Set as value
                    // TODO create this object just once in the partition and reuse it
                    IntSortedSet invalidVertices = new IntAVLTreeSet(joinValue._2._2);
                    for (int u : joinValue._1.get()) {
                        invalidVertices.add(u);
                    }

                    // If new support is zero this means that no additional
                    // invalid update would be produced by the current edge
                    int newSup = initSup - invalidVertices.size();
                    if (newSup == 0)
                        return null;

                    // Generate output value such that the value in index zero is init support and
                    // the remaining is related to maintain invalid vertices of the current edge.
                    return new Tuple2<>(initSup, invalidVertices.toIntArray());
                }).filter(kv -> kv._2 != null).partitionBy(partitioner).persist(StorageLevel.MEMORY_ONLY());  // TODO repartition?

            // Set blocking true
            prevEdgeSup.unpersist();

            // TODO calculate the best repartition for each steps of the above algorithm using the information of graph sizes
//            Float sumRatio = edgeSup.map(kv -> (kv._2._1 - kv._2._2.length) / (float) kv._2._1).reduce((a, b) -> a + b);
//            long edgeCount = edgeSup.count();
//            float ratio = sumRatio / (float) edgeCount;
//            long t3 = System.currentTimeMillis();
//            log("ratio: " + ratio + ", edgeCount: " + edgeCount + ", sumRatio: " + sumRatio, t1, t3);
//            if (ratio < MIN_THRESHOLD) {
//                edgeVertices = edgeVertices.join(edgeSup).mapValues(joinValues -> {
//                    Tuple2<Integer, int[]> localEdgeSup = joinValues._2;
//                    int newSup = localEdgeSup._1 - localEdgeSup._2.length;
//                    if (newSup > MIN_THRESHOLD)
//                        return joinValues._1;
//
//                    List<Integer> list = new ArrayList<>(newSup);
//
//                    for (Integer v : joinValues._1) {
//                        if (Arrays.binarySearch(joinValues._2._2, v) >= 0)
//                            continue;
//                        list.add(v);
//                    }
//                    return list;
//
//                }).repartition(partition).persist(StorageLevel.MEMORY_ONLY());
//                prevEdgeVertices.unpersist(true);
//            }

        }

        long tEnd = System.currentTimeMillis();
        log("Count: " + edgeSup.count(), tStart, tEnd);
        sc.close();
    }

    private static void log(String msg, long start, long end) {
        log(msg, (end - start));
    }

    private static void log(String msg) {
        log(msg, -1);
    }

    private static void log(String msg, long duration) {
        if (duration == -1)
            System.out.println("KTRUSS " + msg);
        else
            System.out.println("KTRUSS " + msg + ", duration: " + duration + " ms");
    }
}
