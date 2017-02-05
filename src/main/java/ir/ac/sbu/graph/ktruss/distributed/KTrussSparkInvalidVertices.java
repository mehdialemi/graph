package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * This class find k-truss sub-graphs from the big graph stored as edge list in the hdfs (Hadoop File System).
 * This program is written by spark framework and would be run on the spark cluster. To extract k-truss sub-graphs,
 * the following tasks is performed:
 * 1- Find triangles and produce key-values as (edge, triangle vertices).
 * 2- Repetitively remove edges which have support lower than the specified value.
 */
public class KTrussSparkInvalidVertices {

    public static void main(String[] args) {
//        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
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
        GraphUtils.setAppName(conf, "KTruss-EdgeVertexList-" + k + "-MultiSteps", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, long[].class, List.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long tStart = System.currentTimeMillis();

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partition);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl);

        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> edgeVertices = candidates.cogroup(fonl).flatMapToPair(t -> {
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
        }).groupByKey().mapValues(values -> {
            IntSet set = new IntOpenHashSet();
            for (int v : values) {
                set.add(v);
            }
            return set;
        }).repartition(partition)
            .persist(StorageLevel.MEMORY_ONLY()); // Use disk too if graph is very large

        long tEdgeVertices = System.currentTimeMillis();
        log("Completed edge vertices", tStart, tEdgeVertices);

        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> prevEdgeVertices = edgeVertices;
        JavaPairRDD<Tuple2<Integer, Integer>, int[]> edgeSup = edgeVertices.mapValues(v -> new int[]{v.size()}).cache();

        long tEdgeSup = System.currentTimeMillis();
        log("Create edgeSup ", tEdgeVertices, tEdgeSup);

        JavaPairRDD<Tuple2<Integer, Integer>, int[]> prevEdgeSup = edgeSup;
        int iteration = 0;
        long duration = 0;
        long sumDuration = 0;
        int count = 0;
        boolean reloaded = true;
        while (true) {
            if (count != 0 && !reloaded && duration > (sumDuration / (float) count)) {
                float avg = sumDuration / (float) count;
                log("Reloading edge vertices, duration: " + duration + ", avg duration: " + avg + " , count: " + count, -1);
                edgeVertices = edgeVertices.join(edgeSup).mapValues(joinValues -> {
                    for (int invalidVertex : joinValues._2) {
                        joinValues._1.remove(invalidVertex);
                    }
                    return joinValues._1;
                }).repartition(partition).cache();

                reloaded = true;
                prevEdgeVertices.unpersist();
            }

            long t1 = System.currentTimeMillis();
            log("iteration: " + ++iteration, -1);
            JavaPairRDD<Tuple2<Integer, Integer>, int[]> invalids = edgeSup.filter(value -> (value._2[0] + 1 - value._2.length) < minSup);

            long invalidCount = invalids.count();
            long t2 = System.currentTimeMillis();
            duration = t2 - t1;
            if (reloaded) {
                reloaded = false;
                sumDuration = 0;
                count = 0;
            } else {
                sumDuration += duration;
                count ++;
            }

            log("invalid invalidCount: " + invalidCount, duration);
            if (invalidCount == 0)
                break;

            // Join invalids with  edgeVertices
            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> invalidUpdate =
                invalids.join(edgeVertices) // TODO find a best partition and test the reverse join
                    .flatMapToPair(kv -> {

                    // Remove invalid vertices from those which are not marked as invalid for the current edge.
                    // TODO this remove could be replaced with contains and skip in the following loop
                    for (int i = 1; i < kv._2._1.length; i++) {
                        kv._2._2.remove(kv._2._1[i]);
                    }

                    List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>(kv._2._2.size() * 2);

                    // Generate the vertices which should be reported as invalid for the edge of the current triangles.
                    int u = kv._1._1;
                    int v = kv._1._2;
                    for (int w : kv._2._2) {
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
                }).groupByKey(); // TODO repartition?

            // Join invalid update with edgeSup to update the current edgeSup.
            // By this join the list in the value part would be updated with those invalid vertices for the current edge
            edgeSup = invalidUpdate.rightOuterJoin(edgeSup) // TODO find a best partition
                .mapValues(joinValue -> {

                // Calculate previous support
                int initSup = joinValue._2[0];
                int prevSup = initSup + 1 - joinValue._2.length;
                if (!joinValue._1.isPresent()) {
                    if (prevSup < minSup) { // Skip if this edge is already detected as invalid
                        return null;
                    }
                    return joinValue._2;
                }

                // Update the invalid list for the current value.
                // TODO we can store SortedSet or Set as value
                IntSortedSet invalidVertices = new IntAVLTreeSet(joinValue._2, 1, joinValue._2.length - 1);
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
                // TODO value in the edgeSup could be replaced with a Tuple2(initSup, invalidVertices)
                int[] outVal = new int[1 + invalidVertices.size()];
                outVal[0] = initSup;
                System.arraycopy(invalidVertices.toIntArray(), 0, outVal, 1, outVal.length - 1);
                return outVal;
            }).filter(kv -> kv._2 != null).cache();  // TODO repartition?

            prevEdgeSup.unpersist();

            // TODO update edgeVertices if necessary using some condition such as the size of update reaches to a specified thresholds
            // TODO calculate the best repartition for each steps of the above algorithm using the information of graph sizes
        }

        long tEnd = System.currentTimeMillis();
        log("count: " + edgeSup.count(), tStart, tEnd);
        sc.close();
    }

    private static void log(String msg, long start, long end) {
        log(msg, (end - start));
    }

    private static void log(String msg, long duration) {
        if (duration == -1)
            System.out.println("KTRUSS " + msg);
        else
            System.out.println("KTRUSS " + msg + ", duration: " + duration + " ms");
    }
}
