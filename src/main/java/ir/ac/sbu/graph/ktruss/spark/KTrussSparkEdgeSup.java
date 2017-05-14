package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * This class find k-truss sub-graphs from the big graph stored as edge list in the hdfs (Hadoop File System).
 * This program is written by spark framework and would be run on the spark cluster. To extract k-truss sub-graphs,
 * the following tasks is performed:
 * 1- Find triangles and produce key-values as (edge, triangle vertices).
 * 2- Repetitively remove edges which have support lower than the specified value.
 */
public class KTrussSparkEdgeSup extends KTruss {

    public static final IntSet EMPTY_INT_SET = new IntOpenHashSet();

    public KTrussSparkEdgeSup(KTrussConf conf) {
        super(conf);
    }


    public long start(JavaPairRDD<Integer, Integer> edges) {
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> tVertices = triangleVertices(edges);

        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, IntSet>> edgeSup = tVertices
            .mapValues(v -> new Tuple2<>(v.size(), EMPTY_INT_SET)).partitionBy(partitioner)
            .persist(StorageLevel.MEMORY_ONLY());
        final int minSup = conf.k - 2;

        int iteration = 0;
        while (true) {
            long t1 = System.currentTimeMillis();
            log("Iteration: " + ++iteration);

            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, IntSet>> invalids =
                edgeSup.filter(value -> (value._2._1 - value._2._2.size()) < minSup);

            // TODO broad cast invalid if its count is lower than a specified value
            long invalidCount = invalids.count();

            long t2 = System.currentTimeMillis();
            long duration = t2 - t1;
            log("Invalid count: " + invalidCount, duration);

            if (invalidCount == 0)
                break;

            // Join invalids with  tVertices
            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> invalidUpdate =
                tVertices.join(invalids) // TODO find a best partition and test the reverse join
                    .flatMapToPair(kv -> {

                        Tuple2<Integer, IntSet> invalid = kv._2._2;
                        IntIterator iterator = invalid._2.iterator();
                        IntSet original = kv._2._1;
                        while (iterator.hasNext()) {
                            original.remove(iterator.nextInt());
                        }

                        List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>(original.size() * 2);

                        int u = kv._1._1;
                        int v = kv._1._2;

                        // Generate the vertices which should be reported as invalid for the edge of the current triangles.
                        for (Integer w : original) {
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
            edgeSup = edgeSup.leftOuterJoin(invalidUpdate) // TODO find a best partition
                .mapValues(joinValue -> {

                    // Calculate previous support
                    int initSup = joinValue._1._1;
                    IntSet cInvalids = joinValue._1._2;
                    int prevSup = initSup - cInvalids.size();
                    if (!joinValue._2.isPresent()) {
                        if (prevSup < minSup) { // Skip if this edge is already detected as invalid
                            return null;
                        }
                        return joinValue._1;
                    }

                    IntSet set = new IntOpenHashSet(cInvalids);
                    // Update the invalid list for the current value.
                    // TODO we can store SortedSet or Set as value
                    // TODO create this object just once in the partition and reuse it
                    for (int u : joinValue._2.get()) {
                        set.add(u);
                    }

                    // If new support is zero this means that no additional
                    // invalid update would be produced by the current edge
                    int newSup = initSup - set.size();
                    if (newSup == 0)
                        return null;

                    // Generate output value such that the value in index zero is init support and
                    // the remaining is related to maintain invalid vertices of the current edge.
                    return new Tuple2<>(initSup, set);
                }).filter(kv -> kv._2 != null).partitionBy(partitioner).persist(StorageLevel.MEMORY_ONLY());  // TODO repartition?

            // TODO calculate the best repartition for each steps of the above algorithm using the information of graph sizes


        }
        return edgeSup.count();
    }

    public static void main(String[] args) {
        KTrussConf conf = new KTrussConf(args, KTrussSparkTriangleVertices.class.getSimpleName(),
            GraphUtils.VertexDegree.class, long[].class, List.class);

        KTrussSparkEdgeSup kTruss = new KTrussSparkEdgeSup(conf);

        JavaPairRDD<Integer, Integer> edges = kTruss.loadEdges();

        long start = System.currentTimeMillis();
        long count = kTruss.start(edges);
        log("KTruss edge count:" + count, start, System.currentTimeMillis());

        kTruss.close();
    }
}
