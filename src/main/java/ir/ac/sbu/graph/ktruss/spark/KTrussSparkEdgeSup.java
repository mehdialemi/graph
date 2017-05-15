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

        final int minSup = conf.k - 2;

        // Get triangle vertex set for each edge
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> tvSets = createTriangleVertexSet(edges);

        // Construct edgeSup such that the key is an edge and the value contains two elements.
        // The first element is the original support of edge and the value is the invalid triangle vertices
        // of the current edge. Here tvSets is immutable and edgeSup is updated in each iteration.
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, IntSet>> edgeSup = tvSets
            .mapValues(v -> new Tuple2<>(v.size(), EMPTY_INT_SET)).partitionBy(partitioner2)
            .persist(StorageLevel.MEMORY_ONLY());

        int iteration = 0;
        while (true) {
            long t1 = System.currentTimeMillis();
            log("Iteration: " + ++iteration);

            // Find invalid edges by subtracting the current invalid triangle vertices from the
            // original edge support and compare it with the predefined minSup
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, IntSet>> invalids =
                edgeSup.filter(value ->
                    (value._2._1 - value._2._2.size()) < minSup && (value._2._1 - value._2._2.size()) != 0);

            // TODO broad cast invalid if its count is lower than a specified value
            long invalidEdgeCount = invalids.count();

            long t2 = System.currentTimeMillis();
            long duration = t2 - t1;
            log("Invalid count: " + invalidEdgeCount, duration);

            // If no invalid edge is found then the program terminates
            if (invalidEdgeCount == 0)
                break;

            // If we have invalid edges then their triangles are invalid. To find other edges of the invalid
            // triangle, we should join the current invalid edges with the tvSets. By this condition,
            // we are able to determine invalid vertices which should be removed from the triangle
            // vertex set of the valid edges.
            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> invUpdates =
                tvSets.join(invalids) // TODO find a best partition and test the reverse join
                    .flatMapToPair(kv -> {

                        IntSet original = kv._2._1;
                        Tuple2<Integer, IntSet> invalid = kv._2._2;
                        IntIterator iterator = invalid._2.iterator();

                        while (iterator.hasNext()) {
                            original.remove(iterator.nextInt());
                        }

                        List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>(original.size() * 2);

                        int u = kv._1._1;
                        int v = kv._1._2;

                        // Determine the other edges of the current invalid triangle.
                        // Generate the invalid vertices which should be eliminated from the triangle vertex set of the
                        // target edge
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
                    }).groupByKey(partitioner2); // TODO repartition?

            // Join invalid update with edgeSup to update the current edgeSup.
            // By this join the list in the value part would be updated with those invalid vertices for the current edge
            edgeSup = edgeSup.leftOuterJoin(invUpdates) // TODO find a best partition
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
                }).filter(kv -> kv._2 != null).partitionBy(partitioner2).cache();  // TODO repartition?

            // TODO calculate the best repartition for each steps of the above algorithm using the information of graph sizes


        }
        return edgeSup.count();
    }

    public static void main(String[] args) {
        KTrussConf conf = new KTrussConf(args, KTrussSparkEdgeSup.class.getSimpleName(),
            GraphUtils.VertexDegree.class, long[].class, List.class);

        KTrussSparkEdgeSup kTruss = new KTrussSparkEdgeSup(conf);

        JavaPairRDD<Integer, Integer> edges = kTruss.loadEdges();

        long start = System.currentTimeMillis();
        long count = kTruss.start(edges);
        log("KTruss edge count:" + count, start, System.currentTimeMillis());

        kTruss.close();
    }
}
