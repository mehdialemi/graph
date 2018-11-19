package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static ir.ac.sbu.graph.utils.Log.log;

public class KTrussSparkTriangleSet extends KTruss {

    public KTrussSparkTriangleSet(KTrussConf conf) {
        super(conf);
    }

    public JavaPairRDD<Tuple2<Integer, Integer>, IntSet> start(JavaPairRDD<Integer, Integer> edges) {

        // Get triangle vertex set for each edge
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> tvSets = createTriangleVertexSet(edges);

        final int minSup = conf.k - 2;

        int iteration = 0;
        boolean stop = false;

        while (!stop) {
//            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> prevTvSets = tvSets;
            iteration++;
            long t1 = System.currentTimeMillis();

            // Detect invalid edges by comparing the support of triangle vertex set
            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> invalids = tvSets.filter(kv -> kv._2.size() < minSup).cache();
            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> prevInvalids = invalids;
            long invalidEdgeCount = invalids.count();

            // If no invalid edge is found then the program terminates
            if (invalidEdgeCount == 0) {
                break;
            }
            long t2 = System.currentTimeMillis();
            String msg = "iteration: " + iteration + ", invalid edge count: " + invalidEdgeCount;
            log(msg, t2 - t1);

            // The edges in the key part of invalids key-values should be removed. So, we detect other
            // edges of their involved triangle from their triangle vertex set. Here, we determine the
            // vertices which should be removed from the triangle vertex set related to the other edges.
            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> invUpdates = invalids.flatMapToPair(kv -> {
                IntSet original = kv._2;
                List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>(original.size() * 2);
                Tuple2<Integer, Integer> iEdge = kv._1;

                for (Integer w : kv._2) {
                    if (w < iEdge._1)
                        out.add(new Tuple2<>(new Tuple2<>(w, iEdge._1), iEdge._2));
                    else
                        out.add(new Tuple2<>(new Tuple2<>(iEdge._1, w), iEdge._2));

                    if (w < iEdge._2)
                        out.add(new Tuple2<>(new Tuple2<>(w, iEdge._2), iEdge._1));
                    else
                        out.add(new Tuple2<>(new Tuple2<>(iEdge._2, w), iEdge._1));
                }
                return out.iterator();
            }).groupByKey(partitioner2);

            // Remove the invalid vertices from the triangle vertex set of each remaining (valid) edge.
            tvSets = tvSets.filter(kv -> kv._2.size() >= minSup).leftOuterJoin(invUpdates)
                .mapValues(values -> {
                    Optional<Iterable<Integer>> invalidUpdate = values._2;
                    IntSet original = values._1;

                    // If no invalid vertex is present for the current edge then return the original value.
                    if (!invalidUpdate.isPresent()) {
                        return original;
                    }

                    // Remove all invalid vertices from the original triangle vertex set.
                    for (Integer v : invalidUpdate.get()) {
                        original.remove(v.intValue());
                    }

                    // When the triangle vertex set has no other element then the current edge should also
                    // be eliminated from the current tvSets.
                    if (original.size() == 0)
                        return null;

                    return original;
                }).filter(kv -> kv._2 != null)
                .persist(StorageLevel.MEMORY_ONLY());
//            prevTvSets.unpersist();
//            prevInvalids.unpersist();
        }
        return tvSets;
    }

    public static void main(String[] args) {
        KTrussConf conf = new KTrussConf(args, KTrussSparkTriangleSet.class.getSimpleName(),
            GraphUtils.VertexDegreeInt.class, int[].class, List.class, IntSet.class, IntOpenHashSet.class);

        KTrussSparkTriangleSet kTruss = new KTrussSparkTriangleSet(conf);

        JavaPairRDD<Integer, Integer> edges = kTruss.loadEdges();

        long start = System.currentTimeMillis();
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> tVertices = kTruss.start(edges);
        long edgeCount = tVertices.count();
        long duration = System.currentTimeMillis() - start;
        kTruss.close();
        log("KTruss Edge Count: " + edgeCount, duration);
    }
}
