package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

import static ir.ac.sbu.graph.utils.Log.log;

public class KTrussInvalidUpdates extends KTruss {

    public static final IntSet EMPTY_SET = new IntOpenHashSet();
    public static final Tuple2<Boolean, IntSet> INVALID_UPDATE_VALUE = new Tuple2<>(true, EMPTY_SET);
    public static final Integer INV_VERTEX = Integer.MIN_VALUE;

    public KTrussInvalidUpdates(KTrussConf conf) {
        super(conf);
    }

    public long start(JavaPairRDD<Integer, Integer> edges) {

        final int minSup = conf.k - 2;

        // Get triangle vertex set for each edge
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> tvSets = createTriangleVertexSet(edges)
            .persist(StorageLevel.MEMORY_AND_DISK());

        // Find invalid edges which have support less than minSup
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> invalids = tvSets.filter(t -> t._2.size() < minSup)
            .partitionBy(partitioner2).cache();

        // This RDD is going to store all update information regarding invalid vertices of the edges' triangle vertex set.
        // The key specifies an edge and the value is two parts: 1. A boolean to determine if the current edge is invalid. 2: The invalid
        // vertex set of the current edge (key)
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Boolean, IntSet>> tvInvSets =
            sc.parallelize(new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Boolean, IntSet>>>())
                .mapToPair(t -> new Tuple2<>(t._1, t._2));

        int iteration = 0;
        while (true) {
            long invCount = invalids.count();
            if (invCount == 0)
                break;

            log("iteration: " + ++iteration + ", invalids: " + invCount);

            // The edges in the key part of invalids key-values should be removed. So, we detect other
            // edges of their involved triangle from their triangle vertex set. Here, we determine the
            // vertices which should be removed from the triangle vertex set related to the other edges.
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> invUpdates = invalids.flatMapToPair(t -> {
                List<Tuple2<Tuple2<Integer, Integer>, Integer>> list = new ArrayList<>();
                list.add(new Tuple2<>(t._1, INV_VERTEX));
                int u = t._1._1;
                int v = t._1._2;

                IntIterator iter = t._2.iterator();
                while (iter.hasNext()) {
                    int w = iter.nextInt();
                    if (u < w)
                        list.add(new Tuple2<>(new Tuple2<>(u, w), v));
                    else
                        list.add(new Tuple2<>(new Tuple2<>(w, u), v));

                    if (v < w)
                        list.add(new Tuple2<>(new Tuple2<>(v, w), u));
                    else
                        list.add(new Tuple2<>(new Tuple2<>(w, v), u));
                }
                return list.iterator();
            }).partitionBy(partitioner2).cache();

            // Update tvInvSets
            tvInvSets = tvInvSets.cogroup(invUpdates).mapValues(t -> {

                Iterator<Tuple2<Boolean, IntSet>> prevInvValue = t._1.iterator();
                Tuple2<Boolean, IntSet> value;  // The value to store the current invalid update status
                IntSet tvInvSet;  // The final tvInvSet of invalid vertices

                // If the current edge has a previously added invalid triangle vertices
                if (prevInvValue.hasNext()) {
                    value = prevInvValue.next();

                    if (value._1)  // When the current edge is invalid, we should return immediately.
                        return value;

                    tvInvSet = value._2;  // Get the previous invalid vertex tvInvSet
                } else {  // The first invalid triangle vertex report for the current edge. So, initialize the value part.
                    tvInvSet = new IntOpenHashSet();
                    value = new Tuple2<>(false, tvInvSet);
                }

                // Iterate over all invalid vertices and add them to the tvInvSet which is in the return value.
                for (Integer v : t._2) {
                    // The INV_VERTEX determine if the current edge was determined invalid in the previous iteration. If so, construct
                    // an invalid value for the current edge.
                    if (INV_VERTEX.equals(v))
                        return INVALID_UPDATE_VALUE;
                    tvInvSet.add(v.intValue());
                }

                return value;
            }).partitionBy(partitioner2).cache();  // Partition by the same partitioner used in tvSets

            // Find invalid edges using all invalid update information and the original triangle vertex sets.
            invalids = tvSets.join(tvInvSets).flatMapToPair(t -> {
                Tuple2<Boolean, IntSet> tvInvSet = t._2._2;
                IntSet tvSet = t._2._1;

                // When the current edge is invalid we have nothing to do with it.
                if (tvInvSet._1)
                    return Collections.emptyIterator();

                // Remove all invalid vertices from the current triangle vertex set
                tvSet.removeAll(tvInvSet._2);

                if (tvSet.size() < minSup) {
                    return Arrays.asList(new Tuple2<>(t._1, tvSet)).iterator();
                }

                return Collections.emptyIterator();
            }).partitionBy(partitioner2).cache();
        }

        return tvSets.subtractByKey(tvInvSets.filter(t -> t._2._1)).count();
    }

    public static void main(String[] args) {
        KTrussConf conf = new KTrussConf(args, KTrussInvalidUpdates.class.getSimpleName(),
            GraphUtils.VertexDegree.class, long[].class, List.class);
        KTrussInvalidUpdates kTruss = new KTrussInvalidUpdates(conf);

        long tload = System.currentTimeMillis();
        JavaPairRDD<Integer, Integer> edges = kTruss.loadEdges();
        log("Load edges ", tload, System.currentTimeMillis());

        long start = System.currentTimeMillis();
        long edgeCount = kTruss.start(edges);
        long duration = System.currentTimeMillis() - start;

        kTruss.close();

        log("KTruss Edge Count: " + edgeCount, duration);
    }
}
