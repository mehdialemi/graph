package ir.ac.sbu.graph.kcore;

import static ir.ac.sbu.graph.utils.Log.log;

import ir.ac.sbu.graph.utils.Log;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class KCoreDegInfo extends KCore {

    public KCoreDegInfo(KCoreConf kCoreConf) {
        super(kCoreConf);
    }

    public JavaPairRDD<Integer, Integer> start(JavaPairRDD<Integer, Integer> edges) {
        long tNeighborList = System.currentTimeMillis();
        JavaPairRDD<Integer, int[]> neighborList = createNeighborList(edges);
        log("Neighbor list created", tNeighborList, System.currentTimeMillis());

        JavaPairRDD<Integer, Integer> degInfo = neighborList.mapValues(v -> v.length);
        long t1, t2;
        final int k = conf.k;
        while (true) {
            t1 = System.currentTimeMillis();
            // Get invalid vertices and partition by the partitioner used to partition neighbors
            JavaPairRDD<Integer, Integer> invalids = degInfo.filter(v -> v._2 < k)
                .repartition(conf.partitionNum)
                .cache();

            long count = invalids.count();
            t2 = System.currentTimeMillis();
            log("K-core, current invalid count: " + count, t1, t2);
            if (count == 0)
                break;

            // Find the amount of value to be subtracted from each vertex degrees.
            JavaPairRDD<Integer, Integer> vSubtract = neighborList.rightOuterJoin(invalids)
                .filter(t -> t._2._1.isPresent())  // filter only invalid vertices to get their neighbors
                .flatMapToPair(t -> {
                    int[] allNeighbors = t._2._1.get();
                    List<Tuple2<Integer, Integer>> out = new ArrayList<>(allNeighbors.length);
                    for (int allNeighbor : allNeighbors) {
                        out.add(new Tuple2<>(allNeighbor, 1));
                    }
                    return out.iterator();
                })
                .reduceByKey((a, b) -> a + b);  // aggregate all subtract info per vertex

            // Update vInfo to remove invalid vertices and subtract the degree of other remaining vertices using the vSubtract map
            degInfo = degInfo.cogroup(vSubtract).flatMapToPair(t -> {
                Iterator<Integer> degIterator = t._2._1.iterator();
                if (!degIterator.hasNext())
                    return Collections.emptyIterator();

                int currentDeg = degIterator.next();
                if (currentDeg < k)
                    return Collections.emptyIterator();

                for (Integer sub : t._2._2) {
                    currentDeg -= sub;
                }

                if (currentDeg <= 0)
                    return Collections.emptyIterator();

                return Collections.singleton(new Tuple2<>(t._1, currentDeg)).iterator();
            }).repartition(conf.partitionNum);
        }

        return degInfo;
    }

    public static void main(String[] args) {
        KCoreConf kCoreConf = new KCoreConf(args, KCoreDegInfo.class.getSimpleName(), int[].class);
        KCoreDegInfo kCore = new KCoreDegInfo(kCoreConf);

        long tload = System.currentTimeMillis();
        JavaPairRDD<Integer, Integer> edges = kCore.loadEdges();
        log("Edges are loaded", tload, System.currentTimeMillis());

        long tStart = System.currentTimeMillis();
        JavaPairRDD<Integer, Integer> neighbors = kCore.start(edges);

        log("KCore vertex count: " + neighbors.count(), tStart, System.currentTimeMillis());

        kCore.close();
    }
}
