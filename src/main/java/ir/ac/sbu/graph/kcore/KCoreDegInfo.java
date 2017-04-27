package ir.ac.sbu.graph.kcore;

import static ir.ac.sbu.graph.utils.Log.log;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class KCoreDegInfo extends KCore {

    public KCoreDegInfo(KCoreConf kCoreConf) {
        super(kCoreConf);
    }

    public JavaPairRDD<Integer, Integer> start(JavaPairRDD<Integer, Integer> edges) {
        JavaPairRDD<Integer, int[]> neighborList = createNeighborList(edges);
        JavaPairRDD<Integer, Integer> degInfo = neighborList.mapValues(v -> v.length).cache();
        long t1, t2;
        final int k = conf.k;
        while (true) {
            t1 = System.currentTimeMillis();
            // Get invalid vertices and partition by the partitioner used to partition neighbors
            JavaPairRDD<Integer, Integer> invalids = degInfo.filter(v -> v._2 < k);

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
                .reduceByKey((a, b) -> a + b)  // aggregate all subtract info per vertex
                .repartition(conf.partitionNum).cache();

            // Update vInfo to remove invalid vertices and subtract the degree of other remaining vertices using the vSubtract map
            degInfo = degInfo
                .leftOuterJoin(vSubtract)
                .filter(t -> t._2._1 >= k)
                .mapValues(t -> t._2.isPresent() ? t._1 - t._2.get() : t._1).filter(v -> v._2 > 0)
                .repartition(conf.partitionNum)
                .cache();
        }
        return degInfo;
    }

    public static void main(String[] args) {
        KCoreConf kCoreConf = new KCoreConf(args, KCoreDegInfo.class.getSimpleName(), int[].class);
        KCoreDegInfo kCore = new KCoreDegInfo(kCoreConf);

        long tload = System.currentTimeMillis();
        JavaPairRDD<Integer, Integer> edges = kCore.loadEdges();
        log("Load edges ", tload, System.currentTimeMillis());

        long t1 = System.currentTimeMillis();
        JavaPairRDD<Integer, Integer> neighbors = kCore.start(edges);

        log("Vertex count: " + neighbors.count(), t1, System.currentTimeMillis());

        kCore.close();
    }
}
