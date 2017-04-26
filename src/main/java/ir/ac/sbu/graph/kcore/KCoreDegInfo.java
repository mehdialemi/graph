package ir.ac.sbu.graph.kcore;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class KCoreDegInfo extends KCore {

    private JavaPairRDD<Integer, Integer> degInfo;

    public KCoreDegInfo(JavaPairRDD<Integer, int[]> neighborList, int k, Partitioner partitioner) {
        super(neighborList, k, partitioner);
    }

    public void start() {
        degInfo = neighborList.mapValues(v -> v.length).cache();
        long t1, t2;
        while (true) {
            t1 = System.currentTimeMillis();
            // Get invalid vertices and partition by the partitioner used to partition neighbors
            JavaPairRDD<Integer, Integer> invalids = degInfo.filter(v -> v._2 < k).partitionBy(partitioner);

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
                    .partitionBy(partitioner);


            // Update vInfo to remove invalid vertices and subtract the degree of other remaining vertices using the vSubtract map
            degInfo = degInfo
                    .leftOuterJoin(vSubtract)
                    .filter(t -> t._2._1 >= k)
                    .mapValues(t -> t._2.isPresent() ? t._1 - t._2.get() : t._1).filter(v -> v._2 > 0)
                    .cache();
        }


    }

    @Override
    public JavaPairRDD<Integer, int[]> getNeighborList() {
        return neighborList.leftOuterJoin(degInfo)
                .filter(t -> t._2._2.isPresent())
                .mapValues(t -> t._1).cache();
    }

    @Override
    public JavaPairRDD<Integer, Integer> getDegInfo() {
        return degInfo;
    }
}
