package ir.ac.sbu.graph.spark.partitioning;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.*;

public class MergePartitioning {


    public static JavaPairRDD<Integer, Integer> simpleMerge(JavaSparkContext sc, JavaPairRDD<Integer, Integer> partitions, int partitionCapacity) {
        List<Tuple2<Integer, Integer>> partitionList = partitions
                .mapToPair(kv -> new Tuple2<>(kv._2, 1))
                .reduceByKey((a, b) -> a + b).collect();

        ArrayList<Tuple2<Integer, Integer>> pList = new ArrayList<>(partitionList);
        Collections.sort(pList, (a, b) -> a._1 - b._1);

        final Map<Integer, Integer> pMap = new HashMap<>();
        int pNum = 0;
        int currentBatch = 0;
        for (int i = 0; i < pList.size(); i++) {
            final long currentPSize = pList.get(i)._2;
            currentBatch += currentPSize;
            if (currentBatch > partitionCapacity) {
                pNum++;
                pMap.put(pList.get(i)._1, pNum);
                currentBatch = 0;
                continue;
            }
            pMap.put(pList.get(i)._1, pNum);
        }

        final Broadcast<Map<Integer, Integer>> broadcast = sc.broadcast(pMap);

        return partitions.mapPartitionsToPair(partition -> {
            Map<Integer, Integer> map = broadcast.getValue();
            List<Tuple2<Integer, Integer>> out = new ArrayList<>();
            while (partition.hasNext()) {
                final Tuple2<Integer, Integer> next = partition.next();
                final int newPartitionNumber = map.get(next._2);
                out.add(new Tuple2<>(next._1, newPartitionNumber));
            }
            return out.iterator();
        }).cache();
    }

}
