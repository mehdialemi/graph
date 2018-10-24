package ir.ac.sbu.graph.spark.partitioning;

import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PartitioningCost extends SparkApp {

    private NeighborList neighborList;
    private final JavaPairRDD<Integer, Integer> vertexPartition;

    public PartitioningCost(NeighborList neighborList, JavaPairRDD<Integer, Integer> vertexPartition) {
        super(neighborList);
        this.neighborList = neighborList;
        this.vertexPartition = vertexPartition;
    }

    public long getNumCut() {
        JavaPairRDD<Integer, int[]> neighbors = neighborList.getOrCreate();

        JavaPairRDD<Integer, Tuple2<Integer, int[]>> neighborListPartition = neighbors
                .join(vertexPartition)
                .mapValues(v -> new Tuple2<>(v._2, v._1));

        JavaPairRDD<Integer, Integer> adjacentPartitions = neighborListPartition.flatMapToPair(kv -> {
            List<Tuple2<Integer, Integer>> out = new ArrayList<>();
            int vertex = kv._1;
            int partition = kv._2._1;
            int[] adjacentVertices = kv._2._2;
            for (int adjacentVertex : adjacentVertices) {
                if (adjacentVertex > vertex)
                    out.add(new Tuple2<>(adjacentVertex, partition));
            }
            return out.iterator();
        });

        final JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> join = vertexPartition
                .leftOuterJoin(adjacentPartitions).cache();

        long cost = join.filter(kv -> {
            if (!kv._2._2.isPresent())
                return false;
            if (kv._2._1.intValue() == kv._2._2.get().intValue())
                return false;
            return true;
        }).count();

        return cost / 2;
    }

}
