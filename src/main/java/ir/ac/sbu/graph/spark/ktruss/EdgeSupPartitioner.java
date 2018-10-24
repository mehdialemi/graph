package ir.ac.sbu.graph.spark.ktruss;

import org.apache.spark.Partitioner;
import scala.Tuple2;

import java.util.Map;

public class EdgeSupPartitioner extends Partitioner {

    private final int numPartitions;
    private final Map<Integer, Tuple2<Integer, Integer>> map ;

    public EdgeSupPartitioner(int numPartitions, Map<Integer, Tuple2<Integer, Integer>> map) {
        this.numPartitions = numPartitions;
        this.map = map;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object o) {
        EdgeSup edgeSup = (EdgeSup) o;
        Tuple2 <Integer, Integer> partitionRange = map.get(edgeSup.sup);
        Integer lower = partitionRange._1;
        Integer upper = partitionRange._2;
        return edgeSup.hashCode() % lower + upper;
    }
}
