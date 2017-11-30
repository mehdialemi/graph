package ir.ac.sbu.graph.spark;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

/**
 * Create and maintain neighbor list such that each item is a key-value which key is vertex id and
 * value is neighbor list
 */
public class NeighborList extends SparkApp {

    private EdgeLoader edgeLoader;
    private JavaPairRDD<Integer, int[]> neighbors;

    public NeighborList(NeighborList neighborList) {
        this(neighborList.edgeLoader);
    }

    public NeighborList(EdgeLoader edgeLoader) {
        super(edgeLoader);
        this.edgeLoader = edgeLoader;
    }

    public JavaPairRDD<Integer, int[]> create() {
        if (neighbors == null) {
            JavaPairRDD<Integer, Integer> edges = edgeLoader.create();
            neighbors = edges.groupByKey(conf.getPartitionNum()).mapToPair(t -> {
                IntSet set = new IntOpenHashSet();
                for (Integer v : t._2) {
                    set.add(v.intValue());
                }
                return new Tuple2<>(t._1, set.toIntArray());
            }).cache();
        }
        return neighbors;
    }
}
