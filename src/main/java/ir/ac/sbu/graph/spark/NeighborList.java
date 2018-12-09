package ir.ac.sbu.graph.spark;

import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import static ir.ac.sbu.graph.utils.Log.log;

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

    public NeighborList(SparkAppConf conf, JavaRDD<Edge> rdd) {
        super(conf);
        JavaPairRDD<Integer, Integer> edges = rdd.mapToPair(t -> new Tuple2 <>(t.v1, t.v2));
        neighbors = createNeighbors(edges);
    }

    public JavaPairRDD<Integer, int[]> getOrCreate() {
        if (neighbors == null) {
            JavaPairRDD<Integer, Integer> edges = edgeLoader.create();
//            log("edge count: " + edges.count());
            neighbors = createNeighbors(edges);
        }
        return neighbors;
    }

    private JavaPairRDD <Integer, int[]> createNeighbors(JavaPairRDD <Integer, Integer> edges) {
        return edges.groupByKey(conf.getPartitionNum()).mapToPair(t -> {
            IntSet set = new IntOpenHashSet();
            for (Integer v : t._2) {
                set.add(v.intValue());
            }
            return new Tuple2<>(t._1, set.toIntArray());
        }).cache();
    }

    public long numEdges() {
        return getOrCreate()
                .map(kv -> kv._2.length)
                .reduce((a, b) -> a + b) / 2;
    }
}
