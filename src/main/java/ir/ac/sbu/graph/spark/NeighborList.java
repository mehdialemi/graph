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
    protected JavaPairRDD<Integer, int[]> neighbors;
    private long edgeCount;
    private long vertexCount;

    public NeighborList(NeighborList neighborList) {
        this(neighborList.edgeLoader);
    }

    public NeighborList(EdgeLoader edgeLoader) {
        super(edgeLoader);
        this.edgeLoader = edgeLoader;
    }

    public JavaPairRDD<Integer, int[]> getOrCreate() {
        if (neighbors == null) {
            JavaPairRDD <Integer, Integer> edges = edgeLoader.create();
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

    public long getEdgeCount() {
        return edgeCount;
    }

    public void setEdgeCount(long edgeCount) {
        this.edgeCount = edgeCount;
    }

    public long getVertexCount() {
        return vertexCount;
    }

    public void setVertexCount(long vertexCount) {
        this.vertexCount = vertexCount;
    }
}
