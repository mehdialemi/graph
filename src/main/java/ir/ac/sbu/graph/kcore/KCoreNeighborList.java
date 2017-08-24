package ir.ac.sbu.graph.kcore;

import static ir.ac.sbu.graph.utils.Log.log;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class KCoreNeighborList extends KCore {
    public static final int[] EMPTY_ARRAY = new int[]{};

    public KCoreNeighborList(KCoreConf kCoreConf) {
        super(kCoreConf);
    }

    public JavaPairRDD<Integer, int[]> start(JavaPairRDD<Integer, Integer> edges) {
        long tNeighborList = System.currentTimeMillis();
        JavaPairRDD<Integer, int[]> neighborList = createNeighborList(edges);
        log("Neighbor list created", tNeighborList, System.currentTimeMillis());

        long t1, t2;
        final int k = conf.k;
        final Partitioner partitioner = this.partitioner;
        while (true) {
            t1 = System.currentTimeMillis();
            JavaPairRDD<Integer, Integer> update = neighborList.filter(nl -> nl._2.length < k && nl._2.length > 0)
                .flatMapToPair(nl -> {
                    List<Tuple2<Integer, Integer>> out = new ArrayList<>(nl._2.length);
                    for (int v : nl._2) {
                        out.add(new Tuple2<>(v, nl._1));
                    }
                    return out.iterator();
                }).partitionBy(partitioner);

            long count = update.count();
            t2 = System.currentTimeMillis();
            log("K-core, current update count: " + count, t1, t2);
            if (count == 0)
                break;

            neighborList = neighborList.cogroup(update).mapValues(value -> {
                int[] allNeighbors = value._1().iterator().next();
                if (allNeighbors.length < k) {
                    return EMPTY_ARRAY;
                }
                Iterator<Integer> updateIterator = value._2.iterator();
                if (!updateIterator.hasNext())
                    return allNeighbors;

                IntSet set = new IntOpenHashSet(allNeighbors);
                do {
                    int v = updateIterator.next();
                    set.remove(v);
                } while (updateIterator.hasNext());

                return set.toIntArray();
            }).cache();
        }

        return neighborList.filter(t -> t._2.length != 0);
    }

    public static void main(String[] args) {
        KCoreConf kCoreConf = new KCoreConf(args, KCoreNeighborList.class.getSimpleName(), int[].class);
        KCoreNeighborList kCore = new KCoreNeighborList(kCoreConf);

        long tload = System.currentTimeMillis();
        JavaPairRDD<Integer, Integer> edges = kCore.loadEdges();
        log("Edges are loaded", tload, System.currentTimeMillis());

        long t1 = System.currentTimeMillis();
        JavaPairRDD<Integer, int[]> neighbors = kCore.start(edges);

        log("KCore vertex count: " + neighbors.count(), t1, System.currentTimeMillis());

        kCore.close();
    }
}
