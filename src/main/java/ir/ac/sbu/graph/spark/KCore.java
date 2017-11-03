package ir.ac.sbu.graph.spark;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Create k-core sub-graph
 */
public class KCore extends NeighborList {
    public static final int[] EMPTY_ARRAY = new int[]{};

    private final NeighborList neighborList;
    private KConf kConf;

    public KCore(NeighborList neighborList, KConf kConf) {
        super(neighborList);
        this.neighborList = neighborList;
        this.kConf = kConf;
    }

    @Override
    public JavaPairRDD<Integer, int[]> create() {

        JavaPairRDD<Integer, int[]> neighbors = neighborList.create();
        if (kConf.getMaxIter() < 1) {
            return neighbors;
        }

        final int k = kConf.getK();
        for (int iter = 0 ; iter < kConf.getMaxIter(); iter ++ ) {
            long t1 = System.currentTimeMillis();
            JavaPairRDD<Integer, Integer> update = neighbors
                    .filter(nl -> nl._2.length < k && nl._2.length > 0)
                    .flatMapToPair(nl -> {
                        List<Tuple2<Integer, Integer>> out = new ArrayList<>(nl._2.length);
                        for (int v : nl._2) {
                            out.add(new Tuple2<>(v, nl._1));
                        }
                        return out.iterator();
                    }).repartition(conf.getPartitionNum());

            long count = update.count();
            long t2 = System.currentTimeMillis();
            log("K-core, current update count: " + count, t1, t2);
            if (count == 0)
                break;

            neighbors = neighbors.cogroup(update).mapValues(value -> {
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

        return neighbors.filter(t -> t._2.length != 0);
    }

    public static void main(String[] args) {
        Logger.getLogger("org.apache.spar").setLevel(Level.INFO);

        long t1 = System.currentTimeMillis();
        KConf kConf = new KConf(new ArgumentReader(args), "KCore");
        EdgeLoader edgeLoader = new EdgeLoader(kConf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        KCore kCore = new KCore(neighborList, kConf);
        JavaPairRDD<Integer, int[]> kCoreSubGraph = kCore.create();

        log("KCore vertex count: " + kCoreSubGraph.count(), t1, System.currentTimeMillis());

        kCore.close();
    }
}
