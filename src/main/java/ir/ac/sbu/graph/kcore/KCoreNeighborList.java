package ir.ac.sbu.graph.kcore;

import static ir.ac.sbu.graph.utils.Log.log;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class KCoreNeighborList extends KCore {
    public static final int[] EMPTY_ARRAY = new int[]{};

    public KCoreNeighborList(KCoreConf kCoreConf) {
        super(kCoreConf);
    }

    public JavaPairRDD<Integer, int[]> start() {
        long t1, t2;
        JavaPairRDD<Integer, int[]> neighborList = createNeighborList();
        final int k = kCoreConf.k;
        final Partitioner partitioner = kCoreConf.getPartitioner();
        while (true) {
            t1 = System.currentTimeMillis();
            JavaPairRDD<Integer, Integer> update = neighborList.filter(nl -> nl._2.length < k && nl._2.length > 0)
                .flatMapToPair(nl -> {
                    List<Tuple2<Integer, Integer>> out = new ArrayList<>(nl._2.length);
                    for (int v : nl._2) {
                        out.add(new Tuple2<>(v, nl._1));
                    }
                    return out.iterator();
                }).partitionBy(partitioner).cache();

            long count = update.count();
            t2 = System.currentTimeMillis();
            log("K-core, current invalid count: " + count, t1, t2);
            if (count == 0)
                break;

            neighborList = neighborList.cogroup(update).mapValues(value -> {
                int[] allNeighbors = value._1().iterator().next();
                if (allNeighbors.length < k) {
                    return EMPTY_ARRAY;
                }
                IntSet set = new IntOpenHashSet(allNeighbors);
                for (Integer v : value._2) {
                    set.remove(v.intValue());
                }
                return set.toIntArray();
            }).cache();
        }

        return neighborList.filter(t -> t._2.length != 0);
    }

    public static void main(String[] args) {
        KCoreConf kCoreConf = new KCoreConf(args, KCoreNeighborList.class.getSimpleName(), int[].class);
        KCoreNeighborList kCore = new KCoreNeighborList(kCoreConf);
        JavaPairRDD<Integer, int[]> neighbors = kCore.start();
        log("Vertex count: " + neighbors.count());
        kCore.close();
    }
}
