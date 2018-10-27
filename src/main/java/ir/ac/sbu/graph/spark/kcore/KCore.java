package ir.ac.sbu.graph.spark.kcore;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.*;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Create k-core sub-graph
 */
public class KCore extends NeighborList {

    private KCoreConf kConf;
    private Queue<JavaPairRDD<Integer, int[]>> neighborQueue;

    public KCore(NeighborList neighborList, KCoreConf kConf) {
        super(neighborList);
        this.kConf = kConf;
        neighborQueue = new LinkedList<>();
    }

    public KCore(JavaRDD<Edge> rdd, KCoreConf kConf) {
        super(kConf, rdd);
        this.kConf = kConf;
        neighborQueue = new LinkedList<>();
    }

    public void unpersist() {
        for (JavaPairRDD<Integer, int[]> rdd : neighborQueue) {
            rdd.unpersist();
        }
    }

    @Override
    public JavaPairRDD<Integer, int[]> getOrCreate() {

        JavaPairRDD<Integer, int[]> neighbors = super.getOrCreate();
        if (kConf.getKcMaxIter() < 1) {
            return neighbors;
        }

        neighborQueue.add(neighbors);

        final int k = kConf.getKc();
        for (int iter = 0; iter < kConf.getKcMaxIter(); iter ++ ) {
            long t1 = System.currentTimeMillis();

            JavaPairRDD<Integer, int[]> invalids = neighbors.filter(nl -> nl._2.length < k).cache();
            long count = invalids.count();
            long t2 = System.currentTimeMillis();
            log("K-core, invalids: " + count, t1, t2);

            if (invalids.count() == 0)
                break;

            JavaPairRDD<Integer, Iterable<Integer>> invUpdate = invalids
                    .flatMapToPair(nl -> {
                        List<Tuple2<Integer, Integer>> out = new ArrayList<>(nl._2.length);

                        for (int v : nl._2) {
                            out.add(new Tuple2<>(v, nl._1));
                        }
                        return out.iterator();
                    }).groupByKey(conf.getPartitionNum());

            neighbors = neighbors.filter(nl -> nl._2.length >= k)
                    .leftOuterJoin(invUpdate)
                    .mapValues(value -> {
                        if (!value._2.isPresent())
                            return value._1;

                        IntSet invSet = new IntOpenHashSet();
                        for (int inv : value._2.get()) {
                            invSet.add(inv);
                        }

                        IntList nSet = new IntArrayList();
                        for (int v : value._1) {
                            if (invSet.contains(v))
                                continue;
                            nSet.add(v);
                        }

                        return nSet.toIntArray();
            }).cache();

            if (neighborQueue.size() > 1)
                neighborQueue.remove().unpersist();

            neighborQueue.add(neighbors);
        }

        if (neighborQueue.size() > 1)
            neighborQueue.remove().unpersist();

        return neighbors;
    }

    public static void main(String[] args) {
        long t1 = System.currentTimeMillis();
        KCoreConf kConf = new KCoreConf(new ArgumentReader(args), true);
        kConf.init();

        EdgeLoader edgeLoader = new EdgeLoader(kConf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        KCore kCore = new KCore(neighborList, kConf);
        JavaPairRDD<Integer, int[]> kCoreSubGraph = kCore.getOrCreate();

        log("KCore vertex count: " + kCoreSubGraph.count(), t1, System.currentTimeMillis());

        kCore.close();
    }
}
