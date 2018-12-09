package ir.ac.sbu.graph.spark.kcore;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Create k-core sub-graph
 */
public class KCore extends NeighborList {

    private KCoreConf kConf;
    private Queue<JavaPairRDD<Integer, int[]>> neighborQueue;

    public KCore(NeighborList neighborList, KCoreConf kConf) throws URISyntaxException {
        super(neighborList);
        String master = conf.getSc().master();
        this.conf.getSc().setCheckpointDir("/tmp/checkpoint");
        this.kConf = kConf;
        neighborQueue = new LinkedList<>();
        if (master.contains("local")) {
            return;
        }
        String masterHost = new URI(conf.getSc().master()).getHost();
        this.conf.getSc().setCheckpointDir("hdfs://" + masterHost + "/shared/checkpoint");

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

    public JavaPairRDD<Integer, int[]> getK(JavaPairRDD<Integer, int[]> neighbors, int k) {
        return perform(neighbors, k);
    }

    @Override
    public JavaPairRDD<Integer, int[]> getOrCreate() {

        JavaPairRDD<Integer, int[]> neighbors = super.getOrCreate();

        return perform(neighbors, kConf.getKc());
    }

    private JavaPairRDD <Integer, int[]> perform(JavaPairRDD <Integer, int[]> neighbors, int k) {
        if (kConf.getKcMaxIter() < 1) {
            return neighbors;
        }
        long t1 = System.currentTimeMillis();
        neighborQueue.add(neighbors);
        long invalidCount = 0;
        long vCount = neighbors.count();
        long firstDuration = 0;
        long firstInvalids = 0;
        long allDurations = 0;
        int iterations = 0;
        log("vertex count: " + vCount);
        for (int iter = 0; iter < kConf.getKcMaxIter(); iter ++ ) {
            if ((iter + 1)% 50 == 0)
                neighbors.checkpoint();

            JavaPairRDD<Integer, int[]> invalids = neighbors.filter(nl -> nl._2.length < k).cache();
            long count = invalids.count();
            invalidCount += count;
            long t2 = System.currentTimeMillis();
            long duration = t2 - t1;
            if (iter == 0) {
                firstDuration = duration;
                firstInvalids = count;
            }
            allDurations += duration;
            log("K-core, invalids: " + count, duration);

            if (count == 0)
                break;

            iterations = iter + 1;
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
            t1 = System.currentTimeMillis();
        }

        if (neighborQueue.size() > 1)
            neighborQueue.remove().unpersist();

        NumberFormat nf = new DecimalFormat("##.##");
        double invRatio = invalidCount / (double) vCount;
        double kciRatio = firstInvalids / (double) invalidCount;
        double kctRatio = firstDuration / (double) allDurations;
        log("iterations: " + iterations + ", invRatio: " + nf.format(invRatio) +
                ", kci: " + nf.format(kciRatio) + ", kct: " + nf.format(kctRatio));

        return neighbors;
    }

    public static void main(String[] args) throws URISyntaxException {
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
