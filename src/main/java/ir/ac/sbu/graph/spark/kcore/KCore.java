package ir.ac.sbu.graph.spark.kcore;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Create k-core sub-graph
 */
public class KCore extends NeighborList {

    private KCoreConf kConf;
    private Queue <JavaPairRDD <Integer, int[]>> neighborQueue;

    public KCore(NeighborList neighborList, KCoreConf kConf) throws URISyntaxException {
        super(neighborList);
        String master = conf.getSc().master();
        this.conf.getSc().setCheckpointDir("/tmp/checkpoint");
        this.kConf = kConf;
        neighborQueue = new LinkedList <>();
        if (master.contains("local")) {
            return;
        }
        String masterHost = new URI(conf.getSc().master()).getHost();
        this.conf.getSc().setCheckpointDir("hdfs://" + masterHost + "/shared/checkpoint");

    }

    public JavaPairRDD <Integer, int[]> getK(JavaPairRDD <Integer, int[]> neighbors, int k) {
        return perform(neighbors, k);
    }

    @Override
    public JavaPairRDD <Integer, int[]> getOrCreate() {
        return perform(super.getOrCreate(), kConf.getKc());
    }

    public JavaPairRDD <Integer, int[]> perform(JavaPairRDD <Integer, int[]> neighbors, int k) {
        log("kcore iteration: " + kConf.getKcMaxIter() );

        long t1 = System.currentTimeMillis();
        neighborQueue.add(neighbors);
        long invalidCount = 0;
//        long vCount = neighbors.count();
        long neighborDuration = System.currentTimeMillis() - t1;
//        log("vCount: " + vCount, neighborDuration);

        if (kConf.getKcMaxIter() < 1) {
            return neighbors;
        }

        long firstDuration = 0;
        long firstInvalids = 0;
        long allDurations = 0;
        int iterations = 0;
//        log("vertex count: " + vCount);
        for (int iter = 0; iter < kConf.getKcMaxIter(); iter++) {
            t1 = System.currentTimeMillis();
            if ((iter + 1) % 50 == 0)
                neighbors.checkpoint();

            JavaPairRDD <Integer, int[]> invalids = neighbors.filter(nl -> nl._2.length < k).cache();
            long count = invalids.count();
            invalidCount += count;
            long t2 = System.currentTimeMillis();
            long duration = t2 - t1;
            if (iter == 0) {
                firstInvalids = count;
            }
            if (iter == 1) {
                firstDuration = duration;
            }
            allDurations += duration;
            log("K-core, invalids: " + count, duration);

            if (count == 0)
                break;

            iterations = iter + 1;
            JavaPairRDD <Integer, Iterable <Integer>> invUpdate = invalids
                    .flatMapToPair(nl -> {
                        List <Tuple2 <Integer, Integer>> out = new ArrayList <>(nl._2.length);

                        for (int v : nl._2) {
                            out.add(new Tuple2 <>(v, nl._1));
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
//
//        NumberFormat nf = new DecimalFormat("##.####");
//        double invRatio = invalidCount / (double) vCount;
//        double kciRatio = firstInvalids / (double) invalidCount;
//        double kctRatio = firstDuration / (double) allDurations;
//        log("K: " + k + "\nnumIterations: " + iterations + "\ninvRatio: " + nf.format(invRatio) +
//                "\nkci: " + nf.format(kciRatio) + "\nkct: " + nf.format(kctRatio) +
//                "\ninvalids: " + invalidCount + "\nvCount: " + vCount +
//                "\nkcore duration: " + allDurations);

        return neighbors;
    }

    public static void main(String[] args) throws URISyntaxException {
        long t1 = System.currentTimeMillis();
        KCoreConf kConf = new KCoreConf(new ArgumentReader(args), true);
        kConf.init();

        EdgeLoader edgeLoader = new EdgeLoader(kConf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        KCore kCore = new KCore(neighborList, kConf);
        JavaPairRDD <Integer, int[]> kCoreSubGraph = kCore.getOrCreate();
        long t2 = System.currentTimeMillis();
        log("KCore vertex count: " + kCoreSubGraph.count(), t1, t2);

        kCore.close();
    }
}
