package ir.ac.sbu.graph.spark.kcore;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Create k-core sub-graph
 */
public class KCore extends NeighborList {

    private KCoreConf kConf;

    public KCore(NeighborList neighborList, KCoreConf kConf) throws URISyntaxException {
        super(neighborList);
        String master = conf.getSc().master();
        this.conf.getSc().setCheckpointDir("/tmp/checkpoint");
        this.kConf = kConf;
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
        long invalidCount = 0;

        long firstDuration = 0;
        long firstInvalids = 0;
        long allDurations = 0;
        int iterations = 0;
        for (int iter = 0; iter < kConf.getKcMaxIter(); iter++) {
            long t1 = System.currentTimeMillis();
            if ((iter + 1) % 50 == 0)
                neighbors.checkpoint();

            JavaPairRDD <Integer, Iterable <Integer>> invUpdate = neighbors.filter(nl -> nl._2.length < k)
                    .flatMapToPair(nl -> {
                        List <Tuple2 <Integer, Integer>> out = new ArrayList <>(nl._2.length);

                        for (int v : nl._2) {
                            out.add(new Tuple2 <>(v, nl._1));
                        }
                        return out.iterator();
                    }).groupByKey(conf.getPartitionNum());

            long count = invUpdate.count();
            if (count == 0) {
                break;
            }
            long t2 = System.currentTimeMillis();
            long duration = t2 - t1;
            log("K-core, updates: " + count, duration);

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
        }

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
