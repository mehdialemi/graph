package ir.ac.sbu.graph.spark.kcore;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

import java.net.URISyntaxException;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Create k-core sub-graph
 */
public class MaxCore extends SparkApp {

    private KCore kCore;

    public MaxCore(KCore kCore) {
        super(kCore);
        this.kCore = kCore;
    }

    public void printKStats(NeighborList neighborList, int startK, int step) {
        log("startK: " + startK + ", step: " + step);
        JavaPairRDD <Integer, int[]> neighbors = neighborList.getOrCreate();
        int partitions = neighbors.getNumPartitions() * 5;
        int k = startK;

        while(true) {
            JavaPairRDD <Integer, int[]> neighbors2 = kCore.getK(neighbors, k);

            long count = neighbors2.count();
            if (count == 0) {
                k -= step;
                if(step == 1)
                    break;
                step = 1;
                neighbors = neighbors.repartition(partitions)
                        .persist(StorageLevel.DISK_ONLY());
                log("k: " + k + ", neighbors: " + count + ", step: " + step);
            } else {
                neighbors = neighbors2.repartition(partitions)
                        .persist(StorageLevel.DISK_ONLY());
                log("k: " + k + ", neighbors: " + count);
                k += step;
            }
            neighbors.checkpoint();
        }
    }

    public static void main(String[] args) throws URISyntaxException {
        ArgumentReader argumentReader = new ArgumentReader(args);
        KCoreConf kConf = new KCoreConf(argumentReader, true) {
            @Override
            protected String createAppName() {
                return "KCore-" + super.createAppName();
            }
        };
        kConf.init();
        int step = argumentReader.nextInt(10);
        EdgeLoader edgeLoader = new EdgeLoader(kConf);
        NeighborList neighborList = new NeighborList(edgeLoader);
        KCore kCore = new KCore(neighborList, kConf);

        MaxCore maxCore = new MaxCore(kCore);
        maxCore.printKStats(neighborList, kConf.kc, step);
        kCore.close();
    }
}
