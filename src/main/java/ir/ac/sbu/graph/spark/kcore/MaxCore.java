package ir.ac.sbu.graph.spark.kcore;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

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

    public void printKStats(NeighborList neighborList) {

        JavaPairRDD <Integer, int[]> neighbors = neighborList.getOrCreate();
        int partitions = neighbors.getNumPartitions();
        int k = 2;
        while(true) {
            neighbors = kCore.getK(neighbors, k)
                    .repartition(partitions)
                    .persist(StorageLevel.DISK_ONLY());
            long count = neighbors.count();
            log("k: " + k + ", neighbors: " + count);
            if (count == 0)
                break;
            k ++;
        }
    }

    public static void main(String[] args) {
        KCoreConf kConf = new KCoreConf(new ArgumentReader(args), true);
        kConf.init();

        EdgeLoader edgeLoader = new EdgeLoader(kConf);
        NeighborList neighborList = new NeighborList(edgeLoader);
        KCore kCore = new KCore(neighborList, kConf);

        MaxCore maxCore = new MaxCore(kCore);
        maxCore.printKStats(neighborList);
        kCore.close();
    }
}
