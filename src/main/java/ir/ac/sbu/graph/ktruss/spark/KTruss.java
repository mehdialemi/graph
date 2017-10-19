package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.IntGraphUtils;
import ir.ac.sbu.graph.utils.Log;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Base class for all KTruss solutions
 */
public class KTruss {

    protected KTrussConf conf;
    protected final JavaSparkContext sc;
    protected final Partitioner mediumPartitioner;
    protected final Partitioner biggerPartitioner;
    protected JavaPairRDD<Integer, int[]> fonl;

    public KTruss(KTrussConf conf) {
        this.conf = conf;
        sc = new JavaSparkContext(conf.sparkConf);
        mediumPartitioner = new HashPartitioner(conf.partitionNum);
        biggerPartitioner = new HashPartitioner(conf.partitionNum * 3);
        Log.setName(conf.name);
    }

    public JavaSparkContext getSc() {
        return sc;
    }

    public void close() {
        sc.close();
    }

    protected JavaPairRDD<Integer, int[]> createCandidates(JavaPairRDD<Integer, Integer> edges) {

        fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, mediumPartitioner, biggerPartitioner);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl)
                .partitionBy(biggerPartitioner).persist(StorageLevel.MEMORY_AND_DISK());

        return candidates;
    }
}
