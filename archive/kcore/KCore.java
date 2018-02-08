package ir.ac.sbu.graph.kcore;

import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.Log;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.HashPartitioner;
import org.apache.spark.JobExecutionStatus;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Common methods among KCore solutions
 */
public class KCore {

    protected final KCoreConf conf;
    protected final JavaSparkContext sc;
    protected final Partitioner partitioner;

    public KCore(KCoreConf conf) {
        this.conf = conf;
        sc = new JavaSparkContext(conf.sparkConf);
        Log.setName(conf.name);
        partitioner = new HashPartitioner(conf.partitionNum);
    }

    public JavaSparkContext getSc() {
        return sc;
    }

    public void close() {
        sc.close();
    }

}
