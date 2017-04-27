package ir.ac.sbu.graph.kcore;

import ir.ac.sbu.graph.utils.GraphLoader;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Common methods among KCore solutions
 */
public class KCore {

    protected KCoreConf kCoreConf;
    protected JavaSparkContext sc;

    public KCore(KCoreConf kCoreConf) {
        this.kCoreConf = kCoreConf;
        sc = new JavaSparkContext(kCoreConf.sparkConf);
    }

    protected JavaPairRDD<Integer, int[]> createNeighborList() {
        JavaRDD<String> input = sc.textFile(kCoreConf.inputPath);
        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);
        return edges.groupByKey(kCoreConf.getPartitioner()).mapToPair(t -> {
            IntSet set = new IntOpenHashSet();
            for (Integer v : t._2) {
                set.add(v.intValue());
            }
            return new Tuple2<>(t._1, set.toIntArray());
        }).cache();
    }
}
