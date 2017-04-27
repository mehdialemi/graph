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

    protected KCoreConf conf;
    protected JavaSparkContext sc;

    public KCore(KCoreConf conf) {
        this.conf = conf;
        sc = new JavaSparkContext(conf.sparkConf);
    }

    public void close() {
        sc.close();
    }

    public JavaPairRDD<Integer, Integer> loadEdges() {
        JavaRDD<String> input = sc.textFile(conf.inputPath, conf.partitionNum);
        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);
        return edges.cache();
    }

    protected JavaPairRDD<Integer, int[]> createNeighborList(JavaPairRDD<Integer, Integer> edges) {
        return edges.groupByKey(conf.partitionNum).mapToPair(t -> {
            IntSet set = new IntOpenHashSet();
            for (Integer v : t._2) {
                set.add(v.intValue());
            }
            return new Tuple2<>(t._1, set.toIntArray());
        }).repartition(conf.partitionNum).cache();
    }
}
