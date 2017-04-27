package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Base class for all KTruss solutions
 */
public class KTruss {

    protected KTrussConf conf;
    protected final JavaSparkContext sc;
    protected final Partitioner partitioner;
    protected final Partitioner partitioner2;
    protected JavaPairRDD<Integer, int[]> fonl;

    public KTruss(KTrussConf conf) {
        this.conf = conf;
        sc = new JavaSparkContext(this.conf.sparkConf);
        partitioner = new HashPartitioner(conf.partitionNum);
        partitioner2 = new HashPartitioner(conf.partitionNum * 3);
    }

    public void close() {
        sc.close();
    }

    public JavaPairRDD<Integer, Integer> loadEdges() {
        JavaRDD<String> input = sc.textFile(conf.inputPath, conf.partitionNum);
        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);
        return edges.cache();
    }

    protected JavaPairRDD<Tuple2<Integer, Integer>, IntSet> triangleVertices(JavaPairRDD<Integer, Integer> edges) {
        JavaPairRDD<Integer, int[]> candidates = createCandidates(edges);
        // Generate kv such that key is an edge and value is its triangle vertices.
        return candidates.cogroup(fonl).flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;
            List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>();
            for (int[] cVal : t._2._1) {
                int u = cVal[0];
                Tuple2<Integer, Integer> uv;
                if (u < v)
                    uv = new Tuple2<>(u, v);
                else
                    uv = new Tuple2<>(v, u);

                // The intersection determines triangles which u and v are two of their vertices.
                // Always generate and edge (u, v) such that u < v.
                int fi = 1;
                int ci = 1;
                while (fi < fVal.length && ci < cVal.length) {
                    if (fVal[fi] < cVal[ci])
                        fi ++;
                    else if (fVal[fi] > cVal[ci])
                        ci ++;
                    else {
                        int w = fVal[fi];
                        output.add(new Tuple2<>(uv, w));
                        if (u < w)
                            output.add(new Tuple2<>(new Tuple2<>(u, w), v));
                        else
                            output.add(new Tuple2<>(new Tuple2<>(w, u), v));

                        if (v < w)
                            output.add(new Tuple2<>(new Tuple2<>(v, w), u));
                        else
                            output.add(new Tuple2<>(new Tuple2<>(w, v), u));
                        fi ++;
                        ci ++;
                    }
                }
            }

            return output.iterator();
        }).groupByKey(partitioner2)
            .mapValues(values -> {
                IntSet set = new IntOpenHashSet();
                for (int v : values) {
                    set.add(v);
                }
                return set;
            }).persist(StorageLevel.MEMORY_ONLY()); // Use disk too if graph is very large
    }


    protected JavaPairRDD<Integer, int[]> createCandidates(JavaPairRDD<Integer, Integer> edges) {
        fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partitioner);
        return FonlDegTC.generateCandidatesInteger(fonl).partitionBy(partitioner2);
    }
}
