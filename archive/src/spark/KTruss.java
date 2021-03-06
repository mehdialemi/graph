package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.Log;
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
        sc = new JavaSparkContext(conf.sparkConf);
        partitioner = new HashPartitioner(conf.partitionNum);
        partitioner2 = new HashPartitioner(conf.partitionNum * 3);
        Log.setName(conf.name);
    }

    public JavaSparkContext getSc() {
        return sc;
    }

    public void close() {
        sc.close();
    }

    protected JavaPairRDD<Tuple2<Integer, Integer>, IntSet> createTriangleVertexSet(JavaPairRDD<Integer, Integer> edges) {
        JavaPairRDD<Integer, int[]> candidates = createCandidates(edges);

        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> tvSet = candidates.cogroup(fonl).flatMapToPair(t -> {
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

                // The intersection determines triangles which u and vertex are two of their vertices.
                // Always generate and edge (u, vertex) such that u < vertex.
                int fi = 1;
                int ci = 1;
                while (fi < fVal.length && ci < cVal.length) {
                    if (fVal[fi] < cVal[ci])
                        fi++;
                    else if (fVal[fi] > cVal[ci])
                        ci++;
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
                        fi++;
                        ci++;
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
            }).persist(StorageLevel.MEMORY_AND_DISK()); // Use disk too if graph is very large

        candidates.unpersist();
        return tvSet;
    }

    protected JavaPairRDD<Integer, int[]> createCandidates(JavaPairRDD<Integer, Integer> edges) {
        fonl = IntGraphUtils.createFonl(edges, partitioner, partitioner2);
        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl)
                .partitionBy(partitioner2).persist(StorageLevel.MEMORY_AND_DISK());
        return candidates;
    }
}
