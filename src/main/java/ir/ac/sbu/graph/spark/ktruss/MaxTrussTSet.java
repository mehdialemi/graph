package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.VertexByte;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Generate ktruss in 2 phase:
 * 1: Create triangle set using {@link Triangle}
 * 2: Iteratively, prune invalid edges which have not enough support
 */
public class MaxTrussTSet extends SparkApp {
    private static final int INVALID = -1;
    private static final int META_LEN = 4;
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;
    public static final int CHECKPOINT_ITERATION = 50;

    private int partitionNum;
    private final NeighborList neighborList;
    private KTrussConf ktConf;
    private AtomicInteger iterations = new AtomicInteger(0);

    public MaxTrussTSet(NeighborList neighborList, KTrussConf ktConf) throws URISyntaxException {
        super(neighborList);
        this.neighborList = neighborList;
        this.ktConf = ktConf;
        String master = ktConf.getSc().master();
        this.ktConf.getSc().setCheckpointDir("/tmp/checkpoint");
        if (master.contains("local")) {
            return;
        }
        String masterHost = new URI(ktConf.getSc().master()).getHost();
        this.ktConf.getSc().setCheckpointDir("hdfs://" + masterHost + "/shared/checkpoint");
    }

    public Map<Integer, JavaRDD<Edge>> explore() {
        KCore kCore = new KCore(neighborList, ktConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD<Integer, int[]> fonl = triangle.getOrCreateFonl();

        JavaPairRDD<Integer, int[]> candidates = triangle.createCandidates(fonl);

        partitionNum = fonl.getNumPartitions();

        JavaPairRDD<Edge, int[]> tSet = createTSet(fonl, candidates);

        tSet.checkpoint();
        while (true) {

        }
    }

    private void maxTruss(JavaPairRDD<Edge, int[]> tSet) {
        JavaPairRDD <Edge, Integer> eSup = tSet.mapValues(v -> v[0]).cache();

        JavaPairRDD <Edge, Tuple2 <Integer, Integer>> extend = extend(tSet).repartition(partitionNum);

        JavaPairRDD <Edge, Iterable <Tuple2 <Integer, Integer>>> joinResult = eSup.join(extend)
                .filter(kv -> kv._2._1 > kv._2._2._2)
                .mapValues(v -> v._2)
                .groupByKey();
    }

    private JavaPairRDD <Edge, Tuple2 <Integer, Integer>> extend(JavaPairRDD<Edge, int[]> tSet) {
        return tSet.flatMapToPair(kv -> {
            int i = META_LEN;
            int sup = kv._2[0];

            Edge e = kv._1;
            List<Tuple2<Edge, Tuple2<Integer, Integer>>> out = new ArrayList<>((kv._2.length - 3) * 2);
            for (; i < kv._2[1]; i++) {
                if (kv._2[i] == INVALID)
                    continue;
                out.add(new Tuple2<>(new Edge(e.v1, kv._2[i]), new Tuple2<>(e.v2, sup)));
                out.add(new Tuple2<>(new Edge(e.v2, kv._2[i]), new Tuple2<>(e.v1, sup)));
            }

            for (; i < kv._2[2]; i++) {
                if (kv._2[i] == INVALID)
                    continue;
                out.add(new Tuple2<>(new Edge(e.v1, kv._2[i]), new Tuple2<>(e.v2, sup)));
                out.add(new Tuple2<>(new Edge(kv._2[i], e.v2), new Tuple2<>(e.v1, sup)));
            }

            for (; i < kv._2[3]; i++) {
                if (kv._2[i] == INVALID)
                    continue;
                out.add(new Tuple2<>(new Edge(kv._2[i], e.v1), new Tuple2<>(e.v2, sup)));
                out.add(new Tuple2<>(new Edge(kv._2[i], e.v2), new Tuple2<>(e.v1, sup)));
            }

            return out.iterator();
        });
    }

    private JavaPairRDD<Edge, int[]> createTSet(JavaPairRDD<Integer, int[]> fonl,
                                                JavaPairRDD<Integer, int[]> candidates) {
        int partitionNum = fonl.getNumPartitions();
        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD<Edge, int[]> tSet = candidates.cogroup(fonl)
                .flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;

            List<Tuple2<Edge, VertexByte>> output = new ArrayList<>();
            for (int[] cVal : t._2._1) {
                int u = cVal[0];
                Edge uv = new Edge(u, v);

                // The intersection determines triangles which u and v are two of their vertices.
                // Always generate and edge (u, v) such that u < v.
                int fi = 1;
                int ci = 1;
                while (fi < fVal.length && ci < cVal.length) {
                    if (fVal[fi] < cVal[ci])
                        fi++;
                    else if (fVal[fi] > cVal[ci])
                        ci++;
                    else {
                        int w = fVal[fi];
                        Edge uw = new Edge(u, w);
                        Edge vw = new Edge(v, w);

                        output.add(new Tuple2<>(uv, new VertexByte(w, W_UVW)));
                        output.add(new Tuple2<>(uw, new VertexByte(v, V_UVW)));
                        output.add(new Tuple2<>(vw, new VertexByte(u, U_UVW)));

                        fi++;
                        ci++;
                    }
                }
            }

            return output.iterator();
        }).groupByKey(partitionNum * 2)
                .mapValues(values -> {
                    List<VertexByte> list = new ArrayList<>();
                    int sw = 0, sv = 0, su = 0;
                    for (VertexByte value : values) {
                        list.add(value);
                        if (value.b == W_UVW)
                            sw++;
                        else if (value.b  == V_UVW)
                            sv++;
                        else
                            su++;
                    }

                    int offsetW = META_LEN;
                    int offsetV = sw + META_LEN;
                    int offsetU = sw + sv + META_LEN;
                    int[] set = new int[META_LEN + list.size()];
                    set[0] = list.size();  // support of edge
                    set[1] = offsetW + sw;  // exclusive max offset of w
                    set[2] = offsetV + sv;  // exclusive max offset of v
                    set[3] = offsetU + su;  // exclusive max offset of u

                    for (VertexByte vb : list) {
                        if (vb.b == W_UVW)
                            set[offsetW++] = vb.v;
                        else if (vb.b == V_UVW)
                            set[offsetV++] = vb.v;
                        else
                            set[offsetU++] = vb.v;
                    }

                    return set;
                }).persist(StorageLevel.DISK_ONLY()); // Use disk too because this RDD often is very large

        return tSet;
    }

    public static void main(String[] args) throws URISyntaxException {
        long t1 = System.currentTimeMillis();

        KTrussConf ktConf = new KTrussConf(new ArgumentReader(args));
        ktConf.init();

        EdgeLoader edgeLoader = new EdgeLoader(ktConf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        MaxTrussTSet kTrussTSet = new MaxTrussTSet(neighborList, ktConf);
        Map <Integer, JavaRDD <Edge>> eTrussMap = kTrussTSet.explore();
        log("KTruss edge count: " + eTrussMap.size(), t1, System.currentTimeMillis());

        for (Map.Entry <Integer, JavaRDD <Edge>> entry : eTrussMap.entrySet()) {
            log("K: " + entry.getKey() + ", Count: " + entry.getValue().count());
        }
        kTrussTSet.close();
    }
}

