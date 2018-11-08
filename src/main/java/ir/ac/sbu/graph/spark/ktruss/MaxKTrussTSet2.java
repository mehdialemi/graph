package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.*;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.kcore.KCoreConf;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.VertexByte;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
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
public class MaxKTrussTSet2 extends SparkApp {
    private static final int INVALID = -1;
    private static final int META_LEN = 4;
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;
    public static final int CHECKPOINT_ITERATION = 50;

    private final NeighborList neighborList;
    private AtomicInteger iterations = new AtomicInteger(0);
    private final KCoreConf kCoreConf;

    public MaxKTrussTSet2(NeighborList neighborList, SparkAppConf conf) throws URISyntaxException {
        super(neighborList);
        this.neighborList = neighborList;
        String master = conf.getSc().master();
        this.conf.getSc().setCheckpointDir("/tmp/checkpoint");
        kCoreConf = new KCoreConf(conf, 2, 1000);
        if (master.contains("local")) {
            return;
        }
        String masterHost = new URI(conf.getSc().master()).getHost();
        this.conf.getSc().setCheckpointDir("hdfs://" + masterHost + "/shared/checkpoint");

    }

    public Map <Integer, JavaRDD <Edge>> explore() {

        KCore kCore = new KCore(neighborList, kCoreConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD <Integer, int[]> fonl = triangle.getOrCreateFonl();

        JavaPairRDD <Integer, int[]> candidates = triangle.createCandidates(fonl);

        JavaPairRDD <Edge, int[]> tSet = createTSet(fonl, candidates);
        int partitionNum = tSet.getNumPartitions();

        long count = tSet.count();
        log("edge count: " + count);

        int k = 4;
        long totalInvalids = 0;
        Map<Integer, JavaRDD<Edge>> maxTruss = new HashMap <>();

        while (totalInvalids < count) {
            long t1 = System.currentTimeMillis();
            final int minSup = k - 2;
            final int maxSupport = minSup - 1;
            long minSupInvalids = 0;

            int iter = 0;
            while (true) {
                iter++;
                long t1Iter = System.currentTimeMillis();
                iterations.incrementAndGet();

                if (iterations.get() % CHECKPOINT_ITERATION == 0) {
                    long t = System.currentTimeMillis();
                    tSet.checkpoint();
                    log("check pointing tSet", t, System.currentTimeMillis());
                }

                JavaPairRDD <Edge, int[]> invalids = tSet.filter(kv -> kv._2[0] < minSup).cache();
                long invalidCount = invalids.count();

                // If no invalid edge is found then the program terminates
                if (invalidCount == 0) {
                    break;
                }

                JavaRDD <Edge> edges = maxTruss.get(maxSupport);
                if (edges == null) {
                    edges = conf.getSc().parallelize(new ArrayList <>());
                    maxTruss.put(maxSupport, edges);
                }
                edges.union(invalids.keys().cache());

                totalInvalids += invalidCount;
                minSupInvalids += invalidCount;

                long t2Iter = System.currentTimeMillis();
                String msg = "iteration: " + iter + ", invalid edge count: " + invalidCount;
                log(msg, t2Iter - t1Iter);

                // The edges in the key part of invalids key-values should be removed. So, we detect other
                // edges of their involved triangle from their triangle vertex set. Here, we determine the
                // vertices which should be removed from the triangle vertex set related to the other edges.
                JavaPairRDD <Edge, Iterable <Integer>> invUpdates = invalids.flatMapToPair(kv -> {
                    int i = META_LEN;

                    Edge e = kv._1;
                    List <Tuple2 <Edge, Integer>> out = new ArrayList <>();
                    for (; i < kv._2[1]; i++) {
                        if (kv._2[i] == INVALID)
                            continue;
                        out.add(new Tuple2 <>(new Edge(e.v1, kv._2[i]), e.v2));
                        out.add(new Tuple2 <>(new Edge(e.v2, kv._2[i]), e.v1));
                    }

                    for (; i < kv._2[2]; i++) {
                        if (kv._2[i] == INVALID)
                            continue;
                        out.add(new Tuple2 <>(new Edge(e.v1, kv._2[i]), e.v2));
                        out.add(new Tuple2 <>(new Edge(kv._2[i], e.v2), e.v1));
                    }

                    for (; i < kv._2[3]; i++) {
                        if (kv._2[i] == INVALID)
                            continue;
                        out.add(new Tuple2 <>(new Edge(kv._2[i], e.v1), e.v2));
                        out.add(new Tuple2 <>(new Edge(kv._2[i], e.v2), e.v1));
                    }

                    return out.iterator();
                }).groupByKey(partitionNum).cache();

                // Remove the invalid vertices from the triangle vertex set of each remaining (valid) edge.
                tSet = tSet.filter(kv -> kv._2[0] >= minSup).leftOuterJoin(invUpdates)
                        .mapValues(values -> {
                            Optional <Iterable <Integer>> invalidUpdate = values._2;
                            int[] set = values._1;

                            // If no invalid vertex is present for the current edge then return the set value.
                            if (!invalidUpdate.isPresent()) {
                                return set;
                            }

                            IntSet iSet = new IntOpenHashSet();
                            for (int v : invalidUpdate.get()) {
                                iSet.add(v);
                            }

                            if (iSet.size() == 0)
                                return set;

                            int i = META_LEN;
                            int offsetW = META_LEN;
                            int wLen = 0;
                            for (; i < set[1]; i++) {
                                if (set[i] == INVALID || iSet.contains(set[i]))
                                    continue;
                                set[offsetW + wLen] = set[i];
                                wLen++;
                            }
                            set[1] = offsetW + wLen; // exclusive index of w

                            int offsetV = set[1];
                            int vLen = 0;
                            for (; i < set[2]; i++) {
                                if (set[i] == INVALID || iSet.contains(set[i]))
                                    continue;
                                set[offsetV + vLen] = set[i];
                                vLen++;
                            }
                            set[2] = offsetV + vLen; // exclusive index of vertex

                            int offsetU = set[2];
                            int uLen = 0;
                            for (; i < set[3]; i++) {
                                if (set[i] == INVALID || iSet.contains(set[i]))
                                    continue;
                                set[offsetU + uLen] = set[i];
                                uLen++;
                            }
                            set[3] = offsetU + uLen; // exclusive index of u

                            set[0] = wLen + vLen + uLen;

                            int[] value = new int[META_LEN + set[0]];
                            System.arraycopy(set, 0, value, 0, value.length);
                            return value;
                        }).persist(StorageLevel.MEMORY_ONLY());
//                tSetQueue.add(tSet);
            }

            long t2 = System.currentTimeMillis();
            log("Support: " + maxSupport + ", MinSupInvalids: " + minSupInvalids +
                    ", Total Invalids: " + totalInvalids, t1, t2);
            k++;
        }

        return maxTruss;
    }

    private JavaPairRDD <Edge, int[]> createTSet(JavaPairRDD <Integer, int[]> fonl,
                                                 JavaPairRDD <Integer, int[]> candidates) {
        int partitionNum = fonl.getNumPartitions();
        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD <Edge, int[]> tSet = candidates.cogroup(fonl)
                .flatMapToPair(t -> {
                    int[] fVal = t._2._2.iterator().next();
                    Arrays.sort(fVal, 1, fVal.length);
                    int v = t._1;

                    List <Tuple2 <Edge, VertexByte>> output = new ArrayList <>();
                    for (int[] cVal : t._2._1) {
                        int u = cVal[0];
                        Edge uv = new Edge(u, v);

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
                                Edge uw = new Edge(u, w);
                                Edge vw = new Edge(v, w);

                                output.add(new Tuple2 <>(uv, new VertexByte(w, W_UVW)));
                                output.add(new Tuple2 <>(uw, new VertexByte(v, V_UVW)));
                                output.add(new Tuple2 <>(vw, new VertexByte(u, U_UVW)));

                                fi++;
                                ci++;
                            }
                        }
                    }

                    return output.iterator();
                }).groupByKey(partitionNum * 2)
                .mapValues(values -> {
                    List <VertexByte> list = new ArrayList <>();
                    int sw = 0, sv = 0, su = 0;
                    for (VertexByte value : values) {
                        list.add(value);
                        if (value.sign == W_UVW)
                            sw++;
                        else if (value.sign == V_UVW)
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
                    set[2] = offsetV + sv;  // exclusive max offset of vertex
                    set[3] = offsetU + su;  // exclusive max offset of u

                    for (VertexByte vb : list) {
                        if (vb.sign == W_UVW)
                            set[offsetW++] = vb.vertex;
                        else if (vb.sign == V_UVW)
                            set[offsetV++] = vb.vertex;
                        else
                            set[offsetU++] = vb.vertex;
                    }

                    return set;
                }).persist(StorageLevel.DISK_ONLY()); // Use disk too because this RDD often is very large

        return tSet;
    }

    public static void main(String[] args) throws URISyntaxException {
        long t1 = System.currentTimeMillis();

        ArgumentReader argumentReader = new ArgumentReader(args);
        SparkAppConf conf = new SparkAppConf(argumentReader) {
            @Override
            protected String createAppName() {
                return "MaxKTrussTSet2-" + super.createAppName();
            }
        };
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        MaxKTrussTSet2 kTrussTSet = new MaxKTrussTSet2(neighborList, conf);
        Map <Integer, JavaRDD <Edge>> eTrussMap = kTrussTSet.explore();
        log("KTruss edge count: " + eTrussMap.size(), t1, System.currentTimeMillis());

        for (Map.Entry <Integer, JavaRDD <Edge>> entry : eTrussMap.entrySet()) {
            log("K: " + entry.getKey() + ", Count: " + entry.getValue().count());
        }

        kTrussTSet.close();
    }
}

