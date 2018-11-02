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
public class MaxKTrussTSetSubSet extends SparkApp {
    private static final int INVALID = -1;
    private static final int META_LEN = 4;
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;
    public static final int CHECKPOINT_ITERATION = 50;

    private final NeighborList neighborList;
    private AtomicInteger iterations = new AtomicInteger(0);
    private final KCoreConf kCoreConf;

    public MaxKTrussTSetSubSet(NeighborList neighborList, SparkAppConf conf) throws URISyntaxException {
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

    public Map <Integer, Integer> explore() {

        KCore kCore = new KCore(neighborList, kCoreConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD<Integer, int[]> fonl = triangle.getOrCreateFonl();

        JavaPairRDD<Integer, int[]> candidates = triangle.createCandidates(fonl);

        int partitionNum = fonl.getNumPartitions();

        JavaPairRDD<Edge, int[]> tSet = createTSet(fonl, candidates);

        long count = tSet.count();
        log("edge count: " + count);

        JavaPairRDD <Edge, int[]> subTSet = null;
        JavaPairRDD <Edge, Integer> truss = conf.getSc().parallelizePairs(new ArrayList <>());
        JavaPairRDD <Edge, int[]> freezedSubTSet = null;

        boolean checkNextSubset = true;
        int minSup = 0;
        int maxSup = 1;
        while (true) {
            long t1 = System.currentTimeMillis();
            if (checkNextSubset) {
                checkNextSubset = false;

                if (freezedSubTSet != null) {
                    tSet = updateTSet(maxSup, tSet, freezedSubTSet, partitionNum).cache();
                    long tSetCount = tSet.count();
                    log("tSetCount: " + tSetCount);
                    if (tSetCount == 0)
                        break;
                    tSet.checkpoint();
                }

                while (true) {
                    maxSup ++;
                    final int max = maxSup;
                    subTSet = tSet.filter(kv -> kv._2[0] < max).cache();
                    minSup = tSet.filter(kv -> kv._2[0] > 0)
                            .map(kv -> kv._2[0])
                            .reduce((a, b) -> Math.min(a, b)) + 1;
                    long subTSetCount = subTSet.count();
                    log("subTSetCount: " + subTSetCount);
                    if (subTSetCount > 0) {
                        break;
                    }
                }
                log("minSup: " + minSup + ", maxSup: " + maxSup);
                freezedSubTSet = subTSet;
                freezedSubTSet.checkpoint();
            }

            Tuple2 <JavaPairRDD <Edge, Integer>, JavaPairRDD <Edge, int[]>> result = generate(minSup, subTSet, partitionNum);
            JavaPairRDD <Edge, Integer> kTruss = result._1.repartition(partitionNum);
            truss = truss.fullOuterJoin(kTruss)
                    .mapValues(v -> Math.min(v._1.orElse(Integer.MAX_VALUE), v._2.orElse(Integer.MAX_VALUE)))
                    .cache();
            subTSet = result._2.cache();
            long kTrussCount = kTruss.count();
            long t2 = System.currentTimeMillis();
            log("kTrussCount: " + kTrussCount, t1, t2);
            minSup ++;

            if (minSup >= maxSup) {
                checkNextSubset = true;
            }
        }

        return truss.mapToPair(kv -> new Tuple2 <>(kv._2, 1))
                .reduceByKey((a, b) -> a + b)
                .collectAsMap();
    }

    private  JavaPairRDD<Edge, int[]> invalidByKCore( JavaPairRDD<Edge, int[]> tSet, int kc) {
        kCoreConf.setKcMaxIter(Integer.MAX_VALUE);
        kCoreConf.setKc(kc);
        KCore kCore = new KCore(tSet.keys(), kCoreConf);
        JavaPairRDD <Integer, int[]> kCoreNeighbors = kCore.getOrCreate();

        JavaPairRDD <Edge, Integer> kCoreEdges = kCoreNeighbors.flatMapToPair(kv -> {
            List <Tuple2 <Edge, Integer>> edges = new ArrayList <>();
            for (int v : kv._2) {
                edges.add(new Tuple2 <>(new Edge(kv._1, v), 1));
            }
            return edges.iterator();
        }).repartition(kCoreConf.getPartitionNum());

        return tSet.subtractByKey(kCoreEdges);
    }

    public Tuple2<JavaPairRDD<Edge, Integer>, JavaPairRDD <Edge, int[]>> generate(
            int minSup, JavaPairRDD <Edge, int[]> tSet, int partitionNum) {

        JavaPairRDD<Edge, Integer> eTruss = conf.getSc().parallelizePairs(new ArrayList <>());
        Queue<JavaPairRDD<Edge, int[]>> tSetQueue = new LinkedList<>();
        tSetQueue.add(tSet);

        log("Generate KTruss for minSup: " + minSup + ", tSet count: " + tSet.count());

        int iter = 0;
        while (true) {
            iter ++;
            long t1 = System.currentTimeMillis();
            iterations.incrementAndGet();

            if (iterations.get() % CHECKPOINT_ITERATION == 0) {
                long t = System.currentTimeMillis();
                tSet.checkpoint();

                log("check pointing tSet", t, System.currentTimeMillis());
            }
            JavaPairRDD<Edge, int[]> invalids = tSet.filter(kv -> kv._2[0] < minSup).cache();
            long invalidCount = invalids.count();

            // If no invalid edge is found then the program terminates
            if (invalidCount == 0) {
                break;
            }
            eTruss = eTruss.union(invalids.mapValues(v -> minSup - 1).cache());

            if (tSetQueue.size() > 1)
                tSetQueue.remove().unpersist();

            // Remove the invalid vertices from the triangle vertex set of each remaining (valid) edge.
            tSet = updateTSet(minSup, tSet, invalids, partitionNum);
            tSetQueue.add(tSet);
            long tSetCount = tSet.count();

            long t2 = System.currentTimeMillis();
            String msg = "iteration: " + iter +
                    ", invalid edge count: " + invalidCount +
                    ", tSetCount: " + tSetCount;
            log(msg, t2 - t1);

        }

        return new Tuple2 <>(eTruss, tSet);
    }

    private JavaPairRDD<Edge, int[]> updateTSet(final int minSup, JavaPairRDD<Edge, int[]> tSet,
                                                JavaPairRDD<Edge, int[]> invalids, int partitionNum) {
        // The edges in the key part of invalids key-values should be removed. So, we detect other
        // edges of their involved triangle from their triangle vertex set. Here, we determine the
        // vertices which should be removed from the triangle vertex set related to the other edges.
        JavaPairRDD<Edge, Iterable<Integer>> invUpdates = invalids.flatMapToPair(kv -> {
            int i = META_LEN;

            Edge e = kv._1;
            List<Tuple2<Edge, Integer>> out = new ArrayList<>((kv._2.length - 3) * 2);
            for (; i < kv._2[1]; i++) {
                if (kv._2[i] == INVALID)
                    continue;
                out.add(new Tuple2<>(new Edge(e.v1, kv._2[i]), e.v2));
                out.add(new Tuple2<>(new Edge(e.v2, kv._2[i]), e.v1));
            }

            for (; i < kv._2[2]; i++) {
                if (kv._2[i] == INVALID)
                    continue;
                out.add(new Tuple2<>(new Edge(e.v1, kv._2[i]), e.v2));
                out.add(new Tuple2<>(new Edge(kv._2[i], e.v2), e.v1));
            }

            for (; i < kv._2[3]; i++) {
                if (kv._2[i] == INVALID)
                    continue;
                out.add(new Tuple2<>(new Edge(kv._2[i], e.v1), e.v2));
                out.add(new Tuple2<>(new Edge(kv._2[i], e.v2), e.v1));
            }

            return out.iterator();
        }).groupByKey(partitionNum).cache();

        return tSet.filter(kv -> kv._2[0] >= minSup).leftOuterJoin(invUpdates)
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

                    for (int i = META_LEN; i < set.length; i++) {
                        if (set[i] == INVALID)
                            continue;
                        if (iSet.contains(set[i])) {
                            set[0]--;
                            set[i] = INVALID;
                        }
                    }

                    return set;
                }).filter(kv -> kv._2 != null)
                .persist(StorageLevel.MEMORY_ONLY());

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

        SparkAppConf conf = new SparkAppConf(new ArgumentReader(args)) {
            @Override
            protected String createAppName() {
                return "MaxKTrussTSet-" + super.createAppName();
            }
        };
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        MaxKTrussTSetSubSet kTrussTSet = new MaxKTrussTSetSubSet(neighborList, conf);
        Map <Integer, Integer> trussMap = kTrussTSet.explore();
        SortedMap<Integer, Integer> sortedMap = new TreeMap <>(trussMap);
        long t2 = System.currentTimeMillis();

        long trussEdges = 0;
        for (Map.Entry <Integer, Integer> entry : sortedMap.entrySet()) {
            log("K: " + entry.getKey() + ", Count: " + entry.getValue());
            trussEdges += entry.getValue();
        }
        log("trussEdges: " + trussEdges, t1, t2);
        kTrussTSet.close();
    }
}

