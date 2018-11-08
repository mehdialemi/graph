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
public class MaxKTrussTSetDirect extends SparkApp {
    private static final int META_LEN = 4;
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;
    public static final int CHECKPOINT_ITERATION = 50;
    public static final int SUP_INDEX = 0;
    public static final int U_MAX_INDEX = 3;
    public static final int V_MAX_INDEX = 2;
    public static final int W_MAX_INDEX = 1;
    public static final int PREV_SUP_SIZE = 1;
    public static final int[] SUP_1 = {1, 1};
    public static final int MAX_SUP_FILTER = 100000;

    private final NeighborList neighborList;
    private AtomicInteger iterations = new AtomicInteger(0);
    private final KCoreConf kCoreConf;

    public MaxKTrussTSetDirect(NeighborList neighborList, SparkAppConf conf) throws URISyntaxException {
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

    public JavaPairRDD <Edge, Integer> explore(float consumptionRatio, int minPartitions) {

        KCore kCore = new KCore(neighborList, kCoreConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD <Integer, int[]> fonl = triangle.getOrCreateFonl();

        JavaPairRDD <Integer, int[]> candidates = triangle.createCandidates(fonl);

        int partitionNum = Math.max(minPartitions, fonl.getNumPartitions());

        JavaPairRDD <Edge, int[]> tSet = createTSet(fonl, candidates);

        int k = 4;
        long count = tSet.count();

        log("edge count: " + count);
        return maxTruss(tSet, partitionNum);
    }

    public JavaPairRDD <Edge, Integer> maxTruss(JavaPairRDD <Edge, int[]> tSet, int partitionNum) {

        int updateMaxSupFilter = 2;
        long prevUpdateCandidCount = 1L;
        while (true) {
            long t1 = System.currentTimeMillis();
            iterations.incrementAndGet();
            if (iterations.get() % CHECKPOINT_ITERATION == 0) {
                long t = System.currentTimeMillis();
                tSet.checkpoint();

                log("check pointing tSet", t, System.currentTimeMillis());
            }

            JavaPairRDD <Edge, int[]> updateCandids;
            final int maxSupFilter = updateMaxSupFilter;
            updateCandids = tSet
                    .filter(kv -> kv._2[SUP_INDEX] < maxSupFilter && kv._2[SUP_INDEX] < kv._2[kv._2.length - 1])
                    .cache();

            long updateCandidCount = updateCandids.count();
            if (updateCandidCount == 0) {
                if (prevUpdateCandidCount == 0 && updateMaxSupFilter > MAX_SUP_FILTER)
                    break;

                updateMaxSupFilter = MAX_SUP_FILTER;
            }

            if (updateCandidCount <= prevUpdateCandidCount) {
                updateMaxSupFilter++;
            }
            prevUpdateCandidCount = updateCandidCount;

            long t2 = System.currentTimeMillis();
            log("Iteration: " + iterations.get() + ", minSup: " + updateMaxSupFilter +
                    ", updateCandidCount: " + updateCandidCount, t1, t2);

            JavaPairRDD <Edge, Iterable <int[]>> invUpdates = updateCandids.flatMapToPair(kv -> {
                Edge e = kv._1;
                int currentSup = kv._2[SUP_INDEX];
                List <Tuple2 <Edge, int[]>> out = new ArrayList <>();
                if (currentSup == 0)
                    return Collections.emptyIterator();

                int[] value1, value2;
                if (currentSup == 1) {
                    value1 = new int[]{e.v1};
                    value2 = new int[]{e.v2};
                } else {
                    value1 = new int[]{e.v1, currentSup};
                    value2 = new int[]{e.v2, currentSup};
                }

                int i = META_LEN;
                final int wMaxIndex = kv._2[W_MAX_INDEX];
                for (; i < wMaxIndex; i++) {
                    final int w = kv._2[i];
                    out.add(new Tuple2 <>(new Edge(e.v2, w), value1));
                    out.add(new Tuple2 <>(new Edge(e.v1, w), value2));
                }

                final int vMaxIndex = kv._2[V_MAX_INDEX];
                for (; i < vMaxIndex; i++) {
                    final int v = kv._2[i];
                    out.add(new Tuple2 <>(new Edge(v, e.v2), value1));
                    out.add(new Tuple2 <>(new Edge(e.v1, v), value2));
                }

                final int uMaxIndex = kv._2[U_MAX_INDEX];
                for (; i < uMaxIndex; i++) {
                    final int u = kv._2[i];
                    out.add(new Tuple2 <>(new Edge(u, e.v2), value1));
                    out.add(new Tuple2 <>(new Edge(u, e.v1), value2));
                }

                if ((uMaxIndex + 1) != kv._2.length)
                    throw new RuntimeException("array index doesn't match");

                return out.iterator();
            }).groupByKey(partitionNum);

            // Remove the invalid vertices from the triangle vertex set of each remaining (valid) edge.
            tSet = tSet.leftOuterJoin(invUpdates)
                    .mapValues(values -> {
                        Optional <Iterable <int[]>> invalidUpdate = values._2;
                        int[] set = values._1;

                        final int prevSup = set[SUP_INDEX];
                        if (prevSup == 1) {
                            return SUP_1;
                        }

                        // If no invalid vertex is present for the current edge then return the set value.
                        if (!invalidUpdate.isPresent()) {
                            set[set.length - 1] = prevSup;
                            return set;
                        }

                        TreeMap<Integer, IntSet> sortedMap = new TreeMap <>();
                        for (int[] v : invalidUpdate.get()) {
                            if (v.length > 1 && prevSup < v[1])
                                continue;

                            int neighborSupport = v.length == 1 ? 1 : v[1];

                            IntSet iSet = sortedMap.get(neighborSupport);
                            if (iSet == null) {
                                iSet = new IntOpenHashSet();
                                sortedMap.put(neighborSupport, iSet);
                            }
                            iSet.add(v[0]);
                        }

                        if (sortedMap.size() == 0) {
                            set[set.length - 1] = prevSup;
                            return set;
                        }

                        IntSet iSet = sortedMap.firstEntry().getValue();
                        int firstSup = sortedMap.firstKey();

                        int nextSup = prevSup - iSet.size();
                        if (nextSup <= firstSup) {
                            iSet.clear();
                            nextSup = firstSup;
                        }
//
//                        IntSet iSet = new IntOpenHashSet();
//
//                        int sup;
//                        boolean changeNextSup = false;
//                        for (Map.Entry <Integer, IntSet> entry : sortedMap.entrySet()) {
//                            sup = entry.getKey();
//                            if (nextSup <= sup) {
//                                changeNextSup = true;
//                                break;
//                            }
//                            int size = entry.getValue().size();
//                            if (nextSup - size <= sup) {
//                                changeNextSup = true;
//                                nextSup = sup;
//                                break;
//                            }
//                            nextSup -= size;
//                            iSet.addAll(entry.getValue());
//                        }
//
//                        if (!changeNextSup)
//                            nextSup = prevSup - iSet.size();

                        if(iSet.size() == 0) {
                            set[SUP_INDEX] = nextSup;
                            set[set.length - 1] = prevSup;
                            return set;
                        }

                        int offsetW = META_LEN;
                        int i = offsetW;
                        int wLen = 0;
                        for (; i < set[W_MAX_INDEX]; i++) {
                            if (iSet.contains(set[i]))
                                continue;
                            set[offsetW + wLen] = set[i];
                            wLen++;
                        }
                        set[W_MAX_INDEX] = offsetW + wLen; // exclusive index of w

                        int offsetV = set[W_MAX_INDEX];
                        int vLen = 0;
                        for (; i < set[V_MAX_INDEX]; i++) {
                            if (iSet.contains(set[i]))
                                continue;
                            set[offsetV + vLen] = set[i];
                            vLen++;
                        }
                        set[V_MAX_INDEX] = offsetV + vLen; // exclusive index of vertex

                        int offsetU = set[V_MAX_INDEX];
                        int uLen = 0;
                        for (; i < set[U_MAX_INDEX]; i++) {
                            if (iSet.contains(set[i]))
                                continue;
                            set[offsetU + uLen] = set[i];
                            uLen++;
                        }
                        set[U_MAX_INDEX] = offsetU + uLen; // exclusive index of u
                        int size = wLen + vLen + uLen;
                        set[SUP_INDEX] = nextSup;

                        if (set[SUP_INDEX] == 0) {
                            throw new RuntimeException("not expected zero for support");
                        }

                        int[] value = new int[META_LEN + size + PREV_SUP_SIZE];
                        System.arraycopy(set, 0, value, 0, value.length - 1);
                        value[value.length - 1] = prevSup;

                        return value;
                    }).persist(StorageLevel.MEMORY_ONLY());
        }

        return tSet.mapValues(v -> v[SUP_INDEX]);
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
                }).groupByKey(partitionNum)
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

                    int[] set = new int[META_LEN + list.size() + PREV_SUP_SIZE];
                    ;
                    final int sup = list.size();

                    if (sup < 1)
                        throw new RuntimeException("Initial sup is less than 1");

                    set[SUP_INDEX] = sup;  // support of edge
                    set[W_MAX_INDEX] = offsetW + sw;  // exclusive max offset of w
                    set[V_MAX_INDEX] = offsetV + sv;  // exclusive max offset of vertex
                    set[U_MAX_INDEX] = offsetU + su;  // exclusive max offset of u

                    if (sup == 1) {
                        set[set.length - 1] = 2;
                    } else {
                        set[set.length - 1] = sup;
                    }

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
                return "MaxKTrussTSetDirect-" + super.createAppName();
            }
        };
        conf.init();
        float consumptionRatio = argumentReader.nextInt(50) / 100f;
        int minPartitions = argumentReader.nextInt(10);

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        MaxKTrussTSetDirect kTrussTSet = new MaxKTrussTSetDirect(neighborList, conf);
        JavaPairRDD <Edge, Integer> maxTruss = kTrussTSet.explore(consumptionRatio, minPartitions);
        Map <Integer, Integer> map = maxTruss.mapToPair(kv -> new Tuple2 <>(kv._2, 1))
                .reduceByKey((a, b) -> a + b)
                .collectAsMap();
        log("MaxTruss edge count: " + maxTruss.count(), t1, System.currentTimeMillis());

        SortedMap<Integer, Integer> sortedMap = new TreeMap <>(map);
        for (Map.Entry <Integer, Integer> entry : sortedMap.entrySet()) {
            log("K: " + entry.getKey() + ", Count: " + entry.getValue());
        }
        kTrussTSet.close();
    }
}

