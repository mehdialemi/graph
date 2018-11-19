package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.*;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.kcore.KCoreConf;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.TSetValue;
import ir.ac.sbu.graph.types.VertexByte;
import it.unimi.dsi.fastutil.ints.*;
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
public class MaxKTrussTSetSimple extends SparkApp {
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;
    private static final int CHECKPOINT_ITERATION = 50;
    private static final int UPDATED = 1;
    private static final int NO_UPDATE = 0;

    private final NeighborList neighborList;
    private AtomicInteger iterations = new AtomicInteger(0);
    private final KCoreConf kCoreConf;

    private MaxKTrussTSetSimple(NeighborList neighborList, SparkAppConf conf) throws URISyntaxException {
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

    private JavaPairRDD <Edge, Integer> explore() {

        KCore kCore = new KCore(neighborList, kCoreConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD <Integer, int[]> fonl = triangle.getOrCreateFonl();

        JavaPairRDD <Integer, int[]> candidates = triangle.createCandidates(fonl);

        JavaPairRDD <Edge, TSetValue> tSet = createTSet(fonl, candidates);
        int partitionNum = tSet.getNumPartitions();

        long count = tSet.count();

        log("edge count: " + count);

        return generate(tSet, partitionNum);
    }


    public JavaPairRDD <Edge, Integer> generate(JavaPairRDD <Edge, TSetValue> tSet, int partitionNum) {

        int iter = 0;
        while (true) {
            iter++;
            long t1 = System.currentTimeMillis();
            iterations.incrementAndGet();

            if (iterations.get() % CHECKPOINT_ITERATION == 0) {
                long t = System.currentTimeMillis();
                tSet.checkpoint();

                log("check pointing tSet", t, System.currentTimeMillis());
            }

            JavaPairRDD <Edge, Iterable <int[]>> claims = tSet.filter(kv -> kv._2.hasUpdate)
                    .flatMapToPair(kv -> {
                        TSetValue tSetValue = kv._2;
//                        if (tSetValue.support >= ma)
//                            return Collections.emptyIterator();

                        Edge e = kv._1;
                        List <Tuple2 <Edge, int[]>> out = new ArrayList <>();

                        int i = 0;
                        for (; i < tSetValue.wLast; i++) {
                            int[] ev1 = new int[]{e.v1, tSetValue.support};
                            int[] ev2 = new int[]{e.v2, tSetValue.support};
                            out.add(new Tuple2 <>(new Edge(e.v1, tSetValue.vertex[i]), ev2));
                            out.add(new Tuple2 <>(new Edge(e.v2, tSetValue.vertex[i]), ev1));
                        }

                        for (; i < tSetValue.vLast; i++) {
                            int[] ev1 = new int[]{e.v1, tSetValue.support};
                            int[] ev2 = new int[]{e.v2, tSetValue.support};
                            out.add(new Tuple2 <>(new Edge(e.v1, tSetValue.vertex[i]), ev2));
                            out.add(new Tuple2 <>(new Edge(tSetValue.vertex[i], e.v2), ev1));
                        }

                        for (; i < tSetValue.uLast; i++) {
                            int[] ev1 = new int[]{e.v1, tSetValue.support};
                            int[] ev2 = new int[]{e.v2, tSetValue.support};
                            out.add(new Tuple2 <>(new Edge(tSetValue.vertex[i], e.v1), ev2));
                            out.add(new Tuple2 <>(new Edge(tSetValue.vertex[i], e.v2), ev1));
                        }

                        return out.iterator();
                    }).groupByKey(partitionNum).cache();

            long claimCount = claims.count();

            // If no invalid edge is found then the program terminates
            if (claimCount == 0) {
                break;
            }

            long t2 = System.currentTimeMillis();
            String msg = "iteration: " + iter + ", claim count: " + claimCount;
            log(msg, t2 - t1);

            // Remove the invalid vertices from the triangle vertex set of each remaining (valid) edge.
            tSet = tSet.leftOuterJoin(claims)
                    .mapValues(values -> {
                        Optional <Iterable <int[]>> vertexSupports = values._2;
                        if (!vertexSupports.isPresent()) {
                            values._1.hasUpdate = false;
                            return values._1;
                        }

                        TSetValue tSetValue = values._1;

//                        Int2IntMap map = new Int2IntOpenHashMap();
//                        for (int[] vertexSup : vertexSupports.get()) {
//                            int support = vertexSup[1];
//                            if (support >= tSetValue.support)
//                                continue;
//                            int vertex = vertexSup[0];
//                            int sup = map.getOrDefault(vertex, Integer.MAX_VALUE);
//                            if (support <= sup)
//                                map.put(vertex, support);
//                        }

                        final Int2ObjectSortedMap <IntSet> sortedMap = new Int2ObjectAVLTreeMap <>();
                        for (int[] vertexSup : vertexSupports.get()) {
                            int support = vertexSup[1];
                            if (support >= tSetValue.support)
                                continue;
                            int vertex = vertexSup[0];
                            IntSet set = sortedMap.get(support);
                            if (set == null) {
                                set = new IntOpenHashSet();
                                sortedMap.put(support, set);
                            }
                            set.add(vertex);
                        }

                        if (sortedMap.size() == 0) {
                            tSetValue.hasUpdate = false;
                            return tSetValue;
                        }

//                        sortedMap.firstIntKey()
                        int maxLowerSup = sortedMap.size() == 1 ? 0 : sortedMap.lastIntKey();
//                        sortedMap.get(maxLowerSup).size();
                        tSetValue.hasUpdate = false;
                        int sup = tSetValue.support;
                        IntSet removeSet = new IntOpenHashSet();
                        IntSet set = new IntOpenHashSet(tSetValue.vertex);
                        int lastSup = 0;
//                        int iteration = 0;
                        for (Map.Entry <Integer, IntSet> entry : sortedMap.entrySet()) {

                            int removeSize = entry.getValue().size();
//
                            Integer otherSup = entry.getKey();
                            if ((sup - otherSup) < maxLowerSup) {
                                sup = otherSup;
                                break;
                            }

                            for (Integer vertex : entry.getValue()) {
                                set.remove(vertex);
                            }

                            removeSet.addAll(entry.getValue());
                            if (set.size() == 0) {
                                sup = otherSup;
                                break;
                            }
                            sup = set.size();
                        }

                        if(set.size() == 0) {
                            tSetValue.support = sup;
                            tSetValue.wLast = 0;
                            tSetValue.vLast = 0;
                            tSetValue.uLast = 0;
                            tSetValue.vertex = new int[] {};
                            return tSetValue;
                        }

                        if (tSetValue.support == sup) {
                            return tSetValue;
                        }

                        tSetValue.support = sup;
                        tSetValue.hasUpdate = true;

                        int offset = 0;
                        for (int i = 0; i < tSetValue.wLast; i++) {
                            if (removeSet.contains(tSetValue.vertex[i]))
                                continue;
                            tSetValue.vertex[offset++] = tSetValue.vertex[i];
                        }
                        int wLast = offset;
                        for (int i = tSetValue.wLast; i < tSetValue.vLast; i++) {
                            if (removeSet.contains(tSetValue.vertex[i]))
                                continue;
                            tSetValue.vertex[offset++] = tSetValue.vertex[i];
                        }
                        int vLast = offset;
                        for (int i = tSetValue.vLast; i < tSetValue.uLast; i++) {
                            if (removeSet.contains(tSetValue.vertex[i]))
                                continue;
                            tSetValue.vertex[offset++] = tSetValue.vertex[i];
                        }
                        int uLast = offset;
                        tSetValue.wLast = wLast;
                        tSetValue.vLast = vLast;
                        tSetValue.uLast = uLast;
                        int[] vertex = new int[uLast];
                        System.arraycopy(tSetValue.vertex, 0, vertex, 0, vertex.length);
                        tSetValue.vertex = vertex;
//                        tSetValue.support = vertex.length;

                        return tSetValue;
                    }).persist(StorageLevel.MEMORY_ONLY());
            printKCount(tSet.mapValues(v -> v.support));
        }

        return tSet.mapValues(v -> v.support).cache();
    }

    private JavaPairRDD <Edge, TSetValue> createTSet(JavaPairRDD <Integer, int[]> fonl,
                                                     JavaPairRDD <Integer, int[]> candidates) {
        int partitionNum = fonl.getNumPartitions();
        // Generate kv such that key is an edge and value is its triangle vertices.

        return candidates.cogroup(fonl)
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
                    int wLen = 0, vLen = 0, uLen = 0;
                    for (VertexByte value : values) {
                        list.add(value);
                        if (value.sign == W_UVW)
                            wLen++;
                        else if (value.sign == V_UVW)
                            vLen++;
                        else
                            uLen++;
                    }

                    TSetValue tSetValue = new TSetValue();
                    final int support = list.size();
                    tSetValue.support = support;
                    tSetValue.hasUpdate = true;
                    tSetValue.wLast = wLen;
                    tSetValue.vLast = wLen + vLen;
                    tSetValue.uLast = tSetValue.vLast + uLen;
                    tSetValue.vertex = new int[support];
                    int offsetW = 0;
                    int offsetV = tSetValue.wLast;
                    int offsetU = tSetValue.vLast;
                    for (VertexByte vb : list) {
                        if (vb.sign == W_UVW) {
                            tSetValue.vertex[offsetW++] = vb.vertex;
                        } else if (vb.sign == V_UVW) {
                            tSetValue.vertex[offsetV++] = vb.vertex;
                        } else {
                            tSetValue.vertex[offsetU++] = vb.vertex;
                        }
                    }

                    return tSetValue;
                }).persist(StorageLevel.DISK_ONLY());
    }

    public static void main(String[] args) throws URISyntaxException {
        long t1 = System.currentTimeMillis();

        ArgumentReader argumentReader = new ArgumentReader(args);
        SparkAppConf conf = new SparkAppConf(argumentReader) {
            @Override
            protected String createAppName() {
                return "MaxKTrussTSet-" + super.createAppName();
            }
        };
        conf.init();

        EdgeLoader edgeLoader = new EdgeLoader(conf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        MaxKTrussTSetSimple kTrussTSet = new MaxKTrussTSetSimple(neighborList, conf);
        JavaPairRDD <Edge, Integer> eTrussMap = kTrussTSet.explore();
        long t2 = System.currentTimeMillis();
        long count = eTrussMap.count();
        log("KTruss edge count: " + count, t1, t2);

        printKCount(eTrussMap, 1000);

        kTrussTSet.close();
    }

    private static void printKCount(JavaPairRDD <Edge, Integer> eTrussMap) {
        printKCount(eTrussMap, 5);
    }

    private static void printKCount(JavaPairRDD <Edge, Integer> eTrussMap, int max) {
        Map <Integer, Long> map = eTrussMap.map(kv -> kv._2).countByValue();
        SortedMap <Integer, Long> sortedMap = new TreeMap <>(map);
        for (Map.Entry <Integer, Long> entry : sortedMap.entrySet()) {
            log("K: " + entry.getKey() + ", Count: " + entry.getValue());
            if (entry.getKey() > max)
                return;
        }
    }
}

