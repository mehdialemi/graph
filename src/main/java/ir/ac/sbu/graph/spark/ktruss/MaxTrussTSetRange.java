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

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Generate ktruss in 2 phase:
 * 1: Create triangle set using {@link Triangle}
 * 2: Iteratively, prune invalid edges which have not enough support
 */
public class MaxTrussTSetRange extends SparkApp {
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;

    private final NeighborList neighborList;
    private final KCoreConf kCoreConf;

    public MaxTrussTSetRange(NeighborList neighborList, SparkAppConf conf) throws URISyntaxException {
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

    public JavaPairRDD <Edge, Integer> explore() {

        KCore kCore = new KCore(neighborList, kCoreConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD <Integer, int[]> fonl = triangle.getOrCreateFonl();

        JavaPairRDD <Integer, int[]> candidates = triangle.createCandidates(fonl);

        JavaPairRDD <Edge, MaxTSetValue> tSet = createTSet(fonl, candidates);

        JavaPairRDD <Edge, MaxTSetValue> rdd = find(tSet, 3);

        return rdd.mapValues(v -> v.sup).reduceByKey(Math::max);
    }


    private JavaPairRDD <Edge, MaxTSetValue> find(JavaPairRDD <Edge, MaxTSetValue> tSet, int maxSup) {

        int numPartitions = tSet.getNumPartitions();
        int iteration = 0;

        JavaPairRDD <Edge, Integer> truss = conf.getSc().parallelizePairs(new ArrayList <>());

        while (true) {
            iteration++;
            long t1 = System.currentTimeMillis();
//            JavaPairRDD <Edge, Integer> finalized = tSet.filter(kv -> kv._2.vSize == 0)
//                    .mapValues(v -> v.sup)
//                    .persist(StorageLevel.DISK_ONLY());
//            truss = truss.union(finalized);

            JavaPairRDD <Edge, MaxTSetValue> filtered = tSet.filter(kv ->
                    kv._2.updated && kv._2.vSize > 0 && kv._2.sup < maxSup);

            JavaPairRDD <Edge, Iterable <int[]>> update = filtered.flatMapToPair(kv -> {
                Edge e = kv._1;
                List <Tuple2 <Edge, int[]>> out = new ArrayList <>();
                List<MaxTSetValue> values = new ArrayList <>();
                MaxTSetValue value = kv._2;
                values.add(value);
                if (value.lowTSetValues != null) {
                    for (MaxTSetValue lowTSetValue : value.lowTSetValues) {
                        if (lowTSetValue.updated) {
                            values.add(lowTSetValue);
                        }
                    }
                }

                for (MaxTSetValue maxTSetValue : values) {
                    int[] sv2 = new int[]{maxTSetValue.sup, e.v2};
                    int[] sv1 = new int[]{maxTSetValue.sup, e.v1};
                    for (int w : value.w) {
                        out.add(new Tuple2 <>(new Edge(e.v1, w), sv2));
                        out.add(new Tuple2 <>(new Edge(e.v2, w), sv1));
                    }
                    for (int v : value.v) {
                        out.add(new Tuple2 <>(new Edge(e.v1, v), sv2));
                        out.add(new Tuple2 <>(new Edge(v, e.v2), sv1));
                    }
                    for (int u : value.u) {
                        out.add(new Tuple2 <>(new Edge(u, e.v1), sv2));
                        out.add(new Tuple2 <>(new Edge(u, e.v2), sv1));
                    }
                }

                return out.iterator();
            }).groupByKey(numPartitions);

            long count = update.count();
            long t2 = System.currentTimeMillis();
            log("iteration: " + iteration + ", updateCount: " + count, t1, t2);
            if (count == 0)
                break;

            tSet = tSet
//                    .filter(kv -> kv._2.vSize > 0)
                    .leftOuterJoin(update)
                    .mapValues(values -> {
                        List<Tuple2<Edge, MaxTSetValue>> out = new ArrayList <>();
//                        Edge edge = kv._1;
//                        Tuple2 <MaxTSetValue, Optional <Iterable <int[]>>> values = kv._2;
                        MaxTSetValue tSetValue = values._1;

                        Optional <Iterable <int[]>> right = values._2;
                        tSetValue.updated = false;
                        if (tSetValue.vSize == 0 || !right.isPresent()) {
                            if (tSetValue.lowTSetValues != null)
                                for (MaxTSetValue lowTSetValue : tSetValue.lowTSetValues) {
                                    lowTSetValue.updated = false;
                                }
                            return tSetValue;
                        }

                        IntSet wSet = new IntOpenHashSet(tSetValue.w);
                        IntSet vSet = new IntOpenHashSet(tSetValue.v);
                        IntSet uSet = new IntOpenHashSet(tSetValue.u);
                        Int2ObjectSortedMap <IntSet> sortedMap = new Int2ObjectAVLTreeMap <>();
                        for (int[] sv : values._2.get()) {
                            int support = sv[0];
                            if (support > tSetValue.sup) {
                                continue;
                            }

                            int vertex = sv[1];
                            if (wSet.contains(vertex) || vSet.contains(vertex) || uSet.contains(vertex)) {
                                IntSet set = sortedMap.get(support);
                                if (set == null) {
                                    set = new IntOpenHashSet();
                                    sortedMap.put(support, set);
                                }
                                set.add(vertex);
                            }
                        }

                        if (sortedMap.size() == 0) {
                            return tSetValue;
                        }

                        int eSup = tSetValue.sup;

                        int removedCount = 0;
                        int pSup = 0;

                        IntSet lastWSet = new IntOpenHashSet();
                        IntSet lastVSet = new IntOpenHashSet();
                        IntSet lastUSet = new IntOpenHashSet();
                        int uSup = 0;
                        List<MaxTSetValue> others = new ArrayList <>();
                        for (Int2ObjectMap.Entry <IntSet> entry : sortedMap.int2ObjectEntrySet()) {
                            lastWSet.clear();
                            lastVSet.clear();
                            lastUSet.clear();
                            uSup = entry.getIntKey();
                            IntSet set = entry.getValue();
                            int removed = set.size();

                            for (int v : set) {
                                if (wSet.remove(v)) {
                                    lastWSet.add(v);
                                } else if (vSet.remove(v)) {
                                    lastVSet.add(v);
                                } else if (uSet.remove(v)) {
                                    lastUSet.add(v);
                                }
                            }

                            if (eSup <= uSup) {
                                if (pSup == 0)
                                    eSup = uSup;
                                break;
                            }

                            removedCount += removed;
                            eSup -= removed;
                            if (eSup <= uSup) {
                                if (pSup > 0 && eSup < uSup) {
                                    eSup = pSup;
                                } else {
                                    eSup = uSup;
                                    MaxTSetValue maxTSetValue = new MaxTSetValue();
                                    maxTSetValue.sup = uSup;
                                    maxTSetValue.w = lastWSet.toIntArray();
                                    maxTSetValue.v = lastVSet.toIntArray();
                                    maxTSetValue.u = lastUSet.toIntArray();
                                    maxTSetValue.vSize = removed;
                                    maxTSetValue.updated = false;
                                    others.add(maxTSetValue);
                                }
                                break;
                            }
                            pSup = eSup;

                            MaxTSetValue maxTSetValue = new MaxTSetValue();
                            maxTSetValue.sup = uSup;
                            maxTSetValue.w = lastWSet.toIntArray();
                            maxTSetValue.v = lastVSet.toIntArray();
                            maxTSetValue.u = lastUSet.toIntArray();
                            maxTSetValue.vSize = removed;
                            maxTSetValue.updated = false;
                            others.add(maxTSetValue);
                        }

                        if (eSup == uSup) {
                            wSet.addAll(lastWSet);
                            vSet.addAll(lastVSet);
                            uSet.addAll(lastUSet);
                        }

                        tSetValue.vSize -= removedCount;

                        tSetValue.sup = eSup;
                        if (tSetValue.w.length > wSet.size())
                            tSetValue.w = wSet.toIntArray();
                        if (tSetValue.v.length > vSet.size())
                            tSetValue.v = vSet.toIntArray();
                        if (tSetValue.u.length > uSet.size())
                            tSetValue.u = uSet.toIntArray();

                        if (others.size() > 0) {
                            if (tSetValue.lowTSetValues == null)
                                tSetValue.lowTSetValues = others;
                            else {
                                tSetValue.lowTSetValues.add(tSetValue);
                                for (MaxTSetValue tValue : tSetValue.lowTSetValues) {
                                    boolean merged = false;
                                    int index = 0;
                                    for (int i = 0; i < others.size(); i++) {
                                        MaxTSetValue oValue = others.get(i);
                                        if (oValue.sup == tValue.sup) {
                                            IntSet set = new IntOpenHashSet(tValue.w);
                                            set.addAll(new IntOpenHashSet(oValue.w));
                                            tValue.w = set.toIntArray();

                                            set = new IntOpenHashSet(tValue.v);
                                            set.addAll(new IntOpenHashSet(oValue.v));
                                            tValue.v = set.toIntArray();

                                            set = new IntOpenHashSet(tValue.u);
                                            set.addAll(new IntOpenHashSet(oValue.u));
                                            tValue.u = set.toIntArray();
                                            tValue.vSize = tValue.w.length + tValue.v.length + tValue.u.length;
                                            merged = true;
                                            index = i;
                                            break;
                                        }
                                        tValue.updated = false;
                                    }
                                    if (merged)
                                        others.remove(index);
                                }
                                tSetValue.lowTSetValues.addAll(others);
                                tSetValue.lowTSetValues.remove(tSetValue);
                            }
                        }

                        if (tSetValue.lowTSetValues != null) {
                            for (MaxTSetValue tValue : tSetValue.lowTSetValues) {
                                if (tValue.sup >= tSetValue.sup) {
                                    tValue.updated = true;
                                    break;
                                }
                            }
                        }

                        tSetValue.updated = true;

                        return tSetValue;
                    }).persist(StorageLevel.MEMORY_AND_DISK());

            printKCount(tSet.mapValues(v -> v.sup).reduceByKey((a, b) -> Math.max(a, b)));
        }

        return tSet;
    }

    private JavaPairRDD <Edge, MaxTSetValue> createTSet(JavaPairRDD <Integer, int[]> fonl,
                                                        JavaPairRDD <Integer, int[]> candidates) {
        int partitionNum = fonl.getNumPartitions();
        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD <Edge, MaxTSetValue> tSet = candidates.cogroup(fonl)
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
                    IntSet wSet = new IntOpenHashSet();
                    IntSet vSet = new IntOpenHashSet();
                    IntSet uSet = new IntOpenHashSet();
                    int sup = 0;
                    for (VertexByte value : values) {
                        if (value.sign == W_UVW)
                            wSet.add(value.vertex);
                        else if (value.sign == V_UVW)
                            vSet.add(value.vertex);
                        else
                            uSet.add(value.vertex);
                        sup++;
                    }
                    MaxTSetValue value = new MaxTSetValue();
                    value.sup = sup;
                    value.w = wSet.toIntArray();
                    value.v = vSet.toIntArray();
                    value.u = uSet.toIntArray();
                    value.updated = true;
                    value.vSize = sup;
                    return value;
                }).persist(StorageLevel.DISK_ONLY()); // Use disk too because this RDD often is very large

        return tSet;
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

        MaxTrussTSetRange kTrussTSet = new MaxTrussTSetRange(neighborList, conf);
        JavaPairRDD <Edge, Integer> truss = kTrussTSet.explore();
        long t2 = System.currentTimeMillis();
        log("KTruss edge count: " + truss.count(), t1, t2);

        printKCount(truss);

        kTrussTSet.close();
    }

    private static void printKCount(JavaPairRDD <Edge, Integer> truss) {
        printKCount(truss, 5);
    }

    private static void printKCount(JavaPairRDD <Edge, Integer> truss, int size) {
        Map <Integer, Long> kCount = truss.map(kv -> kv._2).countByValue();
        SortedMap <Integer, Long> sortedMap = new TreeMap <>(kCount);
        int count = 0;
        int sum = 0;
        for (Map.Entry <Integer, Long> entry : sortedMap.entrySet()) {
            sum += entry.getValue();
            if (count++ > size) {
                continue;
            }
            log("K: " + entry.getKey() + ", entry: " + entry.getValue());
        }
        log("sum: " + sum);
    }
}

