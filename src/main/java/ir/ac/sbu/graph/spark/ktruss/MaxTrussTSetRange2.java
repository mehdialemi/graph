package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.*;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.kcore.KCoreConf;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.OrderedVertex;
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
public class MaxTrussTSetRange2 extends SparkApp {
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;
    private final NeighborList neighborList;
    private final KCoreConf kCoreConf;
    private final AtomicInteger totalIterations = new AtomicInteger(0);

    public MaxTrussTSetRange2(NeighborList neighborList, SparkAppConf conf) throws URISyntaxException {
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

    public JavaPairRDD <Edge, Integer> explore(int minPartitions, int maxK) {
        log("maxK: " + maxK);
        KCore kCore = new KCore(neighborList, kCoreConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD <Integer, int[]> fonl = triangle.getOrCreateFonl();

        int numPartitions = fonl.getNumPartitions();
        numPartitions = Math.max(numPartitions * 5, minPartitions);
        JavaPairRDD <Integer, int[]> candidates = triangle.createCandidates(fonl);

        JavaPairRDD <Edge, MaxTSetValue> tSet = createTSet(fonl, candidates, numPartitions);
        JavaPairRDD <Edge, Integer> maxTruss = conf.getSc().parallelizePairs(new ArrayList <>());
        long maxSup = 2;
        long iteration = 0;
        while (true) {
            iteration ++;
            long t1 = System.currentTimeMillis();
            final long max = maxSup;
            final long cSup = iteration;
            JavaPairRDD <Edge, Iterable <int[]>> update = tSet
                    .filter(kv -> kv._2.updated && kv._2.sup < max)
                    .flatMapToPair(kv -> {
                        Edge e = kv._1;
                        List <Tuple2 <Edge, int[]>> out = new ArrayList <>();
                        MaxTSetValue value = kv._2;

                        int sup = value.sup;
                        int[] sv2 = new int[]{sup, e.v2};
                        int[] sv1 = new int[]{sup, e.v1};
                        for (int i = 0; i < value.w.length; i++) {
                            if (value.kw[i] < value.sup)
                                continue;
                            int w = value.w[i];
                            out.add(new Tuple2 <>(new Edge(e.v1, w), sv2));
                            out.add(new Tuple2 <>(new Edge(e.v2, w), sv1));
                        }

                        for (int i = 0; i < value.v.length; i++) {
                            if (value.kv[i] < value.sup)
                                continue;
                            int v = value.v[i];
                            out.add(new Tuple2 <>(new Edge(e.v1, v), sv2));
                            out.add(new Tuple2 <>(new Edge(v, e.v2), sv1));
                        }

                        for (int i = 0; i < value.u.length; i++) {
                            if (value.ku[i] < value.sup)
                                continue;
                            int u = value.u[i];
                            out.add(new Tuple2 <>(new Edge(u, e.v1), sv2));
                            out.add(new Tuple2 <>(new Edge(u, e.v2), sv1));
                        }

                        return out.iterator();
                    })
                    .groupByKey(numPartitions)
                    .cache();

            tSet = tSet
                    .leftOuterJoin(update)
                    .mapValues(values -> {
                        MaxTSetValue value = values._1;

                        Optional <Iterable <int[]>> right = values._2;
                        if (value.sup < max) {
                            value.updated = false;
                        }

                        if (value.sup == 1) {
                            value.remove = true;
                            return value;
                        }

                        if (!right.isPresent()) {
                            if (value.updated)
                                value.removeTracker = 0;
                            if (value.removeTracker > value.sup) {
                                value.remove = true;
                            } else if(value.sup < cSup)
                                value.removeTracker ++;
                            return value;
                        }
                        value.removeTracker = 0;
                        Int2IntMap updateMap = new Int2IntOpenHashMap();
                        for (int[] sv : values._2.get()) {
                            int support = sv[0];
                            if (support >= value.sup) {
                                continue;
                            }

                            int vertex = sv[1];
                            int sup = updateMap.getOrDefault(vertex, Integer.MAX_VALUE);
                            if (support < sup)
                                updateMap.put(vertex, support);
                        }

                        if (updateMap.size() == 0) {
                            return value;
                        }

                        for (Int2IntMap.Entry entry : updateMap.int2IntEntrySet()) {
                            int vertex = entry.getIntKey();
                            int support = entry.getIntValue();
                            int index;
                            if ((index = Arrays.binarySearch(value.w, vertex)) >= 0) {
                                value.kw[index] = support;
                            } else if ((index = Arrays.binarySearch(value.v, vertex)) >= 0) {
                                value.kv[index] = support;
                            } else if ((index = Arrays.binarySearch(value.u, vertex)) >= 0) {
                                value.ku[index] = support;
                            }
                        }

                        Int2IntAVLTreeMap sortedMap = new Int2IntAVLTreeMap(Comparator.comparingInt(a -> -a));
                        // find max sup after value.sup
                        int supCount = 0;

                        for (int k : value.kw) {
                            if (k < value.sup)
                                sortedMap.addTo(k, 1);
                            else
                                supCount++;
                        }
                        for (int k : value.kv) {
                            if (k < value.sup)
                                sortedMap.addTo(k, 1);
                            else
                                supCount++;
                        }
                        for (int k : value.ku) {
                            if (k < value.sup)
                                sortedMap.addTo(k, 1);
                            else
                                supCount++;
                        }

                        int sum = supCount;
                        int sup = sum;
                        for (Int2IntMap.Entry entry : sortedMap.int2IntEntrySet()) {
                            int uSup = entry.getIntKey();
                            if (uSup <= sum) {
                                break;
                            }

                            sum = sum + entry.getIntValue();
                            if (uSup <= sum) {
                                sup = uSup;
                                break;
                            }
                            sup = sum;
                        }

                        if (sup < value.sup) {
                            value.sup = sup;
                            value.updated = true;
                        }

                        return value;
                    }).persist(StorageLevel.MEMORY_AND_DISK());

            JavaPairRDD <Edge, Integer> truss = tSet.filter(kv -> kv._2.remove)
                    .mapValues(kv -> kv.sup)
                    .persist(StorageLevel.MEMORY_AND_DISK());
            long rcount = truss.count();
            maxTruss = maxTruss.union(truss);
//            tSet = tSet.filter(kv -> !kv._2.remove).cache();
            tSet = tSet.subtractByKey(truss).cache();

            maxSup *= 2;
            long t2 = System.currentTimeMillis();
            long tSetCount = tSet.count();
            if (tSetCount == 0)
                break;
            log("tSet count: " + tSetCount + ", remove count: " + rcount, t1, t2);
            printKCount(maxTruss);
        }

        return maxTruss;
    }

    private JavaPairRDD <Edge, MaxTSetValue> createTSet(JavaPairRDD <Integer, int[]> fonl,
                                                        JavaPairRDD <Integer, int[]> candidates,
                                                        int partitionNum) {

        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD <Edge, MaxTSetValue> tSet = candidates.cogroup(fonl)
                .flatMapToPair(t -> {
                    int[] fVal = t._2._2.iterator().next();
                    Arrays.sort(fVal, 1, fVal.length);
                    int v = t._1;

                    List <Tuple2 <Edge, OrderedVertex>> output = new ArrayList <>();
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

                                output.add(new Tuple2 <>(uv, new OrderedVertex(w, W_UVW)));
                                output.add(new Tuple2 <>(uw, new OrderedVertex(v, V_UVW)));
                                output.add(new Tuple2 <>(vw, new OrderedVertex(u, U_UVW)));

                                fi++;
                                ci++;
                            }
                        }
                    }

                    return output.iterator();
                }).groupByKey(partitionNum)
                .mapValues(values -> {
                    IntSortedSet wSet = new IntAVLTreeSet();
                    IntSortedSet vSet = new IntAVLTreeSet();
                    IntSortedSet uSet = new IntAVLTreeSet();
                    int sup = 0;
                    for (OrderedVertex value : values) {
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
                    value.kw = new int[value.w.length];
                    for (int i = 0; i < value.kw.length; i++) {
                        value.kw[i] = sup;
                    }

                    value.v = vSet.toIntArray();
                    value.kv = new int[value.v.length];
                    for (int i = 0; i < value.kv.length; i++) {
                        value.kv[i] = sup;
                    }

                    value.u = uSet.toIntArray();
                    value.ku = new int[value.u.length];
                    for (int i = 0; i < value.ku.length; i++) {
                        value.ku[i] = sup;
                    }
                    if (sup == 1)
                        value.remove = true;
                    value.updated = true;
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
                return "MaxTrussTSetRange-" + super.createAppName();
            }
        };
        conf.init();
        int minPartitions = argumentReader.nextInt(2);
        int maxK = argumentReader.nextInt(100000);
        EdgeLoader edgeLoader = new EdgeLoader(conf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        MaxTrussTSetRange2 kTrussTSet = new MaxTrussTSetRange2(neighborList, conf);
        JavaPairRDD <Edge, Integer> truss = kTrussTSet.explore(minPartitions, maxK);
        long t2 = System.currentTimeMillis();
        log("KTruss edge count: " + truss.count(), t1, t2);

        printKCount(truss, maxK);

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
            if (++count > size) {
                continue;
            }
            log("K: " + entry.getKey() + ", entry: " + entry.getValue());
        }
        log("sum: " + sum);
    }
}

