package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.*;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.kcore.KCoreConf;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.api.java.JavaPairRDD;
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
public class MaxTrussOrderedTSet extends SparkApp {

    private final NeighborList neighborList;
    private AtomicInteger iterations = new AtomicInteger(0);
    private final KCoreConf kCoreConf;

    MaxTrussOrderedTSet(NeighborList neighborList, SparkAppConf conf) throws URISyntaxException {
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

        JavaPairRDD <Edge, OTSetValue> oTSet = createOrderedTSet(fonl, candidates);
        int numPartitions = oTSet.getNumPartitions();

        long maxUpdates = 2;
        long updateCount = 1;
        while (true) {
            final int iteration = iterations.incrementAndGet();
            if (iteration % 20 == 0)
                oTSet.checkpoint();


            long t1 = System.currentTimeMillis();
            JavaPairRDD <Edge, Iterable <int[]>> supUpdates = oTSet.filter(kv -> kv._2.support < 0 && !kv._2.disabled)
                    .flatMapToPair(kv -> {
                        Edge e = kv._1;
                        OTSetValue oTSetValue = kv._2;
                        int sup = -oTSetValue.support;
                        List <Tuple2 <Edge, int[]>> out = new ArrayList <>();
                        int[] supV1 = new int[]{sup, e.v1};
                        int[] supV2 = new int[]{sup, e.v2};
                        for (int v : oTSetValue.vertices) {
                            out.add(new Tuple2 <>(new Edge(e.v1, v), supV2));
                            // TODO check to remove this
//                            out.add(new Tuple2 <>(new Edge(e.v2, v), supV1));
                        }
                        return out.iterator();
                    }).groupByKey(numPartitions);
            updateCount = supUpdates.count();
            long t2 = System.currentTimeMillis();

            log("iteration: " + iteration + ", updateCount: " + updateCount, t1, t2);
            if (updateCount == 0)
                break;

            if (updateCount > maxUpdates)
                maxUpdates = updateCount;



            oTSet = oTSet.leftOuterJoin(supUpdates).mapValues(values -> {
                OTSetValue otSetValue = values._1;
                int eSup = Math.abs(otSetValue.support);
                otSetValue.support = eSup;
                if (!values._2.isPresent() || otSetValue.disabled) {
                    return otSetValue;
                }

                int prevSup = eSup;
                Int2IntMap map = new Int2IntOpenHashMap();
                for (int[] sv : values._2.get()) {
                    int uSup = Math.abs(sv[0]);
                    int vertex = sv[1];

                    if (uSup >= eSup) {
                        continue;
                    }

                    map.putIfAbsent(vertex, uSup);
                    map.computeIfPresent(vertex, (k, v) -> Math.min(v, uSup));

                }

                Int2IntAVLTreeMap ascSupCount = new Int2IntAVLTreeMap();
                for (Map.Entry <Integer, Integer> entry : map.entrySet()) {
                    int uSup = entry.getValue();
                    ascSupCount.addTo(uSup, 1);
                }

                if (ascSupCount.size() == 0)
                    return otSetValue;

                int uSup;
                int num = 0;
                int pSup = 0;
                for (Int2IntMap.Entry entry : ascSupCount.int2IntEntrySet()) {
                    num++;
                    uSup = entry.getIntKey();
                    int count = entry.getValue();
                    int newSup = eSup - count;
                    if (newSup <= uSup) {
                        if (num == 1)
                            eSup = uSup;
                        if (eSup < pSup)
                            eSup = pSup;
                        break;
                    }
                    eSup = newSup;
                    pSup = eSup;
                }

                if (otSetValue.support == 1)
                    otSetValue.disabled = true;

                if (eSup < prevSup) {
                    otSetValue.support = -eSup;
                }
                return otSetValue;
            }).cache();
            // rebuild oTSet
            JavaPairRDD <Edge, Iterable <int[]>> valueUpdates = oTSet.flatMapToPair(kv -> {
                OTSetValue otSetValue = kv._2;
                List <Tuple2 <Edge, int[]>> out = new ArrayList <>();
                if (otSetValue.disabled)
                    return out.iterator();

                int sup = Math.abs(otSetValue.support);
                Edge e = kv._1;

                int[] supV1 = new int[]{sup, e.v1};
                int[] supV2 = new int[]{sup, e.v2};
                for (int v : otSetValue.vertices) {
                    out.add(new Tuple2 <>(new Edge(e.v1, v), supV2));
                    out.add(new Tuple2 <>(new Edge(e.v2, v), supV1));
                }
                return out.iterator();
            }).groupByKey(numPartitions);

            oTSet = oTSet.leftOuterJoin(valueUpdates).mapValues(values -> {
                OTSetValue otSetValue = values._1;
                int eSup = Math.abs(otSetValue.support);
                if (!values._2.isPresent() || otSetValue.disabled) {
                    return otSetValue;
                }

                IntSet set = new IntOpenHashSet(otSetValue.vertices);
                for (int[] sv : values._2.get()) {
                    int uSup = Math.abs(sv[0]);
                    int vertex = sv[1];

                    if (uSup < eSup) {
                        set.remove(vertex);
                        continue;
                    } else {
                        otSetValue.support = -eSup;
                        set.add(vertex);
                    }
                }

                otSetValue.vertices = set.toIntArray();
                return otSetValue;
            }).cache();

            printKCount(oTSet.mapValues(v -> Math.abs(v.support)));
        }

        return oTSet.mapValues(v -> Math.abs(v.support));
    }


    private JavaPairRDD <Edge, OTSetValue> createOrderedTSet(JavaPairRDD <Integer, int[]> fonl,
                                                                          JavaPairRDD <Integer, int[]> candidates) {
        int partitionNum = fonl.getNumPartitions();
        // Generate kv such that key is an edge and value is its triangle vertices.

        return candidates.cogroup(fonl)
                .flatMapToPair(t -> {
                    int[] fVal = t._2._2.iterator().next();
                    Arrays.sort(fVal, 1, fVal.length);
                    int v = t._1;

                    List <Tuple2 <Edge, Integer>> output = new ArrayList <>();
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

                                output.add(new Tuple2 <>(uv, w));
//                                output.add(new Tuple2 <>(uw, -v));
                                output.add(new Tuple2 <>(uw, 0));
                                output.add(new Tuple2 <>(vw, 0));

                                fi++;
                                ci++;
                            }
                        }
                    }

                    return output.iterator();
                }).groupByKey(partitionNum)
                .mapValues(values -> {
                    int sup = 0;
                    IntSet set = new IntOpenHashSet();
                    for (Integer value : values) {
                        sup++;
                        if (value != 0)
                            set.add(value);
                    }
                    OTSetValue oTSetValue = new OTSetValue();
                    oTSetValue.disabled = false;
                    oTSetValue.support = -sup;
                    oTSetValue.vertices = set.toIntArray();
                    return oTSetValue;
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

        MaxTrussOrderedTSet kTrussTSet = new MaxTrussOrderedTSet(neighborList, conf);
        JavaPairRDD <Edge, Integer> truss = kTrussTSet.explore();
        long count = truss.count();
        log("KTruss edge count: " + count, t1, System.currentTimeMillis());

        printKCount(truss, 10000);
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

