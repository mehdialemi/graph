package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.*;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.kcore.KCoreConf;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.OrderedVertex;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
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
public class KTrussTSet2 extends SparkApp {
    private static final int INVALID = -1;
    private static final int META_LEN = 4;
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;
    public static final int CHECKPOINT_ITERATION = 50;
    public static final int[] EMPTY_ARRAY = {};
    public static final ArrayList <Integer> EMPTY_ARRAY_LIST = new ArrayList <>();

    private final NeighborList neighborList;
    private KTrussConf ktConf;

    public KTrussTSet2(NeighborList neighborList, SparkAppConf conf) throws URISyntaxException {
        super(neighborList);
        this.neighborList = neighborList;
        String master = conf.getSc().master();
        this.conf.getSc().setCheckpointDir("/tmp/checkpoint");
        if (master.contains("local")) {
            return;
        }
        String masterHost = new URI(conf.getSc().master()).getHost();
        this.conf.getSc().setCheckpointDir("hdfs://" + masterHost + "/shared/checkpoint");
    }

    public JavaPairRDD <Edge, Integer> generate(int min, int max) {

        KCore kCore = new KCore(neighborList, new KCoreConf(conf, 2, 1000));

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD <Integer, int[]> fonl = triangle.getOrCreateFonl();

        JavaPairRDD <Integer, int[]> candidates = triangle.createCandidates(fonl);

        int partitionNum = fonl.getNumPartitions();

        JavaPairRDD <Edge, int[]> tSet = createTSet(fonl, candidates);

        Queue <JavaPairRDD <Edge, Integer>> eSupQueue = new LinkedList <>();
        JavaPairRDD <Edge, Integer> fSup = conf.getSc().parallelizePairs(new ArrayList <>());

        JavaPairRDD <Edge, int[]> processed = conf.getSc().parallelizePairs(new ArrayList <>());

        long edgeCount = tSet.count();
        int fCount = 0;
        while (fCount < edgeCount || min <= max) {
            final int minSup = min;
            int minCount = 0;
            JavaPairRDD <Edge, Integer> eSup = tSet.mapValues(v -> v[0]).cache();
            JavaPairRDD <Edge, Integer> minSupEdge = eSup
                    .leftOuterJoin(processed)
                    .mapValues(val -> val._1 - val._2.orElse(EMPTY_ARRAY).length)
                    .filter(kv -> kv._2 == minSup)
                    .persist(StorageLevel.MEMORY_AND_DISK());
            for (int iter = 0; iter < ktConf.getKtMaxIter(); iter++) {

                long t1 = System.currentTimeMillis();

                if ((iter + 1) % CHECKPOINT_ITERATION == 0) {
                    tSet.checkpoint();
                }

                eSupQueue.add(eSup);
                if (eSupQueue.size() > 1)
                    eSupQueue.remove().unpersist();


                long invalidCount = minSupEdge.count();

                // If no invalid edge is found then the program terminates
                if (invalidCount == 0) {
                    min++;
                    break;
                }
                minCount += invalidCount;
                fCount += invalidCount;
                fSup = fSup.union(minSupEdge);
                JavaPairRDD <Edge, Iterable <Integer>> uSup = minSupEdge.join(tSet).mapValues(v -> v._2).flatMapToPair(kv -> {
                    int i = META_LEN;
                    Edge e = kv._1;
                    List <Tuple2 <Edge, Integer>> out = new ArrayList <>((kv._2.length - 3) * 2);
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
                }).groupByKey(partitionNum);

                JavaPairRDD <Edge, int[]> newUpdates = uSup.subtractByKey(fSup)
                        .leftOuterJoin(processed).mapValues(val -> {
                            IntSet set = new IntOpenHashSet(val._2.or(EMPTY_ARRAY));
                            for (Integer v : val._1) {
                                set.add(v);
                            }
                            return set.toIntArray();
                        }).cache();

                processed = processed.fullOuterJoin(newUpdates).mapValues(val -> {
                    if (!val._2.isPresent())
                        return val._1.get();
                    return val._2.get();
                }).cache();

                minSupEdge = newUpdates.mapValues(val -> val.length)
                        .join(eSup)
                        .filter(kv -> (kv._2._2 - kv._2._1) <= minSup)
                        .mapValues(val -> minSup).cache();

                long nCount = newUpdates.count();
                long t2 = System.currentTimeMillis();
                String msg = "minSup: " + minSup + ", minCount: " + minCount + ", iteration: " + (iter + 1) +
                        ", invalid edge count: " + invalidCount + ", newUpdates count: " + nCount;
                log(msg, t2 - t1);
            }
            processed = processed.subtractByKey(fSup).cache();
            tSet = tSet.subtractByKey(fSup).cache();
        }

        return fSup;
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
                }).groupByKey(partitionNum * 2)
                .mapValues(values -> {
                    List <OrderedVertex> list = new ArrayList <>();
                    int sw = 0, sv = 0, su = 0;
                    for (OrderedVertex value : values) {
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

                    for (OrderedVertex vb : list) {
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
        Logger.getLogger("org.apache.spar").setLevel(Level.INFO);
        long t1 = System.currentTimeMillis();

        ArgumentReader argumentReader = new ArgumentReader(args);
        SparkAppConf conf = new SparkAppConf(argumentReader) {
            @Override
            protected String createAppName() {
                return "MaxTrussTSet2" + super.createAppName();
            }
        };
        conf.init();
        int min = argumentReader.nextInt(1);
        int max = argumentReader.nextInt(100000);
        EdgeLoader edgeLoader = new EdgeLoader(conf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        KTrussTSet2 kTrussTSet = new KTrussTSet2(neighborList, conf);
        JavaPairRDD <Edge, Integer> truss = kTrussTSet.generate(min, max);
        long t2 = System.currentTimeMillis();
        log("ktruss decomposition completed", t1, t2);
        printKCount(truss, max);

        kTrussTSet.close();
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

