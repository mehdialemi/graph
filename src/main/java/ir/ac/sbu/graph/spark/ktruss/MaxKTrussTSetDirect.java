package ir.ac.sbu.graph.spark.ktruss;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;
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
public class MaxKTrussTSetDirect extends SparkApp {

    public static final int[] EMPTY_INT_ARRAY = {};
    private final NeighborList neighborList;
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

    public Map <Integer, JavaRDD <Edge>> explore(float consumptionRatio, int minPartitions) {

        KCore kCore = new KCore(neighborList, kCoreConf);

        Triangle triangle = new Triangle(kCore);
        JavaPairRDD <Integer, int[]> fonl = triangle.getOrCreateFonl();
        JavaPairRDD <Integer, int[]> candidates = triangle.createCandidates(fonl);

        int partitionNum = Math.max(minPartitions, fonl.getNumPartitions());
        JavaPairRDD <Edge, Integer> eSup = calcSup(fonl, candidates);

        return maxTruss(eSup, partitionNum, 30);
    }

    public Map <Integer, JavaRDD <Edge>> maxTruss(JavaPairRDD <Edge, Integer> eSup, int partitionNum, int maxSup) {

        Map <Integer, JavaRDD <Edge>> eTrussMap = new HashMap <>();
//        JavaPairRDD <Integer, Tuple2 <int[], int[]>> neighborSup = eSup.mapToPair(kv -> new Tuple2 <>(kv._1.v1, new Tuple2 <>(kv._1.v2, kv._2)))
//                .groupByKey(partitionNum).mapValues(values -> {
//                    IntList listNeighbor = new IntArrayList();
//                    IntList listSup = new IntArrayList();
//                    for (Tuple2 <Integer, Integer> tuple : values) {
//                        listNeighbor.add(tuple._1);
//                        listSup.add(tuple._2);
//                    }
//                    return new Tuple2 <>(listNeighbor.toIntArray(), listSup.toIntArray());
//                });

        JavaPairRDD <Integer, int[]> fonl = eSup.filter(kv -> kv._2 > 1)
                .keys()
                .mapToPair(kv -> new Tuple2 <>(kv.v1, kv.v2))
                .groupByKey(partitionNum)
                .mapValues(values -> {
                    IntSet set = new IntOpenHashSet();
                    for (Integer v : values) {
                        set.add(v);
                    }
                    return set.toIntArray();
                });

        for (int sup = 1; sup < maxSup; sup++) {
            int minSup = sup + 1;
            long eSupCount = eSup.count();
            log("Exploring minSup: " + minSup + ", eSupCount: " + eSupCount);

            int iteration = 0;
            while (true) {
                iteration++;
                long startMinSup = System.currentTimeMillis();
                JavaPairRDD <Edge, Integer> invalids = eSup.filter(kv -> kv._2 < minSup).cache();
                long count = invalids.count();
                long endMinSup = System.currentTimeMillis();
                log("MinSup: " + minSup + ", iteration: " + iteration + ", count: " + count,
                        startMinSup, endMinSup);

                if (count == 0)
                    break;

                JavaRDD <Edge> value = eTrussMap.get(sup);
                JavaRDD <Edge> edges = invalids.keys().persist(StorageLevel.DISK_ONLY());
                if (value == null) {
                    eTrussMap.put(sup, edges);
                } else {
                    value = value.union(edges);
                    eTrussMap.put(sup, value);
                }

                JavaPairRDD <Integer, Iterable <Integer>> removes = edges.flatMapToPair(edge -> {
                    List <Tuple2 <Integer, Integer>> out = new ArrayList <>();
                    out.add(new Tuple2 <>(edge.v1, edge.v2));
                    out.add(new Tuple2 <>(edge.v2, edge.v1));
                    return out.iterator();
                }).groupByKey().cache();

                log("removes count: " + removes.count());

//                JavaPairRDD <Edge, Integer> validSups = eSup.filter(kv -> kv._2 >= minSup).cache();
//
//                log("valid count: " + validSups.count());

                JavaPairRDD <Integer, int[]> allNeighbors = eSup.flatMapToPair(kv -> {
                    List <Tuple2 <Integer, Integer>> out = new ArrayList <>();
                    out.add(new Tuple2 <>(kv._1.v1, kv._1.v2));
                    out.add(new Tuple2 <>(kv._1.v2, kv._1.v1));
                    return out.iterator();
                }).groupByKey().mapValues(values -> {
                    IntSet set = new IntOpenHashSet();
                    for (Integer v : values) {
                        set.add(v);
                    }
                    return set.toIntArray();
                }).repartition(partitionNum).cache();

                log("All neighbors count: " + allNeighbors.count());

                JavaPairRDD <Edge, Integer> subtract = allNeighbors.join(removes).flatMapToPair(kv -> {
                    int u = kv._1;
                    Tuple2 <int[], Iterable <Integer>> values = kv._2;
                    IntIntMap map = new IntIntHashMap();
                    for (Integer v : values._2) {
                        map.addTo(v, 1);
                    }
                    List <Tuple2 <Edge, Integer>> out = new ArrayList <>();
                    for (int v : values._1) {
                        int sub = map.getOrDefault(v, 0);
                        if (sub == 0)
                            continue;
                        out.add(new Tuple2 <>(new Edge(u, v), sub));
                        out.add(new Tuple2 <>(new Edge(v, u), sub));
                    }
                    return out.iterator();
                }).reduceByKey((a, b) -> a + b);
                log("subtract count: " + subtract.count());

                eSup = eSup.filter(kv -> kv._2 >= minSup)
                        .leftOuterJoin(subtract).mapValues(v -> v._1 - v._2.or(0))
                        .cache();
            }

            JavaRDD <Edge> result = eTrussMap.get(sup);
            if (result != null) {
                log("support: " + sup + ", count: " + result.count());
            }
        }
        return eTrussMap;
    }

    private JavaPairRDD <Edge, Integer> calcSup(JavaPairRDD <Integer, int[]> fonl,
                                                JavaPairRDD <Integer, int[]> candidates) {
        int partitionNum = fonl.getNumPartitions();
        JavaPairRDD <Edge, Integer> tSet = candidates.cogroup(fonl)
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

                                output.add(new Tuple2 <>(uv, 1));
                                output.add(new Tuple2 <>(uw, 1));
                                output.add(new Tuple2 <>(vw, 1));

                                fi++;
                                ci++;
                            }
                        }
                    }

                    return output.iterator();
                })
                .reduceByKey((a, b) -> a + b)
                .repartition(partitionNum)
                .cache();

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
        Map <Integer, JavaRDD <Edge>> eTrussMap = kTrussTSet.explore(consumptionRatio, minPartitions);
        log("KTruss edge count: " + eTrussMap.size(), t1, System.currentTimeMillis());

        for (Map.Entry <Integer, JavaRDD <Edge>> entry : eTrussMap.entrySet()) {
            log("K: " + entry.getKey() + ", Count: " + entry.getValue().count());
        }
        kTrussTSet.close();
    }
}

