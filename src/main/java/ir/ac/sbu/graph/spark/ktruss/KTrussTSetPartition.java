package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.*;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.OrderedVertex;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
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
public class KTrussTSetPartition extends SparkApp {
    private static final int INVALID = -1;
    private static final int META_LEN = 4;
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;
    public static final int CHECKPOINT_ITERATION = 50;

    private final NeighborList neighborList;
    private final int k;
    private KTrussConf ktConf;
    private int partitionBufferSize;

    public KTrussTSetPartition(NeighborList neighborList, KTrussConf ktConf, int partitionBufferSize) throws URISyntaxException {
        super(neighborList);
        this.neighborList = neighborList;
        this.k = ktConf.getKt();
        this.ktConf = ktConf;
        this.partitionBufferSize = partitionBufferSize;
        String master = ktConf.getSc().master();
        if (master.contains("local"))
            return;
        String masterHost = new URI(ktConf.getSc().master()).getHost();
        this.ktConf.getSc().setCheckpointDir("hdfs://" + masterHost + "/shared/checkpoint");
    }

    public JavaRDD<EdgeSup> generate() {

        KCore kCore = new KCore(neighborList, ktConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD<Integer, int[]> fonl = triangle.getOrCreateFonl();
        JavaPairRDD<Integer, int[]> candidates = triangle.createCandidates(fonl);
        int partitionNum = fonl.getNumPartitions();

        JavaPairRDD<EdgeSup, int[]> tSet = createTSet(fonl, candidates);
        JavaRDD <EdgeSup> edgeSup = tSet.keys().cache();
        Map <Integer, Long> supCount = edgeSup.map(e -> e.sup).countByValue();

        Map<Integer, Tuple2<Integer, Integer>> map = new HashMap <>();
        Int2IntSortedMap partitionMap = Int2IntSortedMaps.EMPTY_MAP;

        int currentBufferSize = 0;
        int lowerPartitionIndex = 1;
        int upperPartitionIndex = 0;
        Set<Integer> set = new HashSet <>();
        int maxSup = 1;
        for (Map.Entry <Integer, Long> entry : supCount.entrySet()) {
            maxSup = entry.getKey();

            set.add(maxSup);
            long count = entry.getValue();

            long bufferSize = (maxSup * count + (count * 3)) * Integer.BYTES;
            currentBufferSize += bufferSize;
            int pRatio = currentBufferSize / partitionBufferSize;
            upperPartitionIndex = lowerPartitionIndex + pRatio;

            if (upperPartitionIndex > lowerPartitionIndex) {
                for (Integer eSup : set) {
                    map.put(eSup, new Tuple2 <>(lowerPartitionIndex, upperPartitionIndex));
                }
                lowerPartitionIndex = upperPartitionIndex + 1;
                set.clear();
            }

            if (upperPartitionIndex > partitionNum) {
                partitionMap.put(upperPartitionIndex, maxSup);
            }
        }

        for (Integer eSup : set) {
            map.put(eSup, new Tuple2 <>(lowerPartitionIndex, upperPartitionIndex));
        }

        if (partitionMap.containsKey(upperPartitionIndex)) {
            partitionMap.remove(upperPartitionIndex);
        }
        partitionMap.put(upperPartitionIndex + 1, maxSup);

        Partitioner partitioner = new EdgeSupPartitioner(upperPartitionIndex, map);
        JavaPairRDD <EdgeSup, int[]> tSetPartition = tSet.partitionBy(partitioner);

        int low = 0;
        int minSup = 1;
        JavaPairRDD <Edge, Integer> eSup = edgeSup.mapToPair(t -> new Tuple2 <>(new Edge(t.v1, t.v2), t.sup));
        JavaRDD <EdgeSup> result = conf.getSc().parallelize(new ArrayList <>());
        for (Map.Entry <Integer, Integer> entry : partitionMap.entrySet()) {
            int high = entry.getKey();
            maxSup = entry.getValue();
            JavaRDD <Tuple2 <EdgeSup, int[]>> rdd = getPartitionsInRange(tSetPartition, low, high);
            for (int i = minSup; i <= maxSup; i ++) {
                eSup = updateEdgeSupport(rdd, i, eSup, result);
            }
            minSup = maxSup + 1;
        }

        return result;
    }

    private JavaRDD <Tuple2 <EdgeSup, int[]>> getPartitionsInRange(JavaPairRDD<EdgeSup, int[]> tSet, int low, int high) {
        JavaRDD <Tuple2 <EdgeSup, int[]>> rdd = tSet
                .mapPartitionsWithIndex((index, iterator) -> {
                    List<Tuple2<EdgeSup, int[]>> list = new ArrayList <>();
                    if (index >= low && index <= high)
                        return iterator;
                    return list.iterator();
                }, true);
        return rdd;
    }

    private JavaPairRDD<Edge, Integer> updateEdgeSupport(JavaRDD <Tuple2 <EdgeSup, int[]>> input,
                                                         int minSup,
                                                         JavaPairRDD<Edge, Integer> edgeSup,
                                                         JavaRDD<EdgeSup> result) {

        JavaPairRDD <EdgeSup, int[]> tSet = input.mapToPair(t -> new Tuple2 <>(t._1, t._2));
        Queue<JavaPairRDD<EdgeSup, int[]>> tSetQueue = new LinkedList<>();
        tSetQueue.add(tSet);

        int iteration = 0;
        while (true) {

            long t1 = System.currentTimeMillis();

            // Detect invalid edges by comparing the support of triangle vertex set
            JavaPairRDD <EdgeSup, int[]> invalids = tSet.filter(kv -> kv._1.sup < minSup).cache();
            long invalidCount = invalids.count();

            // If no invalid edge is found then the program terminates
            if (invalidCount == 0) {
                break;
            }

            result.union(invalids.keys());

            long t2 = System.currentTimeMillis();
            String msg = "minSup: " + minSup + " iteration: " + (iteration + 1) + ", invalid edge count: " + invalidCount;
            log(msg, t2 - t1);

            // The edges in the key part of invalids key-values should be removed. So, we detect other
            // edges of their involved triangle from their triangle vertex set. Here, we determine the
            // vertices which should be removed from the triangle vertex set related to the other edges.
            JavaPairRDD <Edge, Integer> invUpdates = invalids.flatMapToPair(kv -> {
                int i = META_LEN;

                Edge e = kv._1;
                List <Tuple2 <Edge, Integer>> out = new ArrayList <>((kv._2.length - 3) * 2);
                for (; i < kv._2[1]; i++) {
                    if (kv._2[i] == INVALID)
                        continue;
                    out.add(new Tuple2 <>(new Edge(e.v1, kv._2[i]), 1));
                    out.add(new Tuple2 <>(new Edge(e.v2, kv._2[i]), 1));
                }

                for (; i < kv._2[2]; i++) {
                    if (kv._2[i] == INVALID)
                        continue;
                    out.add(new Tuple2 <>(new Edge(e.v1, kv._2[i]), 1));
                    out.add(new Tuple2 <>(new Edge(kv._2[i], e.v2), 1));
                }

                for (; i < kv._2[3]; i++) {
                    if (kv._2[i] == INVALID)
                        continue;
                    out.add(new Tuple2 <>(new Edge(kv._2[i], e.v1), 1));
                    out.add(new Tuple2 <>(new Edge(kv._2[i], e.v2), 1));
                }

                return out.iterator();
            }).reduceByKey((a, b) -> a + b);

            edgeSup = edgeSup.filter(kv -> kv._2 >= minSup)
                    .join(invUpdates).mapValues(v -> v._1 - v._2).cache();

            tSetQueue.remove().unpersist();
        }

        return edgeSup;
    }


    private JavaPairRDD<EdgeSup, int[]> createTSet(JavaPairRDD<Integer, int[]> fonl,
                                                JavaPairRDD<Integer, int[]> candidates) {
        int partitionNum = fonl.getNumPartitions();
        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD<EdgeSup, int[]> tSet = candidates.cogroup(fonl)
                .flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;

            List<Tuple2<Edge, OrderedVertex>> output = new ArrayList<>();
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

                        output.add(new Tuple2<>(uv, new OrderedVertex(w, W_UVW)));
                        output.add(new Tuple2<>(uw, new OrderedVertex(v, V_UVW)));
                        output.add(new Tuple2<>(vw, new OrderedVertex(u, U_UVW)));

                        fi++;
                        ci++;
                    }
                }
            }

            return output.iterator();
        }).groupByKey(partitionNum * 2)
                .mapValues(values -> {
                    List<OrderedVertex> list = new ArrayList<>();
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
                }).mapToPair(kv -> new Tuple2<>(new EdgeSup(kv._1.v1, kv._1.v2, kv._2.length), kv._2))
                .persist(StorageLevel.MEMORY_AND_DISK()); // Use disk too because this RDD often is very large

        return tSet;
    }

    public static void main(String[] args) throws URISyntaxException {
        Logger.getLogger("org.apache.spar").setLevel(Level.INFO);
        long t1 = System.currentTimeMillis();

        KTrussConf ktConf = new KTrussConf(new ArgumentReader(args));
        ktConf.init();

        EdgeLoader edgeLoader = new EdgeLoader(ktConf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        KTrussTSetPartition kTrussTSet = new KTrussTSetPartition(neighborList, ktConf, 1024*1024*500);
        JavaRDD <EdgeSup> edgeTruss = kTrussTSet.generate();
        log("KTruss edge count: " + edgeTruss.count(), t1, System.currentTimeMillis());

        kTrussTSet.close();
    }
}

