package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import ir.ac.sbu.graph.types.VSign;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
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
public class KTrussTSet extends SparkApp {
    private static final int INVALID = -1;
    private static final int META_LEN = 4;
    private static final byte W_SIGN = (byte) 0;
    private static final byte V_SIGN = (byte) 1;
    private static final byte U_SIGN = (byte) 2;
    public static final int CHECKPOINT_ITERATION = 50;

    private final NeighborList neighborList;
    private final int k;
    private KTrussConf ktConf;

    public KTrussTSet(NeighborList neighborList, KTrussConf ktConf) throws URISyntaxException {
        super(neighborList);
        this.neighborList = neighborList;
        this.k = ktConf.getKt();
        this.ktConf = ktConf;
        String master = ktConf.getSc().master();
        if (master.contains("local"))
            return;
        String masterHost = new URI(ktConf.getSc().master()).getHost();
        this.ktConf.getSc().setCheckpointDir("hdfs://" + masterHost + "/shared/checkpoint");
    }

    public JavaPairRDD <Edge, int[]> generate() throws URISyntaxException {

        JavaPairRDD <Edge, int[]> tSet = createTSet();
        int numPartitions = tSet.getNumPartitions();

        final int minSup = k - 2;
        Queue <JavaPairRDD <Edge, int[]>> tSetQueue = new LinkedList <>();
        Queue <JavaPairRDD <Edge, int[]>> invQueue = new LinkedList <>();
        tSetQueue.add(tSet);
        long kTrussDuration = 0;
        int invalidsCount = 0;
        for (int iter = 0; iter < ktConf.getKtMaxIter(); iter++) {

            long t1 = System.currentTimeMillis();

            if ((iter + 1) % CHECKPOINT_ITERATION == 0) {
                tSet.checkpoint();
            }

            // Detect invalid edges by comparing the support of triangle vertex set
            JavaPairRDD <Edge, int[]> invalids = tSet.filter(kv -> kv._2[0] < minSup).cache();
            long invalidCount = invalids.count();

            // If no invalid edge is found then the program terminates
            if (invalidCount == 0) {
                break;
            }

            invalidsCount += invalidCount;

            if (tSetQueue.size() > 1)
                tSetQueue.remove().unpersist();
            if (invQueue.size() > 1)
                invQueue.remove().unpersist();


            invQueue.add(invalids);

            long t2 = System.currentTimeMillis();
            String msg = "iteration: " + (iter + 1) + ", invalid edge count: " + invalidCount;
            long iterDuration = t2 - t1;
            kTrussDuration += iterDuration;
            log(msg, iterDuration);

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
            }).groupByKey(numPartitions);

            // Remove the invalid vertices from the triangle vertex set of each remaining (valid) edge.
            tSet = tSet.filter(kv -> kv._2[0] >= minSup).leftOuterJoin(invUpdates)
                    .mapValues(values -> {
                        org.apache.spark.api.java.Optional <Iterable <Integer>> invalidUpdate = values._2;
                        int[] set = values._1;

                        // If no invalid vertex is present for the current edge then return the set value.
                        if (!invalidUpdate.isPresent()) {
                            return set;
                        }

                        IntSet iSet = new IntOpenHashSet();
                        for (int v : invalidUpdate.get()) {
                            iSet.add(v);
                        }

                        for (int i = META_LEN; i < set.length; i++) {
                            if (set[i] == INVALID)
                                continue;
                            if (iSet.contains(set[i])) {
                                set[0]--;
                                set[i] = INVALID;
                            }
                        }

                        // When the triangle vertex iSet has no other element then the current edge should also
                        // be eliminated from the current tvSets.
                        if (set[0] <= 0)
                            return null;

                        return set;
                    }).filter(kv -> kv._2 != null)
                    .persist(StorageLevel.MEMORY_AND_DISK());

            tSetQueue.add(tSet);
        }

        log("kTruss duration: " + kTrussDuration + ", invalids: " + invalidsCount);
        return tSet;
    }

    protected JavaPairRDD <Edge, int[]> createTSet() throws URISyntaxException {
        KCore kCore = new KCore(neighborList, ktConf);

        JavaPairRDD <Integer, int[]> neighbors = neighborList.getOrCreate();

        JavaPairRDD <Integer, int[]> kcNeighbors = kCore.perform(neighbors, ktConf.getKc());

        Triangle triangle = new Triangle(this);

        JavaPairRDD <Integer, int[]> fonl = triangle.createFonl(kcNeighbors);

        JavaPairRDD <Integer, int[]> candidates = fonl.filter(t -> t._2.length > 2)
                .flatMapToPair(t -> {

                    int size = t._2.length - 1; // one is for the first index holding node's degree

                    if (size == 1)
                        return Collections.emptyIterator();

                    List <Tuple2 <Integer, int[]>> output;
                    output = new ArrayList <>(size);

                    for (int index = 1; index < size; index++) {
                        int len = size - index;
                        int[] cvalue = new int[len + 1];
                        cvalue[0] = t._1; // First vertex in the triangle
                        System.arraycopy(t._2, index + 1, cvalue, 1, len);
                        Arrays.sort(cvalue, 1, cvalue.length); // quickSort to comfort with fonl
                        output.add(new Tuple2 <>(t._2[index], cvalue));
                    }

                    return output.iterator();
                });

        // Generate kv such that key is an edge and value is its triangle vertices.
        return fonl.cogroup(candidates, conf.getPartitionNum() * 2)
                .flatMapToPair(t -> {

                    int[] fVal = t._2._1.iterator().next();
                    Arrays.sort(fVal, 1, fVal.length);
                    int v = t._1;

                    Map <Edge, List <VSign>> map = new HashMap <>();

                    for (int[] cVal : t._2._2) {
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

                                map.computeIfAbsent(uv, k -> new ArrayList <>()).add(new VSign(w, W_SIGN));
                                map.computeIfAbsent(uw, k -> new ArrayList <>()).add(new VSign(v, V_SIGN));
                                map.computeIfAbsent(vw, k -> new ArrayList <>()).add(new VSign(u, U_SIGN));

                                fi++;
                                ci++;
                            }
                        }
                    }

                    return map.entrySet()
                            .stream()
                            .map(entry -> {
                                List <VSign> value = entry.getValue();
                                int[] vertices = new int[value.size()];
                                byte[] signs = new byte[value.size()];
                                for (int i = 0; i < value.size(); i++) {
                                    VSign vSign = value.get(i);
                                    vertices[i] = vSign.vertex;
                                    signs[i] = vSign.sign;
                                }
                                return new Tuple2 <>(entry.getKey(), new Tuple2<>(vertices, signs));
                            })
                            .iterator();

                }).groupByKey()
                .mapValues(values -> {
                    IntList wList = new IntArrayList();
                    IntList vList = new IntArrayList();
                    IntList uList = new IntArrayList();

                    for (Tuple2 <int[], byte[]> value : values) {
                        int[] vertices = value._1;
                        byte[] signs = value._2;
                        for (int i = 0; i < vertices.length; i++) {
                            switch (signs[i]) {
                                case W_SIGN: wList.add(vertices[i]); break;
                                case V_SIGN: vList.add(vertices[i]); break;
                                case U_SIGN: uList.add(vertices[i]); break;
                            }
                        }
                    }
                    int offsetW = META_LEN;
                    int offsetV = wList.size() + META_LEN;
                    int offsetU = wList.size() + vList.size() + META_LEN;
                    int len = wList.size() + vList.size() + uList.size();
                    int[] set = new int[META_LEN + len];
                    set[0] = len;  // support of edge
                    set[1] = offsetW + wList.size();  // exclusive max offset of w
                    set[2] = offsetV + vList.size();  // exclusive max offset of vertex
                    set[3] = offsetU + uList.size();  // exclusive max offset of u

                    System.arraycopy(wList.toIntArray(), 0, set, offsetW, wList.size());
                    System.arraycopy(vList.toIntArray(), 0, set, offsetV, vList.size());
                    System.arraycopy(uList.toIntArray(), 0, set, offsetU, uList.size());

                    return set;
                }).persist(StorageLevel.MEMORY_AND_DISK());
    }

    public static void main(String[] args) throws URISyntaxException {
        long t1 = System.currentTimeMillis();

        KTrussConf ktConf = new KTrussConf(new ArgumentReader(args));
        ktConf.init();

        EdgeLoader edgeLoader = new EdgeLoader(ktConf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        KTrussTSet kTrussTSet = new KTrussTSet(neighborList, ktConf);
        JavaPairRDD <Edge, int[]> subgraph = kTrussTSet.generate();
        long t2 = System.currentTimeMillis();
        log("KTruss edge count: " + subgraph.count(), t1, t2);

        kTrussTSet.close();
    }
}

