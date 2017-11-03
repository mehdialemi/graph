package ir.ac.sbu.graph.spark;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

import static ir.ac.sbu.graph.utils.Log.log;

/**
 * Generate ktruss in 2 phase:
 * 1: Create triangle set using {@link Triangle}
 * 2: Iteratively, prune invalid edges which have not enough support
 */
public class KTrussTSet extends SparkApp {
    private static final Byte W_UVW = (byte) 0;
    private static final Byte V_UVW = (byte) 1;
    private static final Byte U_UVW = (byte) 2;
    public static final int META_LEN = 4;
    public static final int INVALID = -1;

    private final NeighborList neighborList;
    private final int k;
    private final int partition;
    private KConf kConf;

    public KTrussTSet(NeighborList neighborList, KConf kConf) {
        super(neighborList);
        this.neighborList = neighborList;
        this.k = kConf.getK();
        this.partition = kConf.getPartitionNum() * 3;
        this.kConf = kConf;
    }

    public JavaPairRDD<Tuple2<Integer, Integer>, int[]> generate() {

        NeighborList kCore = new KCore(neighborList, kConf.create(k - 1, 1));

        Triangle triangle = new Triangle(kCore);
        JavaPairRDD<Integer, int[]> fonl = triangle.createFonl();
        JavaPairRDD<Integer, int[]> candidates = triangle.generateCandidates(fonl)
                .repartition(partition)
                .persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<Tuple2<Integer, Integer>, int[]> tSet = createTSet(fonl, candidates);

        final int minSup = k - 2;
        Queue<JavaPairRDD<Tuple2<Integer, Integer>, int[]>> tSetQueue = new LinkedList<>();
        tSetQueue.add(tSet);

        for (int iter = 0; iter < kConf.getMaxIter(); iter ++) {

            long t1 = System.currentTimeMillis();

            if (iter == 1) {
                candidates.unpersist();
                fonl.unpersist();
            }
            
            // Detect invalid edges by comparing the size of triangle vertex set
            JavaPairRDD<Tuple2<Integer, Integer>, int[]> invalids = tSet.filter(kv -> kv._2[0] < minSup).cache();
            long invalidCount = invalids.count();

            // If no invalid edge is found then the program terminates
            if (invalidCount == 0) {
                break;
            }

            if (tSetQueue.size() > 1)
                tSetQueue.remove().unpersist();


            long t2 = System.currentTimeMillis();
            String msg = "iteration: " + (iter + 1) + ", invalid edge count: " + invalidCount;
            log(msg, t2 - t1);

            // The edges in the key part of invalids key-values should be removed. So, we detect other
            // edges of their involved triangle from their triangle vertex set. Here, we determine the
            // vertices which should be removed from the triangle vertex set related to the other edges.
            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> invUpdates = invalids.flatMapToPair(kv -> {
                int i = META_LEN;

                Tuple2<Integer, Integer> e = kv._1;
                List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>((kv._2.length - 3) * 2);
                for (; i < kv._2[1]; i++) {
                    if (kv._2[i] == INVALID)
                        continue;
                    out.add(new Tuple2<>(new Tuple2<>(e._1, kv._2[i]), e._2));
                    out.add(new Tuple2<>(new Tuple2<>(e._2, kv._2[i]), e._1));
                }

                for (; i < kv._2[2]; i++) {
                    if (kv._2[i] == INVALID)
                        continue;
                    out.add(new Tuple2<>(new Tuple2<>(e._1, kv._2[i]), e._2));
                    out.add(new Tuple2<>(new Tuple2<>(kv._2[i], e._2), e._1));
                }

                for (; i < kv._2[3]; i++) {
                    if (kv._2[i] == INVALID)
                        continue;
                    out.add(new Tuple2<>(new Tuple2<>(kv._2[i], e._1), e._2));
                    out.add(new Tuple2<>(new Tuple2<>(kv._2[i], e._2), e._1));
                }

                return out.iterator();
            }).groupByKey(partition);

            // Remove the invalid vertices from the triangle vertex set of each remaining (valid) edge.
            tSet = tSet.filter(kv -> kv._2[0] >= minSup).leftOuterJoin(invUpdates)
                    .mapValues(values -> {
                        org.apache.spark.api.java.Optional<Iterable<Integer>> invalidUpdate = values._2;
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

            invalids.unpersist();
            tSetQueue.add(tSet);
        }

        return tSet;
    }

    private JavaPairRDD<Tuple2<Integer, Integer>, int[]> createTSet(JavaPairRDD<Integer, int[]> fonl, JavaPairRDD<Integer, int[]> candidates) {

        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD<Tuple2<Integer, Integer>, int[]> tSet = candidates.cogroup(fonl).flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;

            List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Byte>>> output = new ArrayList<>();
            for (int[] cVal : t._2._1) {
                int u = cVal[0];
                Tuple2<Integer, Integer> uv = new Tuple2<>(u, v);

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
                        Tuple2<Integer, Integer> uw = new Tuple2<>(u, w);
                        Tuple2<Integer, Integer> vw = new Tuple2<>(v, w);

                        output.add(new Tuple2<>(uv, new Tuple2<>(w, W_UVW)));
                        output.add(new Tuple2<>(uw, new Tuple2<>(v, V_UVW)));
                        output.add(new Tuple2<>(vw, new Tuple2<>(u, U_UVW)));

                        fi++;
                        ci++;
                    }
                }
            }

            return output.iterator();
        }).groupByKey(partition)
                .mapValues(values -> {
                    List<Tuple2<Integer, Byte>> list = new ArrayList<>();
                    int sw = 0, sv = 0, su = 0;
                    for (Tuple2<Integer, Byte> value : values) {
                        list.add(value);
                        if (value._2.equals(W_UVW))
                            sw++;
                        else if (value._2.equals(V_UVW))
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

                    for (Tuple2<Integer, Byte> v : list) {
                        if (v._2.equals(W_UVW))
                            set[offsetW++] = v._1;
                        else if (v._2.equals(V_UVW))
                            set[offsetV++] = v._1;
                        else
                            set[offsetU++] = v._1;
                    }

                    return set;
                }).persist(StorageLevel.MEMORY_AND_DISK()); // Use disk too if graph is very large

        return tSet;
    }

    public static void main(String[] args) {
        long t1 = System.currentTimeMillis();
        KConf kConf = new KConf(new ArgumentReader(args), "KTruss");
        EdgeLoader edgeLoader = new EdgeLoader(kConf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        KTrussTSet kTrussTSet = new KTrussTSet(neighborList, kConf);
        JavaPairRDD<Tuple2<Integer, Integer>, int[]> subgraph = kTrussTSet.generate();
        log("KTruss edge count: " + subgraph.count(), t1, System.currentTimeMillis());

        kTrussTSet.close();
    }
}
