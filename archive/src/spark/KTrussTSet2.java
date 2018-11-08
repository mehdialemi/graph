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
public class KTrussTSet2 extends SparkApp {

    private final NeighborList neighborList;
    private final int k;
    private final int partitionNum;
    private KCoreConf kConf;

    public KTrussTSet2(NeighborList neighborList, KCoreConf kConf) {
        super(neighborList);
        this.neighborList = neighborList;
        this.k = kConf.getKc();
        this.partitionNum = kConf.getPartitionNum() * 5;
        this.kConf = kConf;
    }

    public JavaPairRDD<Edge, int[]> generate() {

        NeighborList kCore = new KCore(neighborList, kConf.create(k - 1, 1));

        Triangle triangle = new Triangle(kCore);
        JavaPairRDD<Integer, int[]> fonl = triangle.createFonl(partitionNum);
        JavaPairRDD<Integer, int[]> candidates = triangle.generateCandidates(fonl)
                .persist(StorageLevel.MEMORY_AND_DISK());

        JavaPairRDD<Edge, int[]> tSet = createTSet(fonl, candidates);

        final int minSup = k - 2;
        Queue<JavaPairRDD<Edge, int[]>> tSetQueue = new LinkedList<>();
        tSetQueue.add(tSet);

        for (int iter = 0; iter < kConf.getKcMaxIter(); iter ++) {

            long t1 = System.currentTimeMillis();

            if (iter == 1) {
                candidates.unpersist();
                fonl.unpersist();
            }

            // Detect invalid edges by comparing the size of triangle vertex set
            JavaPairRDD<Edge, int[]> invalids = tSet.filter(kv -> kv._2.length < minSup).cache();
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
            JavaPairRDD<Edge, Iterable<Integer>> invUpdates = invalids.flatMapToPair(kv -> {
                List<Tuple2<Edge, Integer>> out = new ArrayList<>(kv._2.length * 2);
                int u = kv._1.v1;
                int v = kv._1.v2;

                Edge uw;
                Edge vw;
                for (int w : kv._2) {
                    if (v < w) {
                        uw = new Edge(u, w);
                        vw = new Edge(v, w);
                    } else if (u > w) {
                        uw = new Edge(w, u);
                        vw = new Edge(w, v);
                    } else {
                        uw = new Edge(u, w);
                        vw = new Edge(w, v);
                    }

                    out.add(new Tuple2<>(uw, v));
                    out.add(new Tuple2<>(vw, u));
                }

                return out.iterator();
            }).groupByKey(partitionNum);

            // Remove the invalid vertices from the triangle vertex set of each remaining (valid) edge.
            tSet = tSet.filter(kv -> kv._2.length >= minSup)
                    .leftOuterJoin(invUpdates)
                    .mapValues(values -> {

                        if (!values._2.isPresent())
                            return values._1;

                        IntSet set = new IntOpenHashSet(values._1);
                        for (int inv : values._2.get()) {
                            set.remove(inv);
                        }

                        if (set.size() == 0)
                            return null;

                        return set.toIntArray();
                    }).filter(kv -> kv._2 != null)
                    .persist(StorageLevel.MEMORY_AND_DISK());

            invalids.unpersist();
            tSetQueue.add(tSet);
        }

        return tSet;
    }

    private JavaPairRDD<Edge, int[]> createTSet(JavaPairRDD<Integer, int[]> fonl, JavaPairRDD<Integer, int[]> candidates) {

        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD<Edge, int[]> tSet = candidates.cogroup(fonl, partitionNum).flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;

            List<Tuple2<Edge, Integer>> output = new ArrayList<>();
            for (int[] cVal : t._2._1) {
                int u = cVal[0];
                Edge uv;
                Edge uw;
                Edge vw;

                if (u < v)
                    uv = new Edge(u, v);
                else
                    uv = new Edge(v, u);

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

                        if (u < w)
                            uw = new Edge(u, w);
                        else
                            uw = new Edge(w, u);

                        if (v < w)
                            vw = new Edge(v, w);
                        else
                            vw = new Edge(w, v);

                        output.add(new Tuple2<>(uv, w));
                        output.add(new Tuple2<>(uw, v));
                        output.add(new Tuple2<>(vw, u));

                        fi++;
                        ci++;
                    }
                }
            }

            return output.iterator();
        }).groupByKey(partitionNum)
                .mapValues(values -> {
                    IntSet list = new IntOpenHashSet();
                    for (int w : values) {
                        list.add(w);
                    }

                    return list.toIntArray();
                }).persist(StorageLevel.DISK_ONLY()); // Use disk too if graph is very large

        return tSet;
    }

    public static void main(String[] args) {
        long t1 = System.currentTimeMillis();
        KCoreConf kConf = new KCoreConf(new ArgumentReader(args), "KTruss2");
        EdgeLoader edgeLoader = new EdgeLoader(kConf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        KTrussTSet2 kTrussTSet = new KTrussTSet2(neighborList, kConf);
        JavaPairRDD<Edge, int[]> subgraph = kTrussTSet.generate();
        log("KTruss edge count: " + subgraph.count(), t1, System.currentTimeMillis());

        kTrussTSet.close();
    }
}
