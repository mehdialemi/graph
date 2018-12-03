package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.kcore.KCoreConf;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
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
public class AllTruss extends SparkApp {
    private static final int INVALID = -1;
    private static final int META_LEN = 4;
    private static final byte W_UVW = (byte) 0;
    private static final byte V_UVW = (byte) 1;
    private static final byte U_UVW = (byte) 2;
    public static final int CHECKPOINT_ITERATION = 50;

    private final NeighborList neighborList;
    private final KCoreConf kCoreConf;
    private KTrussConf ktConf;

    public AllTruss(NeighborList neighborList, KTrussConf ktConf) throws URISyntaxException {
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

    public JavaPairRDD <Edge, Integer> generate() {

        KCore kCore = new KCore(neighborList, kCoreConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD <Integer, int[]> fonl = triangle.getOrCreateFonl();

        JavaPairRDD <Integer, int[]> candidates = triangle.createCandidates(fonl);

        int partitionNum = fonl.getNumPartitions();
        JavaPairRDD <Edge, Tuple2 <Integer, Integer>>  fTriangles = conf.getSc().parallelizePairs(new ArrayList <>());

        JavaPairRDD <Edge, Integer> triangles = createTriangles(fonl, candidates).repartition(partitionNum).cache();

        while (true) {
            JavaPairRDD <Edge, Edge> edgeTriangles = triangles.flatMapToPair(kv -> {
                Edge e = kv._1;
                List <Tuple2 <Edge, Edge>> out = new ArrayList <>();
                out.add(new Tuple2 <>(new Edge(e.v1, kv._2), e));
                out.add(new Tuple2 <>(new Edge(e.v2, kv._2), e));
                out.add(new Tuple2 <>(e, e));
                return out.iterator();
            });

            JavaPairRDD <Edge, Tuple2 <Integer, Boolean>> triangleSupState = edgeTriangles.groupByKey().flatMapToPair(kv -> {

                Set <Edge> set = new TreeSet <>((e1, e2) -> e1.equals(e2) ? 0 : 1);
                int sup = 0;
                for (Edge tEdge : kv._2) {
                    set.add(tEdge);
                    sup ++;
                }
                List <Tuple2 <Edge, Integer>> out = new ArrayList <>();
                for (Edge edge : set) {
                    out.add(new Tuple2 <>(edge, sup));
                }
                return out.iterator();
            }).groupByKey(partitionNum).mapValues(values -> {
                int min = 0;
                boolean same = true;
                for (Integer support : values) {
                    if (min == 0) {
                        min = support;
                        continue;
                    }

                    if (support < min) {
                        min = support;
                        same = false;
                    }
                }
                return new Tuple2 <>(min, same);
            }).cache();

            JavaPairRDD <Edge, Tuple2 <Tuple2 <Integer, Boolean>, Integer>> result = triangleSupState.join(triangles);

            JavaPairRDD <Edge, Tuple2 <Integer, Integer>> exclude = result.filter(kv -> kv._2._1._2)
                    .mapValues(values -> new Tuple2 <>(values._1._1, values._2))
                    .repartition(partitionNum)
                    .cache(); // sup, w

            fTriangles = fTriangles.union(exclude);

            triangles = triangles.subtractByKey(exclude).cache();

            long count = triangles.count();
            log("count: " + count);
            if (count == 0)
                break;
        }

        return fTriangles.flatMapToPair(kv -> {
            Edge e = kv._1;

            int sup = kv._2._1;
            int w = kv._2._2;
            List <Tuple2 <Edge, Integer>> out = new ArrayList <>();
            out.add(new Tuple2 <>(new Edge(e.v1, w), sup));
            out.add(new Tuple2 <>(new Edge(e.v2, w), sup));
            out.add(new Tuple2 <>(e, sup));
            return out.iterator();
        }).reduceByKey(Math::max);
    }

    private JavaPairRDD <Edge, Integer> createTriangles(JavaPairRDD <Integer, int[]> fonl,
                                                        JavaPairRDD <Integer, int[]> candidates) {
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

                                output.add(new Tuple2 <>(uv, w));

                                fi++;
                                ci++;
                            }
                        }
                    }

                    return output.iterator();
                });
    }

    public static void main(String[] args) throws URISyntaxException {
        Logger.getLogger("org.apache.spar").setLevel(Level.INFO);
        long t1 = System.currentTimeMillis();

        KTrussConf ktConf = new KTrussConf(new ArgumentReader(args));
        ktConf.init();

        EdgeLoader edgeLoader = new EdgeLoader(ktConf);
        NeighborList neighborList = new NeighborList(edgeLoader);

        AllTruss kTrussTSet = new AllTruss(neighborList, ktConf);
        JavaPairRDD <Edge, Integer> maxTruss = kTrussTSet.generate();
        long t2 = System.currentTimeMillis();
        log("KTruss edge count: " + maxTruss.count(), t1, t2);

        printKCount(maxTruss, 1000);

        kTrussTSet.close();
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

