package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.spark.ArgumentReader;
import ir.ac.sbu.graph.spark.EdgeLoader;
import ir.ac.sbu.graph.spark.NeighborList;
import ir.ac.sbu.graph.spark.SparkApp;
import ir.ac.sbu.graph.spark.kcore.KCore;
import ir.ac.sbu.graph.spark.triangle.Triangle;
import ir.ac.sbu.graph.types.Edge;
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
    private final int k;
    private KTrussConf ktConf;

    public AllTruss(NeighborList neighborList, KTrussConf ktConf) throws URISyntaxException {
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

    public JavaPairRDD <Edge, Integer> generate() throws URISyntaxException {

        KCore kCore = new KCore(neighborList, ktConf);

        Triangle triangle = new Triangle(kCore);

        JavaPairRDD<Integer, int[]> fonl = triangle.getOrCreateFonl();

        JavaPairRDD<Integer, int[]> candidates = triangle.createCandidates(fonl);

        int partitionNum = fonl.getNumPartitions();

        JavaPairRDD <Edge, Integer> triangles = createTriangles(fonl, candidates).cache();

        JavaPairRDD <Edge, Edge> edges = triangles.flatMapToPair(kv -> {
            Edge e = kv._1;
            List <Tuple2 <Edge, Edge>> out = new ArrayList <>();
            out.add(new Tuple2 <>(new Edge(e.v1, kv._2), e));
            out.add(new Tuple2 <>(new Edge(e.v2, kv._2), e));
            out.add(new Tuple2 <>(e, e));
            return out.iterator();
        });

        JavaPairRDD <Edge, Integer> tSup = edges.groupByKey().flatMapToPair(kv -> {
            Map<Tuple2<Integer, Integer>, Integer> map = new HashMap <>();
            int sup = 0;
            for (Edge tEdge : kv._2) {
                map.compute(new Tuple2<>(tEdge.v1, tEdge.v2), (k, v) -> v == null ? 1 : v + 1);
                sup ++;
            }

            List <Tuple2 <Edge, Integer>> out = new ArrayList <>();

            for (Map.Entry <Tuple2 <Integer, Integer>, Integer> entry : map.entrySet()) {
                out.add(new Tuple2 <>(new Edge(entry.getKey()._1, entry.getKey()._2), sup));
            }

            return out.iterator();
        }).reduceByKey(Math::min).cache();

        return tSup;
//        return tSup.join(triangles).flatMapToPair(kv -> {
//            Edge e = kv._1;
//            int sup = kv._2._1;
//            int w = kv._2._2;
//            List <Tuple2 <Edge, Integer>> out = new ArrayList <>();
//            out.add(new Tuple2 <>(new Edge(e.v1, w), sup));
//            out.add(new Tuple2 <>(new Edge(e.v2, w), sup));
//            out.add(new Tuple2 <>(e, sup));
//            return out.iterator();
//        }).reduceByKey(Math::max);

    }

    private JavaPairRDD<Edge, Integer> createTriangles(JavaPairRDD<Integer, int[]> fonl,
                                                JavaPairRDD<Integer, int[]> candidates) {
        return candidates.cogroup(fonl)
                .flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;

            List<Tuple2<Edge, Integer>> output = new ArrayList<>();
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

                        output.add(new Tuple2<>(uv, w));

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

