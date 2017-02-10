package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class KTrussSparkEdgeVertices {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 10;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);
        final int minSup = k - 2;

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "KTruss-EdgeVertexList-" + k + "-MultiSteps", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, long[].class, List.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long start = System.currentTimeMillis();

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partition);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl);

        Partitioner partitioner = new HashPartitioner(partition);

        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> edgeVertices = candidates.cogroup(fonl).flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;
            List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>();
            for (int[] cVal : t._2._1) {
                int u = cVal[0];

                // The intersection determines triangles which u and v are two of their vertices.
                IntList wList = GraphUtils.sortedIntersectionTest(fVal, 1, cVal, 1);
                if (wList == null)
                    continue;

                // Always generate and edge (u, v) such that u < v.
                Tuple2<Integer, Integer> uv;
                if (u < v)
                    uv = new Tuple2<>(u, v);
                else
                    uv = new Tuple2<>(v, u);

                IntListIterator wIter = wList.iterator();
                while (wIter.hasNext()) {
                    int w = wIter.nextInt();
                    output.add(new Tuple2<>(uv, w));
                    if (u < w)
                        output.add(new Tuple2<>(new Tuple2<>(u, w), v));
                    else
                        output.add(new Tuple2<>(new Tuple2<>(w, u), v));

                    if (v < w)
                        output.add(new Tuple2<>(new Tuple2<>(v, w), u));
                    else
                        output.add(new Tuple2<>(new Tuple2<>(w, v), u));
                }
            }

            return output.iterator();
        }).groupByKey().partitionBy(partitioner)
            .mapValues(values -> {
                // TODO use iterator here instead of creating a set and filling it
                IntSet set = new IntOpenHashSet();
                for (int v : values) {
                    set.add(v);
                }
                return set;
            }).persist(StorageLevel.MEMORY_ONLY()); // Use disk too if graph is very large

//        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> prevEdgeVertices = edgeVertices;

        int iteration = 0;
        boolean stop = false;

        while (!stop) {
            iteration++;
            long t1 = System.currentTimeMillis();
            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> invalids = edgeVertices.filter(kv -> kv._2.size() < minSup);
            long invalidEdgeCount = invalids.count();
            if (invalidEdgeCount == 0) {
                break;
            }
            long t2 = System.currentTimeMillis();
            String msg = "iteration: " + iteration + ", invalid edge count: " + invalidEdgeCount;
            logDuration(msg, t2 - t1);

            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> invalidUpdates = invalids.flatMapToPair(kv -> {
                IntSet original = kv._2;
                List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>(original.size() * 2);
                Tuple2<Integer, Integer> iEdge = kv._1;

                for (Integer w : kv._2) {
                    if (w < iEdge._1)
                        out.add(new Tuple2<>(new Tuple2<>(w, iEdge._1), iEdge._2));
                    else
                        out.add(new Tuple2<>(new Tuple2<>(iEdge._1, w), iEdge._2));

                    if (w < iEdge._2)
                        out.add(new Tuple2<>(new Tuple2<>(w, iEdge._2), iEdge._1));
                    else
                        out.add(new Tuple2<>(new Tuple2<>(iEdge._2, w), iEdge._1));
                }
                return out.iterator();
            }).groupByKey().partitionBy(partitioner);

            edgeVertices = edgeVertices.filter(kv -> kv._2.size() >= minSup).leftOuterJoin(invalidUpdates)
                .mapValues(values -> {
                    Optional<Iterable<Integer>> invalidUpdate = values._2;
                    IntSet original = values._1;

                    if (!invalidUpdate.isPresent()) {
                        return original;
                    }

                    for (Integer v : invalidUpdate.get()) {
                        original.remove(v.intValue());
                    }

                    if (original.size() == 0)
                        return null;

                    return original;
                }).filter(kv -> kv._2 != null).partitionBy(partitioner).cache();
        }

        long duration = System.currentTimeMillis() - start;
        long edgeCount = edgeVertices.count();

        logDuration("KTruss Edge Count: " + edgeCount, duration);
        sc.close();
    }

    static void log(String text) {
        System.out.println("KTRUSS [" + new Date() + "] " + text);
    }

    static void logDuration(String text, long millis) {
        log(text + " (" + millis / 1000 + " sec)");
    }
}
