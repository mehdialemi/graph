package ir.ac.sbu.graph.ktruss.others;

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
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class KTrussSparkIntervalByConstant {

    public static final IntOpenHashSet EMPTY_INT_SET = new IntOpenHashSet(1);

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String graphInputPath = "/home/mehdi/graph-data/cit-Patents.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 10;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);
        final int minSup = k - 2;

        int constant = 1;
        if (args.length > 3)
            constant = Integer.parseInt(args[3]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "KTruss-EdgeVertexList-" + k + "-MultiSteps", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, long[].class, List.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long start = System.currentTimeMillis();
        Partitioner partitionerSmall = new HashPartitioner(partition);
        Partitioner partitionerBig = new HashPartitioner(partition * 10);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partitionerSmall);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl).partitionBy(partitionerBig);

        Partitioner partitioner = new HashPartitioner(partition);

        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> edgeVertices = candidates.cogroup(fonl).flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;
            List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>();
            for (int[] cVal : t._2._1) {
                int u = cVal[0];

                // The intersection determines triangles which u and vertex are two of their vertices.
                IntList wList = GraphUtils.sortedIntersectionTest(fVal, 1, cVal, 1);
                if (wList == null)
                    continue;

                // Always generate and edge (u, vertex) such that u < vertex.
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

        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> prevEdgeVertices = edgeVertices;

        int iteration = 0;
        boolean stop = false;

        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> empty = sc.emptyRDD().mapToPair(t -> new Tuple2<>(new Tuple2<>(0, 0), EMPTY_INT_SET));

        while (!stop) {
            final int maxSup = minSup + constant;
            log("iteration: " + ++iteration + ", support: " + maxSup + ", minSup: " + minSup);

            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> partialEdgeVertices = edgeVertices.filter(e -> e._2.size() < maxSup)
                .partitionBy(partitioner).cache();

            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> prevPartialEdgeVertices = partialEdgeVertices;
            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> toRemoveEdges = empty;

            int currStepCount = 0;
            long t1 = System.currentTimeMillis();
            long t2;
            while (!stop) {
                JavaPairRDD<Tuple2<Integer, Integer>, IntSet> invalids = partialEdgeVertices.filter(en -> en._2.size() < minSup).cache();
                long invalidEdgeCount = invalids.count();
                log("invalid edge count: " + invalidEdgeCount);
                if (invalidEdgeCount == 0) {
                    if (currStepCount == 0)
                        stop = true;
                    break;
                } else {
                    t2 = System.currentTimeMillis();
                }

                long currDuration = (t2 - t1);
                logDuration("step: " + currStepCount, currDuration);
                t1 = t2;

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
                }).groupByKey().partitionBy(partitioner).cache();

                JavaPairRDD<Tuple2<Integer, Integer>, IntSet> notInPartial = invalidUpdates.subtractByKey(partialEdgeVertices)
                    .mapValues(value -> {
                        IntSet set = new IntOpenHashSet();
                        for (Integer v : value) {
                            set.add(v.intValue());
                        }
                        return set;
                    }).cache();

                toRemoveEdges = toRemoveEdges.union(notInPartial);

                partialEdgeVertices = partialEdgeVertices.leftOuterJoin(invalidUpdates, partitioner).mapValues(values -> {
                        Optional<Iterable<Integer>> invalidUpdate = values._2;
                        IntSet original = values._1;

                        if (original.size() < minSup)
                            return null;

                        if (!invalidUpdate.isPresent())
                            return original;


                        for (Integer v : invalidUpdate.get()) {
                            original.remove(v.intValue());
                        }

                        if (original.size() == 0)
                            return null;

                        return original;
                    }).filter(kv -> kv._2 != null).partitionBy(partitioner).cache();

                prevPartialEdgeVertices.unpersist();
                currStepCount++;
            }

            System.out.println("Count of toRemove: " + toRemoveEdges.count());

            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> nextEdgeNodes = edgeVertices
                .filter(t -> t._2.size() >= maxSup)
                .cogroup(toRemoveEdges).flatMapToPair(t -> {
                    Iterator<IntSet> it = t._2._1.iterator();
                    if (!it.hasNext())
                        return Collections.emptyIterator();

                    IntSet vertices = it.next();

                    for (IntSet set : t._2._2) {
                        vertices.removeAll(set);
                    }

                    if (vertices.size() == 0)
                        return Collections.emptyIterator();

                    return Collections.singleton(new Tuple2<>(t._1, vertices)).iterator();
                }).cogroup(partialEdgeVertices, partitioner).mapValues(t -> {
                    Iterator<IntSet> it = t._2.iterator();
                    if (it.hasNext())
                        return it.next();
                    return t._1.iterator().next();
                }).partitionBy(partitioner).cache();

            edgeVertices.unpersist();
            edgeVertices = nextEdgeNodes;
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
