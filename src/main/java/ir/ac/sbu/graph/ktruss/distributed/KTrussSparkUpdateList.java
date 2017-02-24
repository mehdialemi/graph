package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

public class KTrussSparkUpdateList {

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
        GraphUtils.setAppName(conf, "KTrussSparkUpdateList-" + k + "-MultiSteps", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, int[].class, Iterable.class, IntSet.class, IntOpenHashSet.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long tStart = System.currentTimeMillis();

        Partitioner partitionerSmall = new HashPartitioner(partition);
        Partitioner partitionerBig = new HashPartitioner(partition * 3);
        Partitioner partitioner = new HashPartitioner(partition);
        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partitioner);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl).partitionBy(partitionerBig);

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
        }).groupByKey(partitionerBig)
            .mapValues(values -> {
                // TODO use iterator here instead of creating a set and filling it
                IntSet set = new IntOpenHashSet();
                for (int v : values) {
                    set.add(v);
                }
                return set;
            }).persist(StorageLevel.MEMORY_ONLY()); // Use disk too if graph is very large

        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> invalids = edgeVertices.filter(t -> t._2.size() < minSup).cache();

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> updates =
            sc.parallelize(new ArrayList<Tuple2<Tuple2<Integer, Integer>, Integer>>()).mapToPair(t -> new Tuple2<>(t._1, t._2));

        int iteration = 0;
        while (true) {
            long invCount = invalids.count();
            if (invCount == 0)
                break;
            System.out.println("iteration: " + ++ iteration + ", invalids: " + invCount);

            JavaPairRDD<Tuple2<Integer, Integer>, Integer> invUpdate = invalids.flatMapToPair(t -> {
                List<Tuple2<Tuple2<Integer, Integer>, Integer>> list = new ArrayList<>();
                int u = t._1._1;
                int v = t._1._2;
                Iterator<Integer> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    int w = iterator.next();
                    if (u < w)
                        list.add(new Tuple2<>(new Tuple2<>(u, w), v));
                    else
                        list.add(new Tuple2<>(new Tuple2<>(w, u), v));

                    if (v < w)
                        list.add(new Tuple2<>(new Tuple2<>(v, w), u));
                    else
                        list.add(new Tuple2<>(new Tuple2<>(w, v), u));
                }
                return list.iterator();
            }).partitionBy(partitioner).cache();

            invalids.unpersist();

            updates = updates.union(invUpdate);

            JavaPairRDD<Tuple2<Integer, Integer>, int[]> allInvUpdates = updates.cogroup(invUpdate).flatMapToPair(t -> {
                if (t._2._2.iterator().hasNext()) {
                    IntSet set = new IntOpenHashSet();
                    for (Integer i : t._2._1) {
                        set.add(i.intValue());
                    }
                    return Arrays.asList(new Tuple2<>(t._1, set.toIntArray())).iterator();
                }

                return Collections.emptyIterator();
            }).partitionBy(partitionerBig);

            invalids = edgeVertices.join(allInvUpdates).flatMapToPair(t -> {
                for (int i : t._2._2) {
                    t._2._1.remove(i);
                }

                if (t._2._1.size() == 0)
                    return Collections.emptyIterator();

                if (t._2._1.size() < minSup) {
                    return Arrays.asList(new Tuple2<>(t._1, t._2._1)).iterator();
                }

                return Collections.emptyIterator();
            }).cache();
        }

        long count = edgeVertices.cogroup(updates).mapValues(t -> {
            IntSet set = t._1.iterator().next();
            for (Integer i : t._2) {
                set.remove(i);
            }
            if (set.size() < minSup)
                return 0;
            return 1;
        }).filter(t -> t._2 != 0).count();

        long tEnd = System.currentTimeMillis();
        log("Count: " + count, tStart, tEnd);
        sc.close();
    }

    private static void log(String msg, long start, long end) {
        log(msg, (end - start));
    }

    private static void log(String msg) {
        log(msg, -1);
    }

    private static void log(String msg, long duration) {
        if (duration == -1)
            System.out.println("KTRUSS " + msg);
        else
            System.out.println("KTRUSS " + msg + ", duration: " + duration + " ms");
    }
}
