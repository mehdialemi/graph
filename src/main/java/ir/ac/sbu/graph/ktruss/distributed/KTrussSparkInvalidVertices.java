package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class KTrussSparkInvalidVertices {

    public static final int CO_PARTITION_FACTOR = 10;
    public static final int[] ZERO_INT = new int[]{0};

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

        long tStart = System.currentTimeMillis();

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partition);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl);

        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> edgeVertexes = candidates.cogroup(fonl).flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;
            List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>();
            for (int[] cVal : t._2._1) {
                int u = cVal[0];
                IntList wList = GraphUtils.sortedIntersectionTest(fVal, 1, cVal, 1);
                if (wList == null)
                    continue;

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
        }).groupByKey().mapValues(values -> {
            IntSet set = new IntOpenHashSet();
            for (Integer v : values) {
                set.add(v.intValue());
            }
            return set;
        }).repartition(partition).cache();

        JavaPairRDD<Tuple2<Integer, Integer>, int[]> edgeSup = edgeVertexes.mapValues(v -> new int[]{v.size()}).cache();

        JavaPairRDD<Tuple2<Integer, Integer>, int[]> prevEdgeSup = edgeSup;
        int iteration = 0;
        while (true) {
            long t1 = System.currentTimeMillis();
            System.out.println("iteration: " + ++iteration);
            JavaPairRDD<Tuple2<Integer, Integer>, int[]> invalids = edgeSup.filter(value -> (value._2[0] + 1 - value._2.length) < minSup);

            long count = invalids.count();
            long t2 = System.currentTimeMillis();
            System.out.println("invalid count: " + count + ", duration: " + (t2 - t1) + " ms");
            if (count == 0)
                break;

            // Join invalids with  edgeVertexes
            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> invalidUpdate =
                invalids.join(edgeVertexes)
                    .flatMapToPair(kv -> {
                        for (int i = 1; i < kv._2._1.length; i++) {
                            kv._2._2.remove(kv._2._1[i]);
                        }

                        List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>(kv._2._2.size() * 2);

                        int u = kv._1._1;
                        int v = kv._1._2;
                        for (int w : kv._2._2) {
                            if (u < w)
                                out.add(new Tuple2<>(new Tuple2<>(u, w), v));
                            else
                                out.add(new Tuple2<>(new Tuple2<>(w, u), v));

                            if (v < w)
                                out.add(new Tuple2<>(new Tuple2<>(v, w), u));
                            else
                                out.add(new Tuple2<>(new Tuple2<>(w, v), u));
                        }

                        return out.iterator();
                    }).groupByKey();

            edgeSup = invalidUpdate.rightOuterJoin(edgeSup).mapValues(joinValue -> {
                int initSup = joinValue._2[0];
                int prevSup = initSup + 1 - joinValue._2.length;
                if (!joinValue._1.isPresent()) {
                    if (prevSup < minSup) {
                        return null;
                    }
                    return joinValue._2;
                }

                IntSortedSet invalidVertexes = new IntAVLTreeSet(joinValue._2, 1, joinValue._2.length - 1);
                for (int u : joinValue._1.get()) {
                    invalidVertexes.add(u);
                }

                int newSup = initSup - invalidVertexes.size();
                if (newSup == 0)
                    return null;

                int[] outVal = new int[1 + invalidVertexes.size()];
                outVal[0] = initSup;
                System.arraycopy(invalidVertexes.toIntArray(), 0, outVal, 1, outVal.length - 1);
                return outVal;
            }).filter(kv -> kv._2 != null).cache();

            prevEdgeSup.unpersist();
        }

        long tEnd = System.currentTimeMillis();
        System.out.println("count: " + edgeSup.count() + ", duration: " + (tEnd - tStart) + " ms");
        sc.close();
    }
}
