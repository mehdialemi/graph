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
public class KTrussSparkTriangleJoin {

    public static final int CO_PARTITION_FACTOR = 10;

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

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partition);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl);

        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> edgeVertexList = candidates.cogroup(fonl).flatMapToPair(t -> {
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
        });

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> edgeSup = edgeVertexList.mapValues(v -> v.size()).cache();

        JavaPairRDD<Tuple2<Integer, Integer>, IntArraySet> invalidUpdates = sc.parallelize(new ArrayList<Integer>(), partition)
            .mapToPair(f -> new Tuple2<>(new Tuple2<>(0, 0), new IntArraySet()));
        int iteration = 0;
        while (true) {
            System.out.println("iteration: " + ++iteration);
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> invalids = edgeSup.filter(e -> e._2 < minSup).cache();
            long count = invalids.count();
            System.out.println("invalid count: " + count);
            if (count == 0)
                break;

            // Join invalids with  edgeVertexList
            JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Integer>> update =
                invalids.join(edgeVertexList).mapValues(v -> v._2).flatMapToPair(kv -> {
                List<Tuple2<Tuple2<Integer, Integer>, Integer>> out = new ArrayList<>(kv._2.size() * 2);
                kv._2.stream().forEach(w -> {
                    Integer u = kv._1._1;
                    Integer v = kv._1._2;
                    if (u < w)
                        out.add(new Tuple2<>(new Tuple2<>(u, w), v));
                    else
                        out.add(new Tuple2<>(new Tuple2<>(w, u), v));

                    if (v < w)
                        out.add(new Tuple2<>(new Tuple2<>(v, w), u));
                    else
                        out.add(new Tuple2<>(new Tuple2<>(w, v), u));
                });
                return out.iterator();
            }).groupByKey().subtractByKey(invalids);

            edgeSup = update.rightOuterJoin(edgeSup).mapValues(values -> {
                int sup = values._2;
                if (!values._1.isPresent()) {
                    if (sup < minSup)
                        return 0;
                    return sup;
                }

                for (Integer u : values._1.get()) {
                    sup -= 1;
                }

                return sup;
            }).filter(kv -> kv._2 > 0);
        }

        System.out.println("count: " + edgeSup.count());
        sc.close();
    }
}
