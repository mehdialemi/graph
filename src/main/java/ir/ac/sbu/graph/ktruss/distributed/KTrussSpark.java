package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class KTrussSpark {

    public static final Iterator<Long> EMPTY_ITERATOR = Collections.emptyIterator();
    public static final ArrayList<Long> EMPTY_LONG_LIST = new ArrayList<>(0);

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

        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);

        JavaPairRDD<Long, long[]> fonl = FonlUtils.createWith2ReduceNoSort(edges, partition);

        JavaPairRDD<Long, long[]> candidates = FonlDegTC.generateCandidates(fonl, true, false);
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> evMap = candidates.cogroup(fonl, partition).flatMapToPair(t -> {
            Iterator<long[]> cvalues = t._2._2.iterator();
            List<Tuple2<Tuple2<Long, Long>, Long>> list = new ArrayList<>();

            if (!cvalues.hasNext())
                return Collections.emptyIterator();
            long[] fv = cvalues.next();
            cvalues = t._2._1.iterator();
            Tuple2<Long, Long>[] tuples = new Tuple2[fv.length];
            while (cvalues.hasNext()) {
                long v = t._1.longValue();
                long[] cvalue = cvalues.next();
                int lastIndex = 1;
                int index = lastIndex;
                long u = cvalue[0];
                Tuple2<Long, Long> uv = null;
                for (int i = 1; i < cvalue.length && lastIndex < fv.length; i++) {
                    long w = cvalue[i];
                    boolean common = false;
                    for (int j = index; j < fv.length; j++, index++) {
                        if (w == fv[j]) {
                            if (uv == null) {
                                if (u < v)
                                    uv = new Tuple2<>(u, v);
                                else
                                    uv = new Tuple2<>(v, u);
                            }
                            common = true;
                            lastIndex = index + 1;
                            // report a triangle with
                            Tuple2<Long, Long> uw;
                            if (u < w)
                                uw = new Tuple2<>(u, w);
                            else
                                uw = new Tuple2<>(w, u);

                            list.add(new Tuple2<>(uw, v));
                            list.add(new Tuple2<>(uv, w));
                            if (tuples[j] == null) {
                                if (v < w)
                                    tuples[j] = new Tuple2<>(v, w);
                                else
                                    tuples[j] = new Tuple2<>(w, v);
                            }
                            list.add(new Tuple2<>(tuples[j], u));
                            break;
                        }
                    }
                    if (!common)
                        index = lastIndex;
                }
            }
            return list.iterator();
        }).groupByKey().repartition(partition * 10);

        int iteration = 1;
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> current = evMap.cache();
        JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> prev = current;
        while (true) {
            System.out.println("iteration: " + iteration ++);

            // TODO store invalids in redis
            // find invalid edges
            JavaPairRDD<Tuple2<Long, Long>, Iterable<Long>> invalids = current.filter(t -> {
                int size = 0;
                Iterator<Long> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    size++;
                    if (size >= minSup)
                        return false;
                    iterator.next();
                }
                return true;
            }).cache();

            long count = invalids.count();
            System.out.println("invalid count: " + count);
            if (count == 0)
                break;

            // find update for other edges to remove invalid edges
            JavaPairRDD<Tuple2<Long, Long>, Long> update = invalids.flatMapToPair(t -> {
                List<Tuple2<Tuple2<Long, Long>, Long>> list = new ArrayList<>();
                long u = t._1._1;
                long v = t._1._2;
                Iterator<Long> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    long w = iterator.next();
                    if (v < w) {
                        list.add(new Tuple2<>(new Tuple2<>(u, w), v));
                        list.add(new Tuple2<>(new Tuple2<>(v, w), u));
                    } else if (u > w) {
                        list.add(new Tuple2<>(new Tuple2<>(w, u), v));
                        list.add(new Tuple2<>(new Tuple2<>(w, v), u));
                    } else {
                        list.add(new Tuple2<>(new Tuple2<>(u, w), v));
                        list.add(new Tuple2<>(new Tuple2<>(w, v), u));
                    }
                }
                return list.iterator();
            });

            current = current.cogroup(update, invalids).flatMapToPair(f -> {
                List<Tuple2<Tuple2<Long, Long>, Iterable<Long>>> output = new ArrayList<>();
                if (f._2._3().iterator().hasNext())
                    return output.iterator();

                Set<Long> set = new HashSet<>();
                for (Long v : f._2._2())
                    set.add(v);

                if (set.size() == 0) {
                    output.add(new Tuple2<>(f._1, f._2._1().iterator().next()));
                    return output.iterator();
                }

                List<Long> list = new ArrayList<>();
                Iterator<Long> iterator = f._2._1().iterator().next().iterator();
                while (iterator.hasNext()) {
                    Long next = iterator.next();
                    if (set.contains(next))
                        continue;
                    list.add(next);
                }
                output.add(new Tuple2<>(f._1, list));
                return output.iterator();
            }).repartition(partition * 5).cache();

            prev.unpersist();
            prev = current;
        }

        System.out.println("count, " + current.count());
        sc.close();
    }
}
