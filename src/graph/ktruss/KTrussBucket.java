package graph.ktruss;

import graph.GraphUtils;
import graph.clusteringco.FonlDegTC;
import graph.clusteringco.FonlUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class KTrussBucket {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        String outputPath = "/home/mehdi/graph-data/output-mapreduce";
        int k = 4; // k-truss

        if (args.length > 0)
            k = Integer.parseInt(args[0]);
        final int support = k - 2;

        if (args.length > 1)
            inputPath = args[1];

        SparkConf conf = new SparkConf();
        conf.setAppName("KTruss MapReduce");
        conf.setMaster("local[2]");
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, long[].class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int partition = 2;
        JavaPairRDD<Long, long[]> fonl = FonlUtils.loadFonl(sc, inputPath, partition);
        JavaPairRDD<Long, long[]> forwardRDD = FonlDegTC.createCandidates(fonl);

        JavaPairRDD<Long, Tuple2<List<Long>, List<List<Long>>>> triangleBucket = forwardRDD.cogroup(fonl, partition)
            .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Iterable<long[]>, Iterable<long[]>>>,
                Long, Tuple2<List<Long>, List<List<Long>>>>() {
                @Override
                public Tuple2<Long, Tuple2<List<Long>, List<List<Long>>>> call(Tuple2<Long, Tuple2<Iterable<long[]>,
                    Iterable<long[]>>> t) throws Exception {
                    Iterator<long[]> iterator = t._2._1.iterator();
                    long[] hDegs = t._2._2.iterator().next();
                    Arrays.sort(hDegs, 1, hDegs.length);
                    List<List<Long>> commonList = new ArrayList<>();
                    List<Long> nodes = new ArrayList<>();
                    while (iterator.hasNext()) {
                        long[] forward = iterator.next();
                        List<Long> common = GraphUtils.sortedIntersection(hDegs, forward, 1, 1);
                        if (common.size() > 0) {
                            nodes.add(forward[0]);
                            Collections.sort(common);
                            commonList.add(common);
                        }
                    }

                    if (nodes.size() > 0) {
                        Collections.sort(nodes);
                        return new Tuple2<>(t._1, new Tuple2<>(nodes, commonList));
                    }
                    return null;
                }
            });

        while (true) {
            triangleBucket.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<List<Long>, List<List<Long>>>>,
                Tuple2<Long, Long>, Integer>() {
                @Override
                public Iterable<Tuple2<Tuple2<Long, Long>, Integer>> call(
                    Tuple2<Long, Tuple2<List<Long>, List<List<Long>>>> t) throws Exception {
                    List<Tuple2<Tuple2<Long, Long>, Integer>> output = new ArrayList<>();
                    for (int i = 0 ; i < t._2._1.size(); i ++) {
                        long node = t._2._1.get(i);
                        List<Long> common = t._2._2.get(i);
                        if (common.size() < support) {

                        }
                    }
                    return null;
                }
            });
        }
    }
}
