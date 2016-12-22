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
public class KTrussSpark2 {

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
                Tuple2<Long, Long> uv = new Tuple2<>(u, v);
                for (int i = 1; i < cvalue.length && lastIndex < fv.length; i++) {
                    long w = cvalue[i];
                    boolean common = false;
                    for (int j = index; j < fv.length; j++, index++) {
                        if (w == fv[j]) {
                            common = true;
                            lastIndex = index + 1;
                            // report a triangle with
                            Tuple2<Long, Long> uw = new Tuple2<>(u, w);
                            list.add(new Tuple2<>(uw, v));
                            list.add(new Tuple2<>(uv, w));
                            if (tuples[j] == null) {
                                tuples[j] = new Tuple2<>(v, w);
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
        }).groupByKey(partition * 10);

        System.out.println("count, " + evMap.count());
    }
}
