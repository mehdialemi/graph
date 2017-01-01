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
public class KTrussSparkTriangle {

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

        JavaPairRDD<Long, Long> edges = GraphLoader.loadEdges(input);

        JavaPairRDD<Long, long[]> fonl = FonlUtils.createWith2ReduceNoSort(edges, partition);

        JavaPairRDD<Long, long[]> candidates = FonlDegTC.generateCandidates(fonl, true, false);
        JavaPairRDD<Tuple2<Long, Long>, long[]> eTriangleMap = candidates.cogroup(fonl, partition).flatMapToPair(t -> {
            Iterator<long[]> cvalues = t._2._2.iterator();
            List<Tuple2<Tuple2<Long, Long>, long[]>> list = new ArrayList<>();

            if (!cvalues.hasNext())
                return Collections.emptyIterator();

            long[] fv = cvalues.next();
            cvalues = t._2._1.iterator();
            while (cvalues.hasNext()) {
                long v = t._1.longValue();
                long[] cvalue = cvalues.next();
                int lastIndex = 1;
                int index = lastIndex;
                long u = cvalue[0];
                Tuple2<Long, Long> uv = null;
                List<Long> wSet = null;
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
                                wSet = new ArrayList<>(Math.min(fv.length, cvalue.length) / 2);
                            }
                            // report a triangle with
                            wSet.add(w);
                            common = true;
                            lastIndex = index + 1;
                            break;
                        }
                    }
                    if (!common)
                        index = lastIndex;
                }

                // TODO use min1 and min2 as u and v and others should be sorted
                if (wSet != null) {
                    long[] wArray = new long[wSet.size()];
                    for (int i = 0; i < wSet.size(); i++) {
                        wArray[i] = wSet.get(i);
                    }
                    Arrays.sort(wArray);
                    list.add(new Tuple2<>(uv, wArray));
                }
            }

            return list.iterator();
        }).repartition(partition * 10).cache();

        JavaPairRDD<Tuple2<Long, Long>, Integer> edgeSup = eTriangleMap.flatMapToPair(t -> {
            long u = t._1._1;
            long v = t._1._2;
            List<Tuple2<Tuple2<Long, Long>, Integer>> list = new ArrayList<>(t._2.length * 2 + 1);
            list.add(new Tuple2<>(t._1, t._2.length));
            for (Long w : t._2) {
                Tuple2<Long, Long> t1;
                Tuple2<Long, Long> t2;
                if (w < u) {
                    t1 = new Tuple2<>(w, u);
                    t2 = new Tuple2<>(w, v);
                } else if (w > v) {
                    t1 = new Tuple2<>(u, w);
                    t2 = new Tuple2<>(v, w);
                } else {
                    t1 = new Tuple2<>(u, w);
                    t2 = new Tuple2<>(w, v);
                }
                list.add(new Tuple2<>(t1, 1));
                list.add(new Tuple2<>(t2, 1));
            }
            return list.iterator();
        }).reduceByKey((a, b) -> a + b).cache();

        System.out.println("count, " + edgeSup.count());
        sc.close();
    }
}
