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

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceNoSortInt(edges, partition);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl, true, false);
        JavaPairRDD<Long, int[]> eTriangleMap = candidates.cogroup(fonl, partition * CO_PARTITION_FACTOR).flatMapToPair(t -> {
            Iterator<int[]> cvalues = t._2._2.iterator();
            List<Tuple2<Long, int[]>> list = new ArrayList<>();

            if (!cvalues.hasNext())
                return Collections.emptyIterator();

            int[] fv = cvalues.next();
            cvalues = t._2._1.iterator();
            while (cvalues.hasNext()) {
                int v = t._1;
                int[] cvalue = cvalues.next();
                int lastIndex = 1;
                int index = lastIndex;
                int u = cvalue[0];
                Long uv = null;
                List<Integer> wSet = null;
                for (int i = 1; i < cvalue.length && lastIndex < fv.length; i++) {
                    int w = cvalue[i];
                    boolean common = false;
                    for (int j = index; j < fv.length; j++, index++) {
                        if (w == fv[j]) {
                            if (uv == null) {
                                if (u < v)
                                    uv = (long) u << 32 | v & 0xFFFFFFFFL;
                                else
                                    uv = (long) v << 32 | u & 0xFFFFFFFFL;
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
                    int[] wArray = new int[wSet.size()];
                    for (int i = 0; i < wSet.size(); i++) {
                        wArray[i] = wSet.get(i);
                    }
                    Arrays.sort(wArray);
                    list.add(new Tuple2<>(uv, wArray));
                }
            }

            return list.iterator();
        }).repartition(partition * CO_PARTITION_FACTOR).cache();

        JavaPairRDD<Long, Integer> edgeSup = eTriangleMap.flatMapToPair(t -> {
            int u = (int) (t._1 >> 32);
            int v = (int) t._1.longValue();

            List<Tuple2<Long, Integer>> list = new ArrayList<>(t._2.length * 2 + 1);
            list.add(new Tuple2<>(t._1, t._2.length));
            for (int w : t._2) {
                Long t1;
                Long t2;
                if (w < u) {
                    t1 = (long) w << 32 | u & 0xFFFFFFFFL;
                    t2 = (long) w << 32 | v & 0xFFFFFFFFL;
                } else if (w > v) {
                    t1 = (long) u << 32 | w & 0xFFFFFFFFL;
                    t2 = (long) v << 32 | w & 0xFFFFFFFFL;
                } else {
                    t1 = (long) u << 32 | w & 0xFFFFFFFFL;
                    t2 = (long) w << 32 | v & 0xFFFFFFFFL;
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