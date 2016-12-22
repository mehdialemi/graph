package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class KTrussSpark {

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
        JavaPairRDD<Long, long[]> fonl = FonlUtils.loadFonl(sc, inputPath, partition);
        JavaPairRDD<Long, long[]> candidates = FonlDegTC.generateCandidates(fonl, true, false);
        JavaPairRDD<Tuple2<Long, Long>, Set<Long>> evMap = candidates.cogroup(fonl, partition).mapPartitions(p -> {
            Map<Tuple2<Long, Long>, Set<Long>> map = new HashMap<>();
            while (p.hasNext()) {
                Tuple2<Long, Tuple2<Iterable<long[]>, Iterable<long[]>>> t = p.next();
                Iterator<long[]> cvalues = t._2._2.iterator();
                if (!cvalues.hasNext())
                    continue;
                long[] fv = cvalues.next();
                cvalues = t._2._1.iterator();
                while (cvalues.hasNext()) {
                    long v = t._1.longValue();
                    long[] cvalue = cvalues.next();
                    int lastIndex = 1;
                    int index = lastIndex;
                    long u = cvalue[0];
                    for (int i = 1; i < cvalue.length && lastIndex < fv.length; i++) {
                        long w = cvalue[i];
                        boolean common = false;
                        for (int j = index; j < fv.length; j++, index++) {
                            if (w == fv[j]) {
                                common = true;
                                lastIndex = index + 1;
                                // report a triangle with
                                Tuple2<Long, Long> uw = new Tuple2<>(u, w);
                                Set<Long> set = map.get(uw);
                                if (set == null) {
                                    set = new HashSet<>();
                                    map.put(uw, set);
                                }
                                set.add(v);

                                Tuple2<Long, Long> uv = new Tuple2<>(u, v);
                                set = map.get(uv);
                                if (set == null) {
                                    set = new HashSet<>();
                                    map.put(uv, set);
                                }
                                set.add(w);

                                Tuple2<Long, Long> vw = new Tuple2<>(v, w);
                                set = map.get(vw);
                                if (set == null) {
                                    set = new HashSet<>();
                                    map.put(vw, set);
                                }
                                set.add(u);
                                break;
                            }
                        }
                        if (!common)
                            index = lastIndex;
                    }
                }
            }
            return map.entrySet().iterator();
        }).groupBy(t -> t.getKey()).mapValues(t -> {
            Set<Long> set = new HashSet<>();
            Iterator<Map.Entry<Tuple2<Long, Long>, Set<Long>>> iterator = t.iterator();
            while (iterator.hasNext())
                set.addAll(iterator.next().getValue());
            return set;
        });

        System.out.println("count, " + evMap.count());
    }
}
