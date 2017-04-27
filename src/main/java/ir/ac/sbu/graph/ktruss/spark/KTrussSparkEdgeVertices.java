package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static ir.ac.sbu.graph.utils.Log.log;

public class KTrussSparkEdgeVertices extends KTruss {

    public KTrussSparkEdgeVertices(KTrussConf conf) {
        super(conf);
    }

    public JavaPairRDD<Tuple2<Integer, Integer>, IntSet> start() {
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> tVertices = triangleVertices();
        final int minSup = conf.k - 2;

        int iteration = 0;
        boolean stop = false;

        while (!stop) {
            iteration++;
            long t1 = System.currentTimeMillis();
            JavaPairRDD<Tuple2<Integer, Integer>, IntSet> invalids = tVertices.filter(kv -> kv._2.size() < minSup);
            long invalidEdgeCount = invalids.count();
            if (invalidEdgeCount == 0) {
                break;
            }
            long t2 = System.currentTimeMillis();
            String msg = "iteration: " + iteration + ", invalid edge count: " + invalidEdgeCount;
            log(msg, t2 - t1);

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
            }).groupByKey();

            tVertices = tVertices.filter(kv -> kv._2.size() >= minSup).leftOuterJoin(invalidUpdates)
                .mapValues(values -> {
                    Optional<Iterable<Integer>> invalidUpdate = values._2;
                    IntSet original = values._1;

                    if (!invalidUpdate.isPresent()) {
                        return original;
                    }

                    for (Integer v : invalidUpdate.get()) {
                        original.remove(v.intValue());
                    }

                    if (original.size() == 0)
                        return null;

                    return original;
                }).filter(kv -> kv._2 != null).repartition(conf.partitionNum).cache();
        }
        return tVertices;
    }

    public static void main(String[] args) {
        KTrussConf conf = new KTrussConf(args, KTrussSparkEdgeVertices.class.getSimpleName(),
            GraphUtils.VertexDegree.class, long[].class, List.class);
        KTrussSparkEdgeVertices kTruss = new KTrussSparkEdgeVertices(conf);


        long start = System.currentTimeMillis();
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> tVertices = kTruss.start();
        long edgeCount = tVertices.count();
        long duration = System.currentTimeMillis() - start;
        kTruss.close();
        log("KTruss Edge Count: " + edgeCount, duration);
    }
}
