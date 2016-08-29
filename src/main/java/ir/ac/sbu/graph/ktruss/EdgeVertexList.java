package ir.ac.sbu.graph.ktruss;

import ir.ac.sbu.graph.GraphUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by mehdi on 8/22/16.
 */
public class EdgeVertexList {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
//        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/cit-Patents.txt";
        String outputPath = "/home/mehdi/ir.ac.sbu.graph-data/output-mapreduce";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);
        final int support = k - 2;

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "KTruss-EdgeVertexList-" + k, partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, long[].class, List.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long start = System.currentTimeMillis();

        JavaRDD<Tuple3<Long, Long, Long>> allTriangles = RebuildTriangles.listTriangles(sc, inputPath, partition);

        long t1 = System.currentTimeMillis();
        JavaPairRDD<Tuple2<Long, Long>, List<Long>> edgeNodes = allTriangles.flatMapToPair(t -> {
            List<Tuple2<Tuple2<Long, Long>, Long>> list = new ArrayList<>(3);
            Tuple2<Long, Long> e1 = t._1() < t._2() ? new Tuple2<>(t._1(), t._2()) : new Tuple2<>(t._2(), t._1());
            Tuple2<Long, Long> e2 = t._1() < t._3() ? new Tuple2<>(t._1(), t._3()) : new Tuple2<>(t._3(), t._1());
            Tuple2<Long, Long> e3 = t._2() < t._3() ? new Tuple2<>(t._2(), t._3()) : new Tuple2<>(t._3(), t._2());
            list.add(new Tuple2<>(e1, t._3()));
            list.add(new Tuple2<>(e2, t._2()));
            list.add(new Tuple2<>(e3, t._1()));
            return list.iterator();
        }).groupByKey().mapValues(t -> {
            List<Long> list = new ArrayList<>();
            t.forEach(node -> list.add(node));
            return list;
        }).repartition(partition).cache();

        int iteration = 0;
        boolean stop = false;
        while (!stop) {
            log("iteration: " + ++iteration);

            long t3 = System.currentTimeMillis();
            JavaPairRDD<Tuple2<Long, Long>, List<Long>> invalidEdges =
                edgeNodes.filter(en -> en._2.size() < support&& en._2.size() > 0);

            long invalidEdgesCount = invalidEdges.count();
            if (invalidEdges.count() == 0) {
                stop = true;
                break;
            }

            long t4 = System.currentTimeMillis();
            logDuration("Finding invalid edges: " + invalidEdgesCount, (t4 - t3));

            JavaPairRDD<Tuple2<Long, Long>, Long> edgeInvalidNodes = invalidEdges
                .flatMapToPair(e -> {
                    List<Tuple2<Tuple2<Long, Long>, Long>> edges = new ArrayList<>();
                    Tuple2<Long, Long> invalidEdge = e._1;

                    for (Long node : e._2) {
                        if (node < invalidEdge._1)
                            edges.add(new Tuple2<>(new Tuple2<>(node, invalidEdge._1), invalidEdge._2));
                        else
                            edges.add(new Tuple2<>(new Tuple2<>(invalidEdge._1, node), invalidEdge._2));

                        if (node < invalidEdge._2)
                            edges.add(new Tuple2<>(new Tuple2<>(node, invalidEdge._2), invalidEdge._1));
                        else
                            edges.add(new Tuple2<>(new Tuple2<>(invalidEdge._2, node), invalidEdge._1));
                    }
                    return edges.iterator();
                });

            JavaPairRDD<Tuple2<Long, Long>, List<Long>> newEdgeNodes = edgeNodes
                .cogroup(edgeInvalidNodes, partition)
                .flatMapToPair(t -> {
                    Iterator<List<Long>> it = t._2._1.iterator();
                    if (!it.hasNext())
                        return Collections.emptyIterator();

                    List<Long> nodes = it.next();

                    if (nodes.size() < support)
                        return Collections.emptyIterator();

                    for (Long n : t._2._2) {
                        nodes.remove(n);
                    }

                    return Collections.singleton(new Tuple2<>(t._1,  nodes)).iterator();
                }).repartition(partition).cache();

            edgeNodes.unpersist();
            edgeNodes = newEdgeNodes;
        }

        long duration = System.currentTimeMillis() - start;
        JavaRDD<Tuple2<Long, Long>> edges = edgeNodes.map(t -> t._1);
        long edgeCount = edges.count();
        logDuration("Remaining ir.ac.sbu.graph edge count: " + edgeCount, duration);
        sc.close();
    }

    static void log(String text) {
        System.out.println("KTRUSS: " + text);
    }

    static void logDuration(String text, long millis) {
        log(text + ", duration " + millis / 1000 + " sec");
    }
}
