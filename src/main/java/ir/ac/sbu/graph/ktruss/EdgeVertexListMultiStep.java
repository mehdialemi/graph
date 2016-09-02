package ir.ac.sbu.graph.ktruss;

import ir.ac.sbu.graph.GraphUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

/**
 *
 */
public class EdgeVertexListMultiStep {

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
        final int support = k - 2;

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "KTruss-EdgeVertexList-" + k + "-MultiSteps", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, long[].class, List.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long start = System.currentTimeMillis();

        JavaRDD<Tuple3<Long, Long, Long>> allTriangles = RebuildTriangles.listTriangles(sc, inputPath, partition);

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

        int step = support + 1;
        float diffTimeRatio = 0.2f;
        while (!stop) {
            final int max = support + step - 1;
            log("iteration: " + ++iteration);
//            log("total edges: " + edgeNodes.count());

            long t1 = System.currentTimeMillis();
            JavaPairRDD<Tuple2<Long, Long>, List<Long>> partialEdgeNodes = edgeNodes.filter(e -> e._2.size() < max)
                .cache();

//            log("partial edges: " + partialEdgeNodes.count());

            JavaPairRDD<Tuple2<Long, Long>, List<Long>> toRemoveEdges = sc.emptyRDD()
                .mapToPair(t -> new Tuple2<>(new Tuple2<>(0L, 0L), new ArrayList<Long>(1)));

            step = 0;
            t1 = System.currentTimeMillis();
            long t2;
            long prevDuration = 0;
            long diffThreshold = 0;
            while (!stop) {
                step ++;
                JavaPairRDD<Tuple2<Long, Long>, List<Long>> invalidEdges = partialEdgeNodes.filter(en -> en._2.size() < support);
                long invalidEdgeCount = invalidEdges.count();
                log("Invalid edge count: " + invalidEdgeCount);
                if (invalidEdgeCount == 0) {
                    if (step == 1)
                        stop = true;
                    break;
                } else {
                    t2 = System.currentTimeMillis();
                }

                long stepDuration = (t2 - t1);
                logDuration("Step: " + step, stepDuration);
                if (step == 1) {
                    diffThreshold = (long) (stepDuration * diffTimeRatio);
                    logDuration("Step: " + step, diffThreshold);
                } else if (step > 2 && (stepDuration > diffThreshold && (stepDuration - prevDuration < diffThreshold))) {
                    break;
                }
                prevDuration = stepDuration;
                t1 = t2;

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

                JavaPairRDD<Tuple2<Long, Long>, List<Long>> validEdges = partialEdgeNodes.subtract(invalidEdges);

                JavaPairRDD<Tuple2<Long, Long>, Tuple2<Boolean, List<Long>>> newEdgeNodes =
                    validEdges.cogroup(edgeInvalidNodes).flatMapToPair(t -> {
                        Iterator<List<Long>> it = t._2._1.iterator();
                        if (!it.hasNext()) {
                            List<Long> nodes = new ArrayList<>();
                            for (Long n : t._2._2) {
                                nodes.add(n);
                            }
                            return Collections.singleton(new Tuple2<>(t._1, new Tuple2<>(false, nodes))).iterator();
                        }

                        List<Long> nodes = it.next();

                        if (nodes.size() < support)
                            return Collections.emptyIterator();

                        for (Long n : t._2._2) {
                            nodes.remove(n);
                        }

                        if (nodes.size() == 0)
                            return Collections.emptyIterator();

                        return Collections.singleton(new Tuple2<>(t._1, new Tuple2<>(true, nodes))).iterator();
                    }).cache();

                toRemoveEdges = toRemoveEdges.union(newEdgeNodes.filter(t -> !t._2._1).mapValues(t -> t._2));
//                log("Step (" + step + "), toRemove Edges: " +
//                    toRemoveEdges.map(t -> t._2.size()).reduce((a, b) -> a + b));

                JavaPairRDD<Tuple2<Long, Long>, List<Long>> nextEdgeNodes =
                    newEdgeNodes.filter(t -> t._2._1).mapValues(t -> t._2).cache();
                partialEdgeNodes.unpersist();
                partialEdgeNodes = nextEdgeNodes;
            }

            JavaPairRDD<Tuple2<Long, Long>, List<Long>> nextEdgeNodes =
                edgeNodes.filter(t -> t._2.size() >= max).cogroup(toRemoveEdges).flatMapToPair(t -> {
                Iterator<List<Long>> it = t._2._1.iterator();
                if (!it.hasNext())
                    return Collections.emptyIterator();

                List<Long> nodes = it.next();

                for (List<Long> n : t._2._2) {
                    nodes.removeAll(n);
                }

                if (nodes.size() == 0)
                    return Collections.emptyIterator();

                return Collections.singleton(new Tuple2<>(t._1, nodes)).iterator();
            }).cogroup(partialEdgeNodes, partition).mapValues(t -> {
                    Iterator<List<Long>> it = t._2.iterator();
                    if (it.hasNext())
                        return it.next();
                    return t._1.iterator().next();
                }).cache();

            edgeNodes.unpersist();
            edgeNodes = nextEdgeNodes;
        }

        long duration = System.currentTimeMillis() - start;
        JavaRDD<Tuple2<Long, Long>> edges = edgeNodes.map(t -> t._1);
        long edgeCount = edges.count();

        logDuration("KTruss Edge Count: " + edgeCount, duration);
        sc.close();
    }

    static void log(String text) {
        System.out.println("KTRUSS [" + new Date() + "] " + text);
    }

    static void logDuration(String text, long millis) {
        log(text + ", duration " + millis / 1000 + " sec");
    }
}
