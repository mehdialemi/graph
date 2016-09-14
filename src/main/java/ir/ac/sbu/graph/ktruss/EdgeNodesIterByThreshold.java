package ir.ac.sbu.graph.ktruss;

import ir.ac.sbu.graph.GraphUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class EdgeNodesIterByThreshold {

    public static final int MIN_PLUS_DIFF_THRESHOLD = 3000;

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

        JavaPairRDD<Tuple2<Long, Long>, List<Long>> edgeNodes = RebuildTriangles.listEdgeNodes(sc, inputPath, partition);

        int iteration = 0;
        boolean stop = false;

        JavaPairRDD<Tuple2<Long, Long>, List<Long>> empty = sc.emptyRDD().mapToPair(t -> new Tuple2<>(new Tuple2<>(0L, 0L), new ArrayList<>(1)));
        int currStepCount = minSup;
        float diffTimeRatio = 0.2f;
        int lastMaxSup = 0;
        int prevStepCount = currStepCount;
        while (!stop) {
            // if we currStep is higher prevStepCount then lastMaxSup was good so use it again.
            final int maxSup = currStepCount > prevStepCount ? lastMaxSup + 1 : minSup + Math.max(currStepCount, minSup);
            lastMaxSup = maxSup;
            prevStepCount = currStepCount;
            log("iteration: " + ++iteration + ", maxSup: " + maxSup + ", minSup: " + minSup);
//            log("total edges: " + edgeNodes.count());

            JavaPairRDD<Tuple2<Long, Long>, List<Long>> partialEdgeNodes = edgeNodes.filter(e -> e._2.size() < maxSup).cache();
//            log("partial edges: " + partialEdgeNodes.count());

            JavaPairRDD<Tuple2<Long, Long>, List<Long>> toRemoveEdges = empty;

            currStepCount = 0;
            long t1 = System.currentTimeMillis();
            long t2;
            long prevDuration = 0;
            long diffThreshold = 0;
            long invalidEdgeCount;
            float sumDurations = 0;
            while (!stop) {
                JavaPairRDD<Tuple2<Long, Long>, List<Long>> invalidEdges = partialEdgeNodes.filter(en -> en._2.size() < minSup);
                invalidEdgeCount = invalidEdges.count();
                log("invalid edge count: " + invalidEdgeCount);
                if (invalidEdgeCount == 0) {
                    if (currStepCount == 0)
                        stop = true;
                    break;
                } else {
                    t2 = System.currentTimeMillis();
                }

                long currDuration = (t2 - t1);

                logDuration("step: " + currStepCount, currDuration);
                if (currStepCount == 0) {
                    diffThreshold = (long) (currDuration * diffTimeRatio) + MIN_PLUS_DIFF_THRESHOLD;
                    log("step: " + currStepCount + ", diff-threshold: " + diffThreshold / 1000 + " sec");
                } else {
                    sumDurations += currDuration;
                    float avgDuration = sumDurations / (float) currStepCount;
                    float ratioDuration = (currDuration - prevDuration) / (float) prevDuration;
                    if (currStepCount > 1 && (currDuration > avgDuration)  &&  ratioDuration > diffTimeRatio) {
                        break;
                    }
                }
                prevDuration = currDuration;
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

                        if (nodes.size() < minSup)
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
                currStepCount++;
            }

            if (stop)
                break;

            JavaPairRDD<Tuple2<Long, Long>, List<Long>> nextEdgeNodes = edgeNodes
                .filter(t -> t._2.size() >= maxSup)
                .cogroup(toRemoveEdges)
                .flatMapToPair(t -> {
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
                }).repartition(partition).cache();

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
        log(text + " (" + millis / 1000 + " sec)");
    }
}
