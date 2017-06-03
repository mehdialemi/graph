package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.util.*;

import static ir.ac.sbu.graph.utils.Log.log;

public class Cohen extends KTruss {

    public Cohen(KTrussConf conf) {
        super(conf);
    }

    private long start(JavaRDD<Tuple2<Integer, Integer>> edgeList) {

        final int minSup = conf.k - 2;

        int iteration = 0;
        while (true) {
            log("Iteration: " + ++iteration);

            // ************************* Generate Triangles *************************
            // 1: Augment edges with degrees
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> edgeDegs = edgeList.flatMapToPair(edge -> {
                List<Tuple2<Integer, Tuple2<Integer, Integer>>> output = new ArrayList<>(2);
                if (edge._1 > edge._2)
                    edge = edge.swap();
                output.add(new Tuple2<>(edge._1, edge));
                output.add(new Tuple2<>(edge._2, edge));
                return output.iterator();
            }).groupByKey()
                .flatMapToPair(ve -> {
                    List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> output = new ArrayList<>();
                    HashSet<Tuple2<Integer, Integer>> edges = new HashSet<>();

                    // Calculate degree and construct output key-value.
                    for (Tuple2<Integer, Integer> e : ve._2) {
                        edges.add(e);
                    }

                    for (Tuple2<Integer, Integer> e : edges) {
                        if (ve._1.equals(e._1))
                            output.add(new Tuple2<>(e, new Tuple2<>(edges.size(), 0)));
                        else
                            output.add(new Tuple2<>(e, new Tuple2<>(0, edges.size())));
                    }

                    return output.iterator();
                }).groupByKey(partitioner2).mapToPair(kv -> {

                    Tuple2<Integer, Integer> edge = kv._1;
                    Iterable<Tuple2<Integer, Integer>> degs = kv._2;
                    int d1 = -1, d2 = -1;
                    int i = 0;
                    for (Tuple2<Integer, Integer> deg : degs) {
                        i++;
                        if (i > 2)
                            throw new RuntimeException("Invalid number of edges");
                        if (deg._1 == 0)
                            d2 = deg._2;
                        else
                            d1 = deg._1;
                    }
                    if (d1 == -1 || d2 == -1)
                        throw new RuntimeException("Invalid degree, edge = " + edge + ", degs = " + degs);

                    return new Tuple2<>(edge, new Tuple2<>(d1, d2));
                }).persist(StorageLevel.DISK_ONLY_2());

            // 2: Construct open triads
            JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>[]> openTriads = edgeDegs.mapToPair(e ->
                e._2._1 < e._2._2 || (e._2._1.equals(e._2._2) && e._1._1 < e._1._2) ? new Tuple2<>(e._1._1, e._1) : new Tuple2<>(e._1._2, e._1))
                .groupByKey()
                .flatMapToPair(ve -> {

                    List<Tuple2<Integer, Integer>> edges = new ArrayList<>();
                    for (Tuple2<Integer, Integer> edge : ve._2) {
                        edges.add(edge);
                    }

                    List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>[]>> output = new ArrayList<>();
                    for (int i = 0; i < edges.size(); i++) {
                        final Tuple2<Integer, Integer> e1 = edges.get(i);
                        for (int j = i + 1; j < edges.size(); j++) {
                            final Tuple2<Integer, Integer> e2 = edges.get(j);
                            Tuple2<Integer, Integer>[] open = new Tuple2[]{e1, e2};

                            Tuple2<Integer, Integer> key;

                            if (e1._1.equals(ve._1)) {
                                if (e2._1.equals(ve._1)) {
                                    if (e1._2 < e2._2)
                                        key = new Tuple2<>(e1._2, e2._2);
                                    else
                                        key = new Tuple2<>(e2._2, e1._2);
                                } else {
                                    if (e1._2 < e2._1)
                                        key = new Tuple2<>(e1._2, e2._1);
                                    else
                                        key = new Tuple2<>(e2._1, e1._2);
                                }
                            } else {
                                if (e2._1.equals(ve._1)) {
                                    if (e1._1 < e2._2)
                                        key = new Tuple2<>(e1._1, e2._2);
                                    else
                                        key = new Tuple2<>(e2._2, e1._1);
                                } else {
                                    if (e1._1 < e2._1)
                                        key = new Tuple2<>(e1._1, e2._1);
                                    else
                                        key = new Tuple2<>(e2._1, e1._1);
                                }
                            }

                            output.add(new Tuple2<>(key, open));
                        }
                    }

                    return output.iterator();
                }).partitionBy(partitioner2).persist(StorageLevel.DISK_ONLY_2());

            // 3: Join open triads with edges to find triangles
            JavaRDD<Tuple2<Integer, Integer>[]> triangles = openTriads.join(edgeDegs).map(value -> {
                Tuple2<Integer, Integer>[] edges = new Tuple2[3];
                edges[0] = value._1;
                edges[1] = value._2._1[0];
                edges[2] = value._2._1[1];
                return edges;
            });

            log("Triangle size: " + triangles.count());

            openTriads.unpersist();
            edgeDegs.unpersist();

            // ******************************  Prune edges with support less than minSup ***************************************
            // Extract triangles' edges
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> edgeCount = triangles
                .flatMapToPair(t -> {
                    List<Tuple2<Tuple2<Integer, Integer>, Integer>> edges = new ArrayList<>(3);
                    edges.add(new Tuple2<>(t[0], 1));
                    edges.add(new Tuple2<>(t[1], 1));
                    edges.add(new Tuple2<>(t[2], 1));
                    return edges.iterator();
                }).reduceByKey((a, b) -> a + b);

            JavaPairRDD<Tuple2<Integer, Integer>, Boolean> newEdges = edgeCount.mapToPair(t -> {
                // If triangle count for the current edge is higher than the specified sup, then include that edge,
                // otherwise exclude the current edge.
                if (t._2 >= minSup)
                    return new Tuple2<>(t._1, true);
                return new Tuple2<>(t._1, false);
            }).cache();

            boolean allTrue = newEdges.map(t -> t._2).reduce((a, b) -> a && b);
            log("Reduction: " + allTrue);
            if (allTrue)
                break;

            edgeList = newEdges.filter(t -> t._2).map(t -> t._1).repartition(conf.partitionNum).cache();
        }

        return edgeList.count();
    }

    public static void main(String[] args) throws FileNotFoundException {
        KTrussConf conf = new KTrussConf(args, Cohen.class.getSimpleName(),
            GraphUtils.class, GraphUtils.VertexDegree.class, long[].class, Map.class, HashMap.class);
        Cohen cohen = new Cohen(conf);

        long tload = System.currentTimeMillis();
        JavaPairRDD<Integer, Integer> edges = cohen.loadEdges();
        JavaRDD<Tuple2<Integer, Integer>> edgeList = edges.groupByKey(conf.partitionNum).flatMap(t -> {
            HashSet<Integer> neighborSet = new HashSet<>();
            for (int neighbor : t._2) {
                neighborSet.add(neighbor);
            }
            List<Tuple2<Integer, Integer>> output = new ArrayList<>(neighborSet.size());
            for (Integer v : neighborSet) {
                output.add(new Tuple2<>(t._1, v));
            }

            return output.iterator();
        }).repartition(conf.partitionNum).cache();
        log("Load edges ", tload, System.currentTimeMillis());

        long start = System.currentTimeMillis();
        long edgeCount = cohen.start(edgeList);
        long duration = System.currentTimeMillis() - start;

        cohen.close();
        log("KTruss Edge Count: " + edgeCount, duration);

    }
}