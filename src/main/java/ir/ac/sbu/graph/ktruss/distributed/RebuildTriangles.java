package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.utils.GraphUtils;
import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

/**
 *
 */
public class RebuildTriangles {

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
        GraphUtils.setAppName(conf, "KTruss-RebuildTriangles-" + k, partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, long[].class, List.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long start = System.currentTimeMillis();

        JavaRDD<Tuple3<Long, Long, Long>> triangles = listTriangles(sc, inputPath, partition)
                .repartition(partition).cache();

        int iteration = 0;
        boolean stop = false;
        while (!stop) {
            log("iteration: " + ++iteration);

            JavaPairRDD<KEdge, Integer> edgeCounts = triangles.flatMapToPair(t -> {
                List<Tuple2<KEdge, Integer>> list = new ArrayList<>(3);
                list.add(new Tuple2<>(new KEdge(t._1(), t._2(), t._3()), 1));
                list.add(new Tuple2<>(new KEdge(t._1(), t._3(), t._2()), 1));
                list.add(new Tuple2<>(new KEdge(t._2(), t._3(), t._1()), 1));
                return list.iterator();
            }).reduceByKey((a, b) -> a + b);
            long t2 = System.currentTimeMillis();

            JavaPairRDD<KEdge, Integer> invalidEdges = edgeCounts.filter(ec -> ec._2 < support);
            long invalidEdgeCount = invalidEdges.count();
            long t3 = System.currentTimeMillis();
            logDuration("Invalid Edge Count: " + invalidEdgeCount, t3 - t2);

            if (invalidEdgeCount == 0) {
                stop = true;
                break;
            }

            JavaRDD<Tuple3<Long, Long, Long>> invalidTriangles = invalidEdges.repartition(partition)
                    .distinct().map(t -> t._1.createTuple3());

            JavaRDD<Tuple3<Long, Long, Long>> newTriangles = triangles.subtract(invalidTriangles).repartition
                    (partition).cache();
            triangles.unpersist();
            triangles = newTriangles;
        }

        JavaRDD<Tuple2<Long, Long>> edges = triangles.flatMap(t -> {
            List<Tuple2<Long, Long>> list = new ArrayList<>(3);
            list.add(new Tuple2<>(t._1(), t._2()));
            list.add(new Tuple2<>(t._1(), t._3()));
            list.add(new Tuple2<>(t._2(), t._3()));
            return list.iterator();
        }).distinct();

        long duration = System.currentTimeMillis() - start;
        logDuration("Remaining ir.ac.sbu.graph edge count: " + edges.count(), duration);
        sc.close();
    }

    static class KEdge implements Serializable {
        long v1;
        long v2;
        long v3;

        public KEdge(long v1, long v2, long v3) {
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            KEdge e = (KEdge) obj;
            return (v1 == e.v1 && v2 == e.v2) || (v1 == e.v2 && v2 == e.v1);
        }

        @Override
        public int hashCode() {
            return (int) (v1 + v2);
        }

        public Tuple3<Long, Long, Long> createTuple3() {
            return GraphUtils.createSorted(v1, v2, v3);
        }
    }

    public static JavaRDD<Tuple3<Long, Long, Long>> listTriangles(JavaSparkContext sc, String inputPath,
                                                                  int partition) {
        JavaPairRDD<Long, long[]> fonl = FonlUtils.loadFonl(sc, inputPath, partition);

        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of higherIds (high deg) would be allocated besides vertex with
        // low number of higherIds (high deg)

        JavaPairRDD<Long, long[]> candidates = FonlDegTC.generateCandidates(fonl, true);
        return candidates.cogroup(fonl, partition).flatMap(t -> {
            List<Tuple3<Long, Long, Long>> triangles = new ArrayList<>();
            Iterator<long[]> iterator = t._2._2.iterator();
            if (!iterator.hasNext())
                return triangles.iterator();

            long[] hDegs = iterator.next();

            iterator = t._2._1.iterator();
            if (!iterator.hasNext())
                return triangles.iterator();

            Arrays.sort(hDegs, 1, hDegs.length);

            do {
                long[] forward = iterator.next();
                List<Long> common = GraphUtils.sortedIntersection(hDegs, forward, 1, 1);
                for (long v : common)
                    triangles.add(GraphUtils.createSorted(forward[0], t._1, v));
            } while (iterator.hasNext());

            return triangles.iterator();
        });
    }

    public static JavaPairRDD<Tuple2<Long, Long>, List<Long>> listEdgeNodes(JavaSparkContext sc, String inputPath,
                                                                            int partition) {
        return listEdgeNodes(sc, inputPath, partition, null);

    }

    public static JavaPairRDD<Tuple2<Long, Long>, List<Long>> listEdgeNodes(JavaSparkContext sc, String inputPath,
                                                                            int partition, Partitioner partitioner) {
        JavaPairRDD<Long, long[]> fonl = FonlUtils.loadFonl(sc, inputPath, partition);

        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of higherIds (high deg) would be allocated besides vertex with
        // low number of higherIds (high deg)

        JavaPairRDD<Long, long[]> candidates = FonlDegTC.generateCandidates(fonl, true);
        JavaPairRDD<Tuple2<Long, Long>, List<Long>> result = candidates.cogroup(fonl, partition).flatMapToPair(t -> {
            Iterator<long[]> iterator = t._2._2.iterator();
            if (!iterator.hasNext())
                return Collections.emptyIterator();

            long[] hDegs = iterator.next();

            iterator = t._2._1.iterator();
            if (!iterator.hasNext())
                return Collections.emptyIterator();

            Arrays.sort(hDegs, 1, hDegs.length);

            List<Tuple2<Tuple2<Long, Long>, Long[]>> list = new ArrayList<>();
            Map<Tuple2<Long, Long>, List<Long>> eMap = new HashMap<>();
            do {
                long[] forward = iterator.next();
                List<Long> common = GraphUtils.sortedIntersection(hDegs, forward, 1, 1);
                for (long v : common) {
                    Tuple3<Long, Long, Long> sorted = GraphUtils.createSorted(forward[0], t._1, v);
                    Tuple2<Long, Long> e = new Tuple2<>(sorted._1(), sorted._2());
                    List<Long> vList = eMap.get(e);
                    if (vList != null)
                        vList.add(sorted._3());
                    else {
                        vList = new ArrayList<>();
                        vList.add(sorted._3());
                        eMap.put(e, vList);
                    }

                    e = new Tuple2<>(sorted._1(), sorted._3());
                    vList = eMap.get(e);
                    if (vList != null)
                        vList.add(sorted._2());
                    else {
                        vList = new ArrayList<>();
                        vList.add(sorted._2());
                        eMap.put(e, vList);
                    }

                    e = new Tuple2<>(sorted._2(), sorted._3());
                    vList = eMap.get(e);
                    if (vList != null)
                        vList.add(sorted._1());
                    else {
                        vList = new ArrayList<>();
                        vList.add(sorted._1());
                        eMap.put(e, vList);
                    }
                }
            } while (iterator.hasNext());

            for (Map.Entry<Tuple2<Long, Long>, List<Long>> e : eMap.entrySet()) {
                list.add(new Tuple2<>(e.getKey(), e.getValue().toArray(new Long[0])));
            }
            return list.iterator();
        }).groupByKey().mapValues(t -> {
            List<Long> list = new ArrayList<>();
            t.forEach(node -> list.addAll(Arrays.asList(node)));
            return list;
        });
        if (partitioner == null)
            return result.repartition(partition).persist(StorageLevel.MEMORY_AND_DISK());
        return result.partitionBy(partitioner).persist(StorageLevel.MEMORY_AND_DISK());
    }

    static void log(String text) {
        System.out.println("KTRUSS: " + text);
    }

    static void logDuration(String text, long millis) {
        log(text + ", duration " + millis / 1000 + " sec");
    }
}
