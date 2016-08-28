package graph.ktruss;

import graph.GraphUtils;
import graph.clusteringco.FonlDegTC;
import graph.clusteringco.FonlUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class RebuildTriangles {

    public static void main(String[] args) {
//        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
        String outputPath = "/home/mehdi/graph-data/output-mapreduce";
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
        while (true) {
            long t1 = System.currentTimeMillis();
            log("iteration: " + ++iteration);

            JavaPairRDD<KEdge, Integer> edgeCounts = triangles.flatMapToPair(t -> {
                List<Tuple2<KEdge, Integer>> list = new ArrayList<>(3);
                list.add(new Tuple2<>(new KEdge(t._1(), t._2(), t._3()), 1));
                list.add(new Tuple2<>(new KEdge(t._1(), t._3(), t._2()), 1));
                list.add(new Tuple2<>(new KEdge(t._2(), t._3(), t._1()), 1));
                return list.iterator();
            }).reduceByKey((a, b) -> a + b);
            edgeCounts.repartition(partition);
            long t2 = System.currentTimeMillis();

            JavaPairRDD<KEdge, Integer> invalidEdges = edgeCounts.filter(ec -> ec._2 < support);
            invalidEdges.repartition(partition);
            long invalidEdgeCount = invalidEdges.count();
            long t3 = System.currentTimeMillis();
            logDuration("Invalid Edge Count: " + invalidEdgeCount, t3 - t2);

            if (invalidEdgeCount == 0)
                break;

            JavaRDD<Tuple3<Long, Long, Long>> invalidTriangles = invalidEdges.distinct().map(t -> t._1.createTuple3());

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
        logDuration("Remaining graph edge count: " + edges.count(), duration);
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

    static void log(String text) {
        System.out.println("KTRUSS: " + text);
    }

    static void logDuration(String text, long millis) {
        log(text + ", duration " + millis / 1000 + " sec");
    }
}
