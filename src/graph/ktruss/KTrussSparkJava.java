package graph.ktruss;

import graph.GraphUtils;
import graph.clusteringco.FonlDegTC;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class KTrussSparkJava {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String inputPath = "input.txt";
        String outputPath = "/home/mehdi/graph-data/output-mapreduce";
        int k = 4; // k-truss

        if (args.length > 0)
            k = Integer.parseInt(args[0]);
        final int support = k - 2;

        if (args.length > 1)
            inputPath = args[1];

        SparkConf conf = new SparkConf();
        conf.setAppName("KTruss MapReduce");
        conf.setMaster("local[2]");
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, long[].class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int partition = 2;
        JavaRDD<Tuple3<Long, Long, Long>> triangles = FonlDegTC.listTriangles(sc, inputPath, partition);

        int iteration = 0;
        while (true) {
            System.out.println("iteration: " + ++iteration);
            triangles.persist(StorageLevel.MEMORY_ONLY());
            long triangleCount = triangles.count();
            System.out.println("Triangle Count: " + triangleCount);

            JavaPairRDD<KEdge, Integer> edgeCounts = triangles
                .flatMapToPair(new PairFlatMapFunction<Tuple3<Long, Long, Long>, KEdge, Integer>() {
                    @Override
                    public Iterable<Tuple2<KEdge, Integer>> call(Tuple3<Long, Long, Long> t) throws Exception {
                        List<Tuple2<KEdge, Integer>> list = new ArrayList<>(3);
                        list.add(new Tuple2<>(new KEdge(t._1(), t._2(), t._3()), 1));
                        list.add(new Tuple2<>(new KEdge(t._1(), t._3(), t._2()), 1));
                        list.add(new Tuple2<>(new KEdge(t._2(), t._3(), t._1()), 1));
                        return list;
                    }
                }).reduceByKey((a, b) -> a + b);

            JavaPairRDD<KEdge, Integer> invalidEdges = edgeCounts.filter(ec -> ec._2 < support);
            long invalidEdgeCount = invalidEdges.count();
            System.out.println("Invalid Edge Count: " + invalidEdgeCount);

            if (invalidEdgeCount == 0)
                break;

            JavaRDD<Tuple3<Long, Long, Long>> invalidTriangles = invalidEdges.map(t -> t._1.createTuple3()).distinct();
            System.out.println("Invalid Triangle Count: " + invalidTriangles.count());

            JavaRDD<Tuple3<Long, Long, Long>> newTriangles = triangles.subtract(invalidTriangles);

            triangles.unpersist();
            triangles = newTriangles;
        }

        JavaRDD<Tuple2<Long, Long>> edges = triangles.flatMap((FlatMapFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>>) t -> {
            List<Tuple2<Long, Long>> list = new ArrayList<>(3);
            list.add(new Tuple2<>(t._1(), t._2()));
            list.add(new Tuple2<>(t._1(), t._3()));
            list.add(new Tuple2<>(t._2(), t._3()));
            return list;
        }).distinct();

        System.out.println("Remaining graph edge count: " + edges.count());
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

}
