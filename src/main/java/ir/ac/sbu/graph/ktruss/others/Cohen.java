package ir.ac.sbu.graph.ktruss.others;

import ir.ac.sbu.graph.utils.GraphUtils;
import ir.ac.sbu.graph.clusteringco.CohenTC;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Cohen {
    public static void main(String[] args) throws FileNotFoundException {
        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 20;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);
        final int support = k - 2;

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "KTruss-Cohen-" + k, partition, inputPath);

        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class,
            Map.class, HashMap.class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        // Convert edges to the appropriate structure.
        JavaRDD<Tuple2<Long, Long>> currentEdges = input.map(line -> {
            if (line.startsWith("#"))
                    return null;
            String[] e = line.split("\\s+");
            long v1 = Long.parseLong(e[0]);
            long v2 = Long.parseLong(e[1]);
            if (v1 < v2)
                return new Tuple2<>(v1, v2);
            else if (v1 > v2)
                return new Tuple2<>(v2, v1);
            return null; // no self loop is accepted
        }).filter(t -> t != null);

        int iteration = 0;
        while (true) {
            JavaRDD<Tuple2<Long, Long>> edgesToExplore = currentEdges.repartition(partition).cache();
            System.out.println("iteration: " + ++iteration);
            JavaRDD<List<Tuple2<Long, Long>>> triangles = CohenTC.listTriangles(edgesToExplore, partition);

            // Extract triangles' edges
            JavaPairRDD<Tuple2<Long, Long>, Integer> triangleEdges = triangles.flatMapToPair(t -> {
                    List<Tuple2<Tuple2<Long, Long>, Integer>> edges = new ArrayList<>(3);
                    edges.add(new Tuple2<>(t.get(0), 1));
                    edges.add(new Tuple2<>(t.get(1), 1));
                    edges.add(new Tuple2<>(t.get(2), 1));
                    return edges.iterator();
                });

            JavaPairRDD<Tuple2<Long, Long>, Integer> edgeCount = triangleEdges.reduceByKey((a, b) -> a + b);
//            long oldEdgeCount = edgeCount.count();

            JavaPairRDD<Tuple2<Long, Long>, Boolean> newEdges = edgeCount.mapToPair(t -> {
                // If triangle count for the current edge is higher than the specified sup, then include that edge,
                // otherwise exclude the current edge.
                if (t._2 >= support)
                    return new Tuple2<>(t._1, true);
                return new Tuple2<>(t._1, false);
            });

            boolean reduction = newEdges.map(t -> t._2).reduce((a, b) -> a && b);
            System.out.println("Reduction: " + reduction);
            if (reduction)
                break;

            edgesToExplore.unpersist();
            // Select only the edge part as output.
            currentEdges = newEdges.filter(t -> t._2).map(t -> t._1);
        }

        System.out.println("Edges: " + currentEdges.distinct().count());
        sc.close();
    }

}