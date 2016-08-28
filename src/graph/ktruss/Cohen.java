package graph.ktruss;

import graph.GraphUtils;
import graph.clusteringco.CohenTC;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.*;

public class Cohen {
    public static void main(String[] args) throws FileNotFoundException {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 20;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");

        final int support = 2;

        GraphUtils.setAppName(conf, "Cohen-TC", partition, inputPath);
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
                // If triangle count for the current edge is higher than the specified support, then include that edge,
                // otherwise exclude the current edge.
                if (t._2 >= support)
                    return new Tuple2<>(t._1, true);
                return new Tuple2<>(t._1, false);
            });

            boolean reduction = newEdges.map(t -> t._2).reduce((a, b) -> a && b);
            System.out.println("Reduction: " + reduction);
            if (reduction)
                break;

            // Select only the edge part as output.
            currentEdges = newEdges.filter(t -> t._2).map(t -> t._1);
            long edges = currentEdges.map(t-> 1).reduce((a, b) -> a + b);
            System.out.println("Edges: " + edges);
        }

        System.out.println("Edges: " + currentEdges.distinct().count());
        sc.close();
    }

}