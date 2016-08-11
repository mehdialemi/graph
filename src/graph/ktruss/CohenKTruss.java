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

public class CohenKTruss {
    public static void main(String[] args) throws FileNotFoundException {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
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

        while (true) {
            JavaRDD<List<Tuple2<Long, Long>>> triangles = CohenTC.listTriangles(currentEdges, partition);

            // Extract triangles' edges
            JavaPairRDD<Tuple2<Long, Long>, Integer> triangleEdges = triangles
                .flatMapToPair(new PairFlatMapFunction<List<Tuple2<Long, Long>>, Tuple2<Long, Long>, Integer>() {
                    @Override
                    public Iterator<Tuple2<Tuple2<Long, Long>, Integer>> call(List<Tuple2<Long, Long>> t) throws
                        Exception {
                        List<Tuple2<Tuple2<Long, Long>, Integer>> edges = new ArrayList<>(3);
                        edges.add(new Tuple2<>(t.get(0), 1));
                        edges.add(new Tuple2<>(t.get(1), 1));
                        edges.add(new Tuple2<>(t.get(2), 1));
                        return edges.iterator();
                    }
                });

            JavaPairRDD<Tuple2<Long, Long>, Integer> edgeCount = triangleEdges.reduceByKey((a, b) -> a + b);

            JavaRDD<Tuple2<Long, Long>> newEdges = edgeCount.filter(t -> {
                // If triangle count for the current edge is higher than the specified support, then include that edge,
                // otherwise exclude the current edge.
                if (t._2 >= support)
                    return true;
                return false;
            }).map(e -> e._1); // Select only the edge part as output.

            long newEdgeCount = newEdges.count();
            long oldEdgeCount = edgeCount.count();

            // If no edge was filtered, then we all done and currentEdges represent k-truss subgraphs.
            if (newEdgeCount == oldEdgeCount) {
                break;
            }

            currentEdges.unpersist();
            currentEdges = newEdges;
            currentEdges.persist(StorageLevel.MEMORY_AND_DISK());
        }
    }

}