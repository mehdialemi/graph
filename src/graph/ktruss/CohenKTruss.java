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
            // Find vertices and their corresponding edges. A key-value item contains a vertex as key and an edge as value.
            JavaPairRDD<Long, Tuple2<Long, Long>> vertexEdge = currentEdges.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, Long>, Long, Tuple2<Long, Long>>() {
                    @Override
                    public Iterable<Tuple2<Long, Tuple2<Long, Long>>> call(Tuple2<Long, Long> edge) throws
                        Exception {
                        List<Tuple2<Long, Tuple2<Long, Long>>> list = new ArrayList<>(2);

                        list.add(new Tuple2<>(edge._1, edge));
                        list.add(new Tuple2<>(edge._2, edge));
                        return list;
                    }
                }).repartition(partition);

            // Find an edge and degree related to one of its vertices.
            // First, collect all edges connected to a vertex by groupByKey() operator.
            // Second, since all neighbors of the current vertex are aggregated here, calculate degree of the vertex.
            // Third, construct key-value <Edge, VertexDegree> which each edge of the current vertex is as a key (Edge) and
            // the value (VertexDegree) is the degree of the current vertex. Here, values for all the keys are the same.
            JavaPairRDD<Tuple2<Long, Long>, Tuple2<Long, Long>> edgeVertexDegree = vertexEdge.groupByKey()
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Tuple2<Long, Long>>>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>> call(Tuple2<Long, Iterable<Tuple2<Long, Long>>> ve) throws
                        Exception {
                        List<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>> list = new ArrayList<>();
                        int degree = 0;
                        Iterator<Tuple2<Long, Long>> iter = ve._2.iterator();

                        Tuple2<Long, Long> vd = new Tuple2<>(ve._1, 0L);

                        // Calculate degree and construct output key-value.
                        Map<Tuple2<Long, Long>, Tuple2<Long, Long>> map = new HashMap<>();
                        while (iter.hasNext()) {
                            Tuple2<Long, Long> e = iter.next();
                            if (map.containsKey(e))
                                continue;
                            map.put(e, vd);
                            degree++;
                        }

                        // Assign degree of the current vertex to all edges.
                        Set<Map.Entry<Tuple2<Long, Long>, Tuple2<Long, Long>>> entrySet = map.entrySet();
                        for (Map.Entry<Tuple2<Long, Long>, Tuple2<Long, Long>> entry : entrySet) {
                            Tuple2<Long, Long> vertexDegree = entry.getValue();
                            list.add(new Tuple2<>(entry.getKey(), new Tuple2<>(vertexDegree._1, vertexDegree._2)));
                        }
                        return list;
                    }
                }).repartition(partition);

            // Find each edge and its corresponding degree of vertices. Here, key is an edge and value includes two integer that first one
            // is the degree of first vertex of the edge and second one is the degree of the second vertex of the edge.
            JavaPairRDD<Tuple2<Long, Long>, Long[]> edgeVertex2Degree = edgeVertexDegree
                .groupByKey()
                .mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Iterable<Tuple2<Long, Long>>>, Tuple2<Long,
                    Long>, Long[]>() {
                    @Override
                    public Tuple2<Tuple2<Long, Long>, Long[]> call(Tuple2<Tuple2<Long, Long>, Iterable<Tuple2<Long,
                        Long>>> e) throws
                        Exception {
                        Tuple2<Long, Long> edge = e._1;  // edge as key.

                        // There should be degree information of two vertices of the current edge.
                        Iterator<Tuple2<Long, Long>> iter = e._2.iterator();
                        Long[] degrees = new Long[2];

                        iter.hasNext();
                        Tuple2<Long, Long> vd = iter.next();  // A VertexDegree object contains a vertex and its degree.

                        // If this VertexDegree is related to first vertex of the edge
                        // then add degree to the first index else add it to the second one.
                        if (vd._1 == edge._1)
                            degrees[0] = vd._2;
                        else
                            degrees[1] = vd._2;

                        iter.hasNext();
                        vd = iter.next();
                        // Again find appropriate index for the second VertexDegree.
                        if (vd._1 == edge._1)
                            degrees[0] = vd._2;
                        else
                            degrees[1] = vd._2;

                        return new Tuple2<>(edge, degrees);
                    }
                });

            // Filter out vertices with higher degree.
            JavaPairRDD<Long, List<Tuple2<Long, Long>>> filteredVertexDegree = edgeVertex2Degree
                .mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long[]>, Long, List<Tuple2<Long, Long>>>() {
                    @Override
                    public Tuple2<Long, List<Tuple2<Long, Long>>> call(Tuple2<Tuple2<Long, Long>, Long[]> e) throws
                        Exception {
                        List<Tuple2<Long, Long>> vd = new ArrayList<>(2);
                        vd.add(new Tuple2<>(e._1._1, e._2[0]));
                        vd.add(new Tuple2<>(e._1._2, e._2[1]));

                        if (e._2[0] <= e._2[1])
                            return new Tuple2<>(e._1._1, vd);
                        return new Tuple2<>(e._1._2, vd);
                    }
                }).repartition(partition);

            // Extract two connected edges as value and a synthetic edge constructed from uncommon vertices of these two edges.
            // Each key-value should contain an (synthetic) edge as key and two edges as value.
            // To do so, group all edges of a vertex together. Synthetic edges are created by all combinations of the aggregated edges.
            JavaPairRDD<Tuple2<Long, Long>, List<Tuple2<Long, Long>>> syntheticEdges = filteredVertexDegree.groupByKey()
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<List<Tuple2<Long, Long>>>>, Tuple2<Long, Long>,
                    List<Tuple2<Long, Long>>>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Long, Long>, List<Tuple2<Long, Long>>>> call(
                        Tuple2<Long, Iterable<List<Tuple2<Long, Long>>>> e)
                        throws Exception {
                        List<Tuple2<Tuple2<Long, Long>, List<Tuple2<Long, Long>>>> list = new ArrayList<>();
                        Long key = e._1;

                        // First, add all edges to a list.
                        Iterator<List<Tuple2<Long, Long>>> iter = e._2.iterator();
                        List<List<Tuple2<Long, Long>>> edges = new ArrayList<>();
                        while (iter.hasNext())
                            edges.add(iter.next());

                        // If size of the "list" variable exceed from the maximum JVM memory, a JavaHeapSpace error will be thrown, causes the
                        // program to be crashed.
                        for (int i = 0; i < edges.size(); i++)
                            for (int j = i + 1; j < edges.size(); j++) {
                                // Select two edges.
                                List<Tuple2<Long, Long>> simpleEdges = new ArrayList<>(2);
                                Tuple2<Long, Long> vd1_1 = edges.get(i).get(0);
                                Tuple2<Long, Long> vd1_2 = edges.get(i).get(1);
                                Tuple2<Long, Long> vd2_1 = edges.get(j).get(0);
                                Tuple2<Long, Long> vd2_2 = edges.get(j).get(1);

                                simpleEdges.add(new Tuple2<>(vd1_1._1, vd1_2._2));
                                simpleEdges.add(new Tuple2<>(vd2_1._1, vd2_2._2));

                                // Find uncommon vertices of the selected two edges in the previous instructions.
                                Tuple2<Long, Long> vd1;
                                Tuple2<Long, Long> vd2;
                                if (vd1_1._1 != key)
                                    vd1 = vd1_1;
                                else
                                    vd1 = vd1_2;

                                if (vd2_1._1 != key)
                                    vd2 = vd2_1;
                                else
                                    vd2 = vd2_2;

                                Tuple2<Long, Long> syntheticEdge;
                                if (vd1._2 < vd2._2)
                                    syntheticEdge = new Tuple2<>(vd1._1, vd2._1);
                                else if (vd1._2 == vd2._2) {
                                    if (vd1._1 < vd2._1)
                                        syntheticEdge = new Tuple2<>(vd1._1, vd2._1);
                                    else
                                        syntheticEdge = new Tuple2<>(vd2._1, vd1._1);
                                } else
                                    syntheticEdge = new Tuple2<>(vd2._1, vd1._1);

                                list.add(new Tuple2<>(syntheticEdge, simpleEdges));
                            }
                        return list;
                    }
                });

            syntheticEdges.persist(StorageLevel.DISK_ONLY());

            // Extract every two connected edges.
            JavaPairRDD<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedEdges = edgeVertex2Degree
                .mapToPair(new PairFunction<Tuple2<Tuple2<Long, Long>, Long[]>, Tuple2<Long, Long>, Tuple2<Long, Long>>
                    () {
                    @Override
                    public Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> call(Tuple2<Tuple2<Long, Long>, Long[]> e)
                        throws Exception {
                        if (e._2[0] > e._2[1])
                            return new Tuple2<>(new Tuple2<>(e._1._2, e._1._1), e._1);
                        else if (e._2[0] == e._2[1]) {
                            if (e._1._1 > e._1._2)
                                return new Tuple2<>(new Tuple2<>(e._1._2, e._1._1), e._1);
                            else
                                return new Tuple2<>(e._1, e._1);
                        }
                        return new Tuple2<>(e._1, e._1);
                    }
                });

            // Find triangles
            JavaRDD<List<Tuple2<Long, Long>>> triangles = syntheticEdges.cogroup(connectedEdges).flatMap(m -> {
                Tuple2<Long, Long> e1;
                Tuple2<Long, Long> e2;
                Tuple2<Long, Long> e3;
                List<List<Tuple2<Long, Long>>> list = new ArrayList<>();

                // If syntheticEdges and connectedEdges of the same key (edge) contain some values at the same time, then a triangle would
                // be created as a 3-length array of the Edge
                Iterator<Tuple2<Long, Long>> oneEdgeIter = m._2._2.iterator();
                if (!oneEdgeIter.hasNext())
                    return list;

                e1 = m._1;
                Iterator<List<Tuple2<Long, Long>>> twoEdgesIter = m._2._1.iterator();
                while (twoEdgesIter.hasNext()) {
                    List<Tuple2<Long, Long>> edges = twoEdgesIter.next();
                    e2 = edges.get(0);
                    e3 = edges.get(1);

                    List<Tuple2<Long, Long>> triangleEdges = Arrays.asList(e1, e2, e3);
                    list.add(triangleEdges);
                }

                return list;
            });

            // Extract triangles' edges
            JavaPairRDD<Tuple2<Long, Long>, Integer> triangleEdges = triangles
                .flatMapToPair(new PairFlatMapFunction<List<Tuple2<Long, Long>>, Tuple2<Long, Long>, Integer>() {
                    @Override
                    public Iterable<Tuple2<Tuple2<Long, Long>, Integer>> call(List<Tuple2<Long, Long>> t) throws Exception {
                        List<Tuple2<Tuple2<Long, Long>, Integer>> edges = new ArrayList<>(3);
                        edges.add(new Tuple2<>(t.get(0), 1));
                        edges.add(new Tuple2<>(t.get(1), 1));
                        edges.add(new Tuple2<>(t.get(2), 1));
                        return edges;
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