package ir.ac.sbu.graph.ktruss.spark;

import ir.ac.sbu.graph.utils.GraphUtils;
import ir.ac.sbu.graph.utils.GraphUtils.VertexDegreeInt;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.util.*;

import static ir.ac.sbu.graph.utils.Log.log;

public class Cohen extends KTruss {

    public Cohen(KTrussConf conf) {
        super(conf);
    }

    private JavaRDD<List<Tuple2<Integer, Integer>>> listTriangles(JavaRDD<Tuple2<Integer, Integer>> edgeList) {
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> edgeVertexDegree = edgeList.flatMapToPair(edge -> {
            List<Tuple2<Integer, Tuple2<Integer, Integer>>> list = new ArrayList<>(2);
            list.add(new Tuple2<>(edge._1, edge));
            list.add(new Tuple2<>(edge._2, edge));
            return list.iterator();
        }).groupByKey()
            .flatMapToPair(ve -> {
                List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> list = new ArrayList<>();
                Iterator<Tuple2<Integer, Integer>> it = ve._2.iterator();

                // Calculate degree and construct output key-value.
                HashSet<Tuple2<Integer, Integer>> edges = new HashSet<>();
                while (it.hasNext()) {
                    Tuple2<Integer, Integer> e = it.next();
                    edges.add(e);
                }

                Tuple2<Integer, Integer> vd = new Tuple2<>(ve._1, edges.size());
                for (Tuple2<Integer, Integer> edge : edges)
                    list.add(new Tuple2<>(edge, vd));

                if (list.size() <= 1)
                    return Collections.emptyIterator();

                return list.iterator();
            });

        // Find each edge and its corresponding degree of vertices. Here, key is an edge and value includes two integer that first one
        // is the degree of first vertex of the edge and second one is the degree of the second vertex of the edge.
        JavaPairRDD<Tuple2<Integer, Integer>, Integer[]> edge2Degrees = edgeVertexDegree.groupByKey()
            .mapToPair(e -> {
                Tuple2<Integer, Integer> edge = e._1;  // edge as key.

                // There should be degree information of two vertices of the current edge.
                Iterator<Tuple2<Integer, Integer>> iter = e._2.iterator();
                Integer[] degrees = new Integer[2];

                iter.hasNext();
                Tuple2<Integer, Integer> vd1 = iter.next();  // A VertexDegree object contains a vertex and its degree.
                if (vd1 == null)
                    throw new NullPointerException("Vertex1 degree for edge " + edge + " is null!!!");

                iter.hasNext();
                Tuple2<Integer, Integer> vd2 = iter.next();

                // If this VertexDegree is related to first vertex of the edge
                // then add degree to the first index else add it to the second one.
                if (vd1._1 == edge._1) {
                    degrees[0] = vd1._2;
                    degrees[1] = vd2._2;
                } else {
                    degrees[0] = vd2._2;
                    degrees[1] = vd1._2;
                }
                return new Tuple2<>(edge, degrees);
            });

//        edge2Degrees.mapToPair(e -> {
//            if (e._2[0] < e._2[1]) // if the degree of the first vertex is lower
//                return new Tuple2<>(e._1._1, e._1);
//            return new Tuple2<>(e._1._2, e._1);
//        }).groupByKey().flatMapToPair(t -> {
//            List<Tuple2<Integer, Integer>> list = new ArrayList<>();
//            for (Tuple2<Integer, Integer> e : t._2) {
//                list.add(e);
//
//            }
//
//            Integer v = t._1;
//            List<Tuple2<Integer, Integer>> output = new ArrayList<>();
//            for (int i = 0; i < list.size(); i++) {
//                Tuple2<Integer, Integer> e1 = list.get(i);
//                int u = e1._1 == v ? e1._2 : e1._1;
//                for (int j = i; j < list.size(); j++) {
//                    Tuple2<Integer, Integer> e2 = list.get(j);
//                    int w = e2._1 == v ? e2._2 : e2._1;
//                }
//                output.add(new Tuple2<>())
//            }
//
//        });



        // Extract every two connected edges. key: degree based sorted, value: vertex based sorted
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> connectedEdges = edge2Degrees
            .mapToPair(e -> {
                if (e._2[0] > e._2[1])
                    return new Tuple2<>(new Tuple2<>(e._1._2, e._1._1), e._1);
                else if (e._2[0] == e._2[1]) {
                    if (e._1._1 > e._1._2)
                        return new Tuple2<>(new Tuple2<>(e._1._2, e._1._1), e._1);
                    else
                        return new Tuple2<>(e._1, e._1);
                }
                return new Tuple2<>(e._1, e._1);
            }).repartition(conf.partitionNum).cache();

        // Filter out vertices with higher degree.
        JavaPairRDD<Integer, VertexDegreeInt[]> filteredVertexDegree = edge2Degrees
            .mapToPair(e -> {
                VertexDegreeInt[] vd = new VertexDegreeInt[2];
                vd[0] = new VertexDegreeInt();
                vd[0].vertex = e._1._1;
                vd[0].degree = e._2[0];

                vd[1] = new VertexDegreeInt();
                vd[1].vertex = e._1._2;
                vd[1].degree = e._2[1];

                if (vd[0].degree <= vd[1].degree)
                    return new Tuple2<>(vd[0].vertex, vd);
                return new Tuple2<>(vd[1].vertex, vd);
            });

        // Extract two connected edges as value and a synthetic edge constructed from uncommon vertices of these two edges.
        // Each key-value should contain an (synthetic) edge as key and two edges as value.
        // To do so, group all edges of a vertex together. Synthetic edges are created by all combinations of the aggregated edges.
        JavaPairRDD<Tuple2<Integer, Integer>, List<Tuple2<Integer, Integer>>> syntheticEdges = filteredVertexDegree.groupByKey()
            .flatMapToPair(e -> {
                List<Tuple2<Tuple2<Integer, Integer>, List<Tuple2<Integer, Integer>>>> list = new ArrayList<>();
                Integer keyVertex = e._1;

                // First, add all edges to a list.
                Iterator<VertexDegreeInt[]> it = e._2.iterator();
                List<VertexDegreeInt[]> edges = new ArrayList<>();
                while (it.hasNext())
                    edges.add(it.next());

                // If size of the "list" variable exceed from the maximum JVM memory, a JavaHeapSpace error will be thrown, causes the
                // program to be crashed.
                for (int i = 0; i < edges.size(); i++)
                    for (int j = i + 1; j < edges.size(); j++) {
                        // Select two edges.
                        List<Tuple2<Integer, Integer>> simpleEdges = new ArrayList<>(2);
                        VertexDegreeInt edge1_vd1 = edges.get(i)[0];
                        VertexDegreeInt edge1_vd2 = edges.get(i)[1];
                        VertexDegreeInt edge2_vd1 = edges.get(j)[0];
                        VertexDegreeInt edge2_vd2 = edges.get(j)[1];

                        if (edge1_vd1.vertex < edge1_vd2.vertex)
                            simpleEdges.add(new Tuple2<>(edge1_vd1.vertex, edge1_vd2.vertex));
                        else
                            simpleEdges.add(new Tuple2<>(edge1_vd2.vertex, edge1_vd1.vertex));

                        if (edge2_vd1.vertex < edge2_vd2.vertex)
                            simpleEdges.add(new Tuple2<>(edge2_vd1.vertex, edge2_vd2.vertex));
                        else
                            simpleEdges.add(new Tuple2<>(edge2_vd2.vertex, edge2_vd1.vertex));

                        // Find uncommon vertices of the selected two edges in the previous instructions.
                        VertexDegreeInt vd1;
                        VertexDegreeInt vd2;
                        if (edge1_vd1.vertex != keyVertex)
                            vd1 = edge1_vd1;
                        else
                            vd1 = edge1_vd2;

                        if (edge2_vd1.vertex != keyVertex)
                            vd2 = edge2_vd1;
                        else
                            vd2 = edge2_vd2;

                        Tuple2<Integer, Integer> syntheticEdge;
                        if (vd1.degree < vd2.degree)
                            syntheticEdge = new Tuple2<>(vd1.vertex, vd2.vertex);
                        else if (vd1.degree == vd2.degree) {
                            if (vd1.vertex < vd2.vertex)
                                syntheticEdge = new Tuple2<>(vd1.vertex, vd2.vertex);
                            else
                                syntheticEdge = new Tuple2<>(vd2.vertex, vd1.vertex);
                        } else
                            syntheticEdge = new Tuple2<>(vd2.vertex, vd1.vertex);

                        list.add(new Tuple2<>(syntheticEdge, simpleEdges));
                    }

                if (list.size() == 0)
                    Collections.emptyIterator();

                return list.iterator();
            });

        // Find triangles
        return syntheticEdges.cogroup(connectedEdges, conf.partitionNum).flatMap(m -> {
            Tuple2<Integer, Integer> e1;
            Tuple2<Integer, Integer> e2;
            Tuple2<Integer, Integer> e3;
            List<List<Tuple2<Integer, Integer>>> list = new ArrayList<>();

            // If syntheticEdges and connectedEdges of the same key (edge) contain some values at the same time, then a triangle would
            // be created as a 3-length array of the Edge
            Iterator<Tuple2<Integer, Integer>> oneEdgeIter = m._2._2.iterator();
            if (!oneEdgeIter.hasNext())
                return Collections.emptyIterator();

            e1 = m._1._1 < m._1._2 ? m._1 : new Tuple2<>(m._1._2, m._1._1);
            Iterator<List<Tuple2<Integer, Integer>>> twoEdgesIter = m._2._1.iterator();
            while (twoEdgesIter.hasNext()) {
                List<Tuple2<Integer, Integer>> edges = twoEdgesIter.next();
                e2 = edges.get(0)._1 < edges.get(0)._2 ? edges.get(0) : new Tuple2<>(edges.get(0)._2, edges.get(0)._1);
                e3 = edges.get(1)._1 < edges.get(1)._2 ? edges.get(1) : new Tuple2<>(edges.get(1)._2, edges.get(1)._1);

                List<Tuple2<Integer, Integer>> triangleEdges = Arrays.asList(e1, e2, e3);
                list.add(triangleEdges);
            }

            if (list.size() == 0)
                return Collections.emptyIterator();

            return list.iterator();
        });

    }

    private long start(JavaRDD<Tuple2<Integer, Integer>> edgeList) {

        final int minSup = conf.k - 2;

        int iteration = 0;
        while (true) {
            log("Iteration: " + ++iteration);
            JavaRDD<List<Tuple2<Integer, Integer>>> triangles = listTriangles(edgeList);
            log("Triangle size: " + triangles.count());

            // Extract triangles' edges
            JavaPairRDD<Tuple2<Integer, Integer>, Integer> edgeCount = triangles.flatMapToPair(t -> {
                List<Tuple2<Tuple2<Integer, Integer>, Integer>> edges = new ArrayList<>(3);
                edges.add(new Tuple2<>(t.get(0), 1));
                edges.add(new Tuple2<>(t.get(1), 1));
                edges.add(new Tuple2<>(t.get(2), 1));
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