package graph.clusteringco;

import graph.GraphUtils;
import graph.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Cohen Triangle listing implementation on Apache Spark
 */
public class CohenTC {
    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");

        GraphUtils.setAppName(conf, "Fonl-GCC-Deg", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class,
            Map.class, HashMap.class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        // Convert edges to the appropriate structure.
        JavaRDD<Edge> currentEdges = input.map(line -> {
            String[] e = line.split("\\s+");
            long v1 = Long.parseLong(e[0]);
            long v2 = Long.parseLong(e[1]);
            Edge edge = new Edge(v1, v2);
            return edge;
        });

            // Find vertices and their corresponding edges. A key-value item contains a vertex as key and an edge as value.
            JavaPairRDD<Long, Edge> vertexEdge = currentEdges.flatMapToPair(new PairFlatMapFunction<Edge, Long, Edge>() {
                @Override
                public Iterable<Tuple2<Long, Edge>> call(Edge edge) throws Exception {
                    List<Tuple2<Long, Edge>> list = new ArrayList<>(2);

                    list.add(new Tuple2<>(edge.v1, edge));
                    list.add(new Tuple2<>(edge.v2, edge));
                    return list;
                }
            });

            // Find an edge and degree related to one of its vertices.
            // First, collect all edges connected to a vertex by groupByKey() operator.
            // Second, since all neighbors of the current vertex are aggregated here, calculate degree of the vertex.
            // Third, construct key-value <Edge, VertexDegree> which each edge of the current vertex is as a key (Edge) and
            // the value (VertexDegree) is the degree of the current vertex. Here, values for all the keys are the same.
            JavaPairRDD<Edge, VertexDegree> edgeVertexDegree = vertexEdge.groupByKey()
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Edge>>, Edge, VertexDegree>() {
                    @Override
                    public Iterable<Tuple2<Edge, VertexDegree>> call(Tuple2<Long, Iterable<Edge>> ve) throws
                        Exception {
                        List<Tuple2<Edge, VertexDegree>> list = new ArrayList<>();
                        int degree = 0;
                        Iterator<Edge> iter = ve._2.iterator();

                        VertexDegree vd = new VertexDegree();
                        vd.v = ve._1;

                        // Calculate degree and construct output key-value.
                        while (iter.hasNext()) {
                            Edge e = iter.next();
                            Tuple2<Edge, VertexDegree> t = new Tuple2<>(e, vd);
                            list.add(t);
                            degree++;
                        }

                        // Assign degree of the current vertex to all edges.
                        for (Tuple2<Edge, VertexDegree> t : list) {
                            t._2.deg = degree;
                        }
                        return list;
                    }
                });

            // Find each edge and its corresponding degree of vertices. Here, key is an edge and value includes two integer that first one
            // is the degree of first vertex of the edge and second one is the degree of the second vertex of the edge.
            JavaPairRDD<Edge, Integer[]> edgeVertex2Degree = edgeVertexDegree.groupByKey()
                .mapToPair(new PairFunction<Tuple2<Edge, Iterable<VertexDegree>>, Edge, Integer[]>() {
                    @Override
                    public Tuple2<Edge, Integer[]> call(Tuple2<Edge, Iterable<VertexDegree>> e) throws
                        Exception {
                        Edge edge = e._1;  // edge as key.

                        // There should be degree information of two vertices of the current edge.
                        Iterator<VertexDegree> iter = e._2.iterator();
                        Integer[] degrees = new Integer[2];

                        iter.hasNext();
                        VertexDegree vd = iter.next();  // A VertexDegree object contains a vertex and its degree.

                        // If this VertexDegree is related to first vertex of the edge
                        // then add degree to the first index else add it to the second one.
                        if (vd.v == edge.v1)
                            degrees[0] = vd.deg;
                        else
                            degrees[1] = vd.deg;

                        iter.hasNext();
                        vd = iter.next();
                        // Again find appropriate index for the second VertexDegree.
                        if (vd.v == edge.v1)
                            degrees[0] = vd.deg;
                        else
                            degrees[1] = vd.deg;

                        return new Tuple2<>(edge, degrees);
                    }
                });

            // Filter out vertices with higher degree.
            JavaPairRDD<Long, VertexDegree[]> filteredVertexDegree = edgeVertex2Degree
                .mapToPair(new PairFunction<Tuple2<Edge, Integer[]>, Long, VertexDegree[]>() {
                    @Override
                    public Tuple2<Long, VertexDegree[]> call(Tuple2<Edge, Integer[]> e) throws Exception {
                        VertexDegree[] vd = new VertexDegree[2];
                        vd[0] = new VertexDegree();
                        vd[0].v = e._1.v1;
                        vd[0].deg = e._2[0];

                        vd[1] = new VertexDegree();
                        vd[1].v = e._1.v2;
                        vd[1].deg = e._2[1];

                        if (e._2[0] <= e._2[1])
                            return new Tuple2<>(e._1.v1, vd);
                        return new Tuple2<>(e._1.v2, vd);
                    }
                });

            // Extract two connected edges as value and a synthetic edge constructed from uncommon vertices of these two edges.
            // Each key-value should contain an (synthetic) edge as key and two edges as value.
            // To do so, group all edges of a vertex together. Synthetic edges are created by all combinations of the aggregated edges.
            JavaPairRDD<Edge, Edge[]> syntheticEdges = filteredVertexDegree.groupByKey()
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<VertexDegree[]>>, Edge, Edge[]>() {
                    @Override
                    public Iterable<Tuple2<Edge, Edge[]>> call(Tuple2<Long, Iterable<VertexDegree[]>> e)
                        throws Exception {
                        List<Tuple2<Edge, Edge[]>> list = new ArrayList<>();
                        Long key = e._1;

                        // First, add all edges to a list.
                        Iterator<VertexDegree[]> iter = e._2.iterator();
                        List<VertexDegree[]> edges = new ArrayList<>();
                        while (iter.hasNext())
                            edges.add(iter.next());

                        // If size of the "list" variable exceed from the maximum JVM memory, a JavaHeapSpace error will be thrown, causes the
                        // program to be crashed.
                        for (int i = 0; i < edges.size(); i++)
                            for (int j = i + 1; j < edges.size(); j++) {
                                // Select two edges.
                                Edge[] simpleEdges = new Edge[2];
                                VertexDegree vd1_1 = edges.get(i)[0];
                                VertexDegree vd1_2 = edges.get(i)[1];
                                VertexDegree vd2_1 = edges.get(j)[0];
                                VertexDegree vd2_2 = edges.get(j)[1];

                                simpleEdges[0] = new Edge(vd1_1.v, vd1_2.v);
                                simpleEdges[1] = new Edge(vd2_1.v, vd2_2.v);

                                // Find uncommon vertices of the selected two edges in the previous instructions.
                                VertexDegree vd1;
                                VertexDegree vd2;
                                if (vd1_1.v != key)
                                    vd1 = vd1_1;
                                else
                                    vd1 = vd1_2;

                                if (vd2_1.v != key)
                                    vd2 = vd2_1;
                                else
                                    vd2 = vd2_2;

                                Edge syntheticEdge;
                                if (vd1.deg < vd2.deg)
                                    syntheticEdge = new Edge(vd1.v, vd2.v);
                                else if (vd1.deg == vd2.deg) {
                                    if (vd1.v < vd2.v)
                                        syntheticEdge = new Edge(vd1.v, vd2.v);
                                    else
                                        syntheticEdge = new Edge(vd2.v, vd1.v);
                                } else
                                    syntheticEdge = new Edge(vd2.v, vd1.v);

                                list.add(new Tuple2<>(syntheticEdge, simpleEdges));
                            }
                        return list;
                    }
                });

            // Extract every two connected edges.
            JavaPairRDD<Edge, Edge> connectedEdges = edgeVertex2Degree
                .mapToPair(new PairFunction<Tuple2<Edge, Integer[]>, Edge, Edge>() {
                    @Override
                    public Tuple2<Edge, Edge> call(Tuple2<Edge, Integer[]> e) throws Exception {
                        if (e._2[0] > e._2[1])
                            return new Tuple2<>(e._1.reverse(), e._1);
                        else if (e._2[0] == e._2[1]) {
                            if (e._1.v1 > e._1.v2)
                                return new Tuple2<>(e._1.reverse(), e._1);
                            else
                                return new Tuple2<>(e._1, e._1);
                        }
                        return new Tuple2<>(e._1, e._1);
                    }
                });

            // Find triangles
            JavaRDD<Edge[]> triangles = syntheticEdges.cogroup(connectedEdges).flatMap(m -> {
                Edge e1;
                Edge e2;
                Edge e3;
                List<Edge[]> list = new ArrayList<>();

                // If syntheticEdges and connectedEdges of the same key (edge) contain some values at the same time, then a triangle would
                // be created as a 3-length array of the Edge
                Iterator<Edge> oneEdgeIter = m._2._2.iterator();
                if (!oneEdgeIter.hasNext())
                    return list;

                e1 = m._1;
                Iterator<Edge[]> twoEdgesIter = m._2._1.iterator();
                while (twoEdgesIter.hasNext()) {
                    Edge[] edges = twoEdgesIter.next();
                    e2 = edges[0];
                    e3 = edges[1];

                    Edge[] triangleEdges = new Edge[]{e1, e2, e3};
                    list.add(triangleEdges);
                }

                return list;
            });

        long totalTriangles = triangles.count();
        OutUtils.printOutputTC(totalTriangles);
        sc.stop();
    }

    static class VertexDegree implements Serializable {
        private static final long serialVersionUID = 1L;

        long v;
        int deg;

        @Override
        public String toString() {
            return "v: " + v + " deg: " + deg;
        }
    }

    static class Edge implements Serializable {
        private static final long serialVersionUID = 1L;

        public long v1;
        public long v2;

        public Edge(long v1, long v2) {
            this.v1 = v1;
            this.v2 = v2;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            Edge e = (Edge) obj;
            return (v1 == e.v1 && v2 == e.v2) || (v1 == e.v2 && v2 == e.v1);
        }

        @Override
        public int hashCode() {
            return (int) (v1 + v2);
        }

        public Edge reverse() {
            return new Edge(v2, v1);
        }

        @Override
        public String toString() {
            return "(" + v1 + "," + v2 + ")";
        }
    }
}
