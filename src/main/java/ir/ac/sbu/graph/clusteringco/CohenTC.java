package ir.ac.sbu.graph.clusteringco;

import ir.ac.sbu.graph.GraphLoader;
import ir.ac.sbu.graph.GraphUtils;
import ir.ac.sbu.graph.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

import static ir.ac.sbu.graph.GraphUtils.VertexDegree;

/**
 * Cohen TriangleParallel listing implementation on Apache Spark
 */
public class CohenTC {
    public static void main(String[] args) {
        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 20;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");

        GraphUtils.setAppName(conf, "Cohen-TC", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class,
            Map.class, HashMap.class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaRDD<Tuple2<Long, Long>> currentEdges = GraphLoader.loadEdgeListSorted(input);

        JavaRDD<List<Tuple2<Long, Long>>> triangles = listTriangles(currentEdges, partition);

        long totalTriangles = triangles.count();
        OutUtils.printOutputTC(totalTriangles);
        sc.stop();
    }

    public static JavaRDD<List<Tuple2<Long, Long>>> listTriangles(JavaRDD<Tuple2<Long, Long>> currentEdges, int partition) {

        // Find vertices and their corresponding edges. A key-value item contains a vertex as key and an edge as value.
        JavaPairRDD<Long, Tuple2<Long, Long>> vertexEdge = currentEdges.flatMapToPair(edge -> {
                List<Tuple2<Long, Tuple2<Long, Long>>> list = new ArrayList<>(2);
                list.add(new Tuple2<>(edge._1, edge));
                list.add(new Tuple2<>(edge._2, edge));
                return list.iterator();
            });

        // Find an edge and degree related to one of its vertices.
        // First, collect all edges connected to a vertex by groupByKey() operator.
        // Second, since all neighbors of the current vertex are aggregated here, calculate degree of the vertex.
        // Third, construct key-value <Edge, VertexDegree> which each edge of the current vertex is as a key (Edge) and
        // the value (VertexDegree) is the degree of the current vertex. Here, values for all the keys are the same.
        JavaPairRDD<Tuple2<Long, Long>, VertexDegree> edgeVertexDegree = vertexEdge.groupByKey()
            .flatMapToPair(ve -> {
                List<Tuple2<Tuple2<Long, Long>, VertexDegree>> list = new ArrayList<>();
                Iterator<Tuple2<Long, Long>> it = ve._2.iterator();

                // Calculate degree and construct output key-value.
                HashSet<Tuple2<Long, Long>> edges = new HashSet<>();
                while (it.hasNext()) {
                    Tuple2<Long, Long> e = it.next();
                    edges.add(e);
                }

                VertexDegree vd = new VertexDegree();
                vd.vertex = ve._1;
                vd.degree = edges.size();

                if (vd.degree == 0)
                    throw new Exception("Vertex " + vd.vertex + " has no edge!!!");

                for (Tuple2<Long, Long> edge : edges)
                    list.add(new Tuple2<>(edge, vd));

                if (list.size() == 0)
                    return Collections.emptyIterator();

                return list.iterator();
            });

        // Find each edge and its corresponding degree of vertices. Here, key is an edge and value includes two integer that first one
        // is the degree of first vertex of the edge and second one is the degree of the second vertex of the edge.
        JavaPairRDD<Tuple2<Long, Long>, Integer[]> edgeVertex2Degree = edgeVertexDegree.groupByKey()
            .mapToPair(e -> {
                Tuple2<Long, Long> edge = e._1;  // edge as key.

                // There should be degree information of two vertices of the current edge.
                Iterator<VertexDegree> iter = e._2.iterator();
                Integer[] degrees = new Integer[2];

                iter.hasNext();
                VertexDegree vd1 = iter.next();  // A VertexDegree object contains a vertex and its degree.
                if (vd1 == null)
                    throw new NullPointerException("Vertex1 degree for edge " + edge + " is null!!!");

                iter.hasNext();
                VertexDegree vd2 = iter.next();
                if (vd2 == null)
                    throw new NullPointerException("Vertex2 degree for edge " + edge + " is null!!!");

                if (vd1 == vd2)
                    throw new NullPointerException("Self loop detected for edge " + edge);

                // If this VertexDegree is related to first vertex of the edge
                // then add degree to the first index else add it to the second one.
                if (vd1.vertex == edge._1) {
                    degrees[0] = vd1.degree;
                    degrees[1] = vd2.degree;
                } else {
                    degrees[0] = vd2.degree;
                    degrees[1] = vd1.degree;
                }
                return new Tuple2<>(edge, degrees);
            });

        // Extract every two connected edges. key: degree based sorted, value: vertex based sorted
        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Long, Long>> connectedEdges = edgeVertex2Degree
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
            });

        connectedEdges.persist(StorageLevel.MEMORY_AND_DISK());

        // Filter out vertices with higher degree.
        JavaPairRDD<Long, VertexDegree[]> filteredVertexDegree = edgeVertex2Degree
            .mapToPair(e -> {
                VertexDegree[] vd = new VertexDegree[2];
                vd[0] = new VertexDegree();
                vd[0].vertex = e._1._1;
                vd[0].degree = e._2[0];

                vd[1] = new VertexDegree();
                vd[1].vertex = e._1._2;
                vd[1].degree = e._2[1];

                if (vd[0].degree <= vd[1].degree)
                    return new Tuple2<>(vd[0].vertex, vd);
                return new Tuple2<>(vd[1].vertex, vd);
            });

        // Extract two connected edges as value and a synthetic edge constructed from uncommon vertices of these two edges.
        // Each key-value should contain an (synthetic) edge as key and two edges as value.
        // To do so, group all edges of a vertex together. Synthetic edges are created by all combinations of the aggregated edges.
        JavaPairRDD<Tuple2<Long, Long>, List<Tuple2<Long, Long>>> syntheticEdges = filteredVertexDegree.groupByKey()
            .flatMapToPair(e -> {
                List<Tuple2<Tuple2<Long, Long>, List<Tuple2<Long, Long>>>> list = new ArrayList<>();
                Long keyVertex = e._1;

                // First, add all edges to a list.
                Iterator<VertexDegree[]> it = e._2.iterator();
                List<VertexDegree[]> edges = new ArrayList<>();
                while (it.hasNext())
                    edges.add(it.next());

                // If size of the "list" variable exceed from the maximum JVM memory, a JavaHeapSpace error will be thrown, causes the
                // program to be crashed.
                for (int i = 0; i < edges.size(); i++)
                    for (int j = i + 1; j < edges.size(); j++) {
                        // Select two edges.
                        List<Tuple2<Long, Long>> simpleEdges = new ArrayList<>(2);
                        VertexDegree edge1_vd1 = edges.get(i)[0];
                        VertexDegree edge1_vd2 = edges.get(i)[1];
                        VertexDegree edge2_vd1 = edges.get(j)[0];
                        VertexDegree edge2_vd2 = edges.get(j)[1];

                        if (edge1_vd1.vertex < edge1_vd2.vertex)
                            simpleEdges.add(new Tuple2<>(edge1_vd1.vertex, edge1_vd2.vertex));
                        else
                            simpleEdges.add(new Tuple2<>(edge1_vd2.vertex, edge1_vd1.vertex));

                        if (edge2_vd1.vertex < edge2_vd2.vertex)
                            simpleEdges.add(new Tuple2<>(edge2_vd1.vertex, edge2_vd2.vertex));
                        else
                            simpleEdges.add(new Tuple2<>(edge2_vd2.vertex, edge2_vd1.vertex));

                        // Find uncommon vertices of the selected two edges in the previous instructions.
                        VertexDegree vd1;
                        VertexDegree vd2;
                        if (edge1_vd1.vertex != keyVertex)
                            vd1 = edge1_vd1;
                        else
                            vd1 = edge1_vd2;

                        if (edge2_vd1.vertex != keyVertex)
                            vd2 = edge2_vd1;
                        else
                            vd2 = edge2_vd2;

                        Tuple2<Long, Long> syntheticEdge;
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

//        syntheticEdges.persist(StorageLevel.DISK_ONLY());


        // Find triangles
        return syntheticEdges.cogroup(connectedEdges, partition).flatMap(m -> {
            Tuple2<Long, Long> e1;
            Tuple2<Long, Long> e2;
            Tuple2<Long, Long> e3;
            List<List<Tuple2<Long, Long>>> list = new ArrayList<>();

            // If syntheticEdges and connectedEdges of the same key (edge) contain some values at the same time, then a triangle would
            // be created as a 3-length array of the Edge
            Iterator<Tuple2<Long, Long>> oneEdgeIter = m._2._2.iterator();
            if (!oneEdgeIter.hasNext())
                return Collections.emptyIterator();

            e1 = m._1._1 < m._1._2 ? m._1 : new Tuple2<>(m._1._2, m._1._1);
            Iterator<List<Tuple2<Long, Long>>> twoEdgesIter = m._2._1.iterator();
            while (twoEdgesIter.hasNext()) {
                List<Tuple2<Long, Long>> edges = twoEdgesIter.next();
                e2 = edges.get(0)._1 < edges.get(0)._2 ? edges.get(0) : new Tuple2<>(edges.get(0)._2, edges.get(0)._1);
                e3 = edges.get(1)._1 < edges.get(1)._2 ? edges.get(1) : new Tuple2<>(edges.get(1)._2, edges.get(1)._1);

                List<Tuple2<Long, Long>> triangleEdges = Arrays.asList(e1, e2, e3);
                list.add(triangleEdges);
            }

            if (list.size() == 0)
                return Collections.emptyIterator();

            return list.iterator();
        });
    }
}
