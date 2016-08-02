package graph.clusteringco;

import graph.GraphUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Utility class to create fonl sorted based on degree or ids
 */
public class FonlUtils implements Serializable {

    static class EVD { // Edge Vertex Degree
        long x1;
        long x2;
        boolean isEdge;

        EVD(long x1, long x2, boolean isEdge) {
            this.x1 = x1;
            this.x2 = x2;
            this.isEdge = isEdge;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            EVD evd = (EVD) obj;
            if (isEdge && evd.isEdge)
                return x1 == evd.x1 && x2 == evd.x2;
            if (!isEdge && evd.isEdge)
                return x1 == evd.x1 || x1 == evd.x2;
            if (isEdge && !evd.isEdge)
                return evd.x1 == x1 || evd.x1 == x2;
            else
                return x1 == evd.x1;
        }

        @Override
        public int hashCode() {
            return (int) (x1);
        }
    }

    public static JavaPairRDD<Long, long[]> createFonlDegreeBased2(JavaPairRDD<Long, Long> edges, int partition) {
        // edges direction from low node id to high node id to prevent edge repetition.
        JavaRDD<Tuple2<Long, Long>> sthEdge =
            edges.map(t -> t._1 < t._2 ? new Tuple2<>(t._1, t._2) : new Tuple2<>(t._2, t._1)).distinct();
        sthEdge.cache();

        // Get degree of each vertex.
        JavaPairRDD<EVD, EVD> vertexDegree = sthEdge
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Long>, Long, Long>() {
                @Override
                public Iterable<Tuple2<Long, Long>> call(Tuple2<Long, Long> t) throws Exception {
                    List<Tuple2<Long, Long>> list = new ArrayList<>(2);
                    list.add(new Tuple2<>(t._1, 1L));
                    list.add(new Tuple2<>(t._2, 1L));
                    return list;
                }
            }).reduceByKey((a, b) -> a + b).mapToPair(new PairFunction<Tuple2<Long, Long>, EVD, EVD>() {
                @Override
                public Tuple2<EVD, EVD> call(Tuple2<Long, Long> t) throws Exception {
                    EVD evd = new EVD(t._1, t._2, false);
                    return new Tuple2<>(evd, evd);
                }
            });

        // This is a neighbor list which key node is a node id and value contains its neighbors with higher degree.
        JavaPairRDD<Long, Iterable<Tuple2<Long, Long>>> neighborWithDegree =
            sthEdge.mapToPair(new PairFunction<Tuple2<Long, Long>, EVD, Byte>() {
            @Override
            public Tuple2<EVD, Byte> call(Tuple2<Long, Long> t) throws Exception {
                EVD evd = new EVD(t._1, t._2, true);
                return new Tuple2<>(evd, (byte) 0);
            }
        }).cogroup(vertexDegree) // Find right direction from lower degree to higher degree node.
            .mapToPair(new PairFunction<Tuple2<EVD, Tuple2<Iterable<Byte>, Iterable<EVD>>>, Long, Tuple2<Long, Long>>() {
                @Override
                public Tuple2<Long, Tuple2<Long, Long>> call(Tuple2<EVD, Tuple2<Iterable<Byte>, Iterable<EVD>>> t)
                    throws Exception {
                    EVD vertexDeg1 = t._2._2.iterator().next();
                    EVD vertexDeg2 = t._2._2.iterator().next();

                    if (vertexDeg1 != null && vertexDeg2 != null) {
                        if (vertexDeg1.x2 < vertexDeg2.x2)
                            return new Tuple2<>(vertexDeg1.x1, new Tuple2<>(vertexDeg2.x1, vertexDeg2.x2));
                        else if (vertexDeg2.x2 < vertexDeg1.x2)
                            return new Tuple2<>(vertexDeg2.x1, new Tuple2<>(vertexDeg1.x1, vertexDeg1.x2));
                        else if (vertexDeg1.x1 < vertexDeg2.x1)
                            return new Tuple2<>(vertexDeg1.x1, new Tuple2<>(vertexDeg2.x1, vertexDeg2.x2));
                        else
                            return new Tuple2<>(vertexDeg2.x1, new Tuple2<>(vertexDeg1.x1, vertexDeg1.x2));
                    }
                    return null;
                }
            }).groupByKey();
        sthEdge.unpersist();
        // Sort neighbor list based on the degree.
        return neighborWithDegree.mapToPair(new PairFunction<Tuple2<Long,Iterable<Tuple2<Long,Long>>>, Long, long[]>() {
            @Override
            public Tuple2<Long, long[]> call(Tuple2<Long, Iterable<Tuple2<Long, Long>>> t) throws Exception {
                int degree = 0;
                // Iterate over higherIds to calculate degree of the current vertex
                for (Tuple2<Long, Long> vd : t._2) {
                    degree++;
                }

                List<Tuple2<Long, Long>> list = new ArrayList<>();
                for (Tuple2<Long, Long> vd : t._2)
                    if (vd._2 > degree || (vd._2 == degree && vd._1 > t._1))
                        list.add(vd);

                Collections.sort(list, (a, b) -> {
                    int x , y;
                    if (a._2 != b._2) {
                        x = (int) (a._2 >> 32);
                        y = (int) (b._2 >> 32);
                        if (x == y) {
                            x = (int) ((long) a._2);
                            y = (int) ((long) b._2);
                        }
                    } else {
                        x = (int) (a._1 >> 32);
                        y = (int) (b._1 >> 32);
                        if (x == y) {
                            x = (int) ((long) a._1);
                            y = (int) ((long) b._1);
                        }
                    }
                    return x - y;
                });

                long[] higherDegs = new long[list.size() + 1];
                higherDegs[0] = degree;
                for (int i = 1 ; i < higherDegs.length ; i ++)
                    higherDegs[i] = list.get(i - 1)._1;

                return new Tuple2<>(t._1, higherDegs);
            }
        });
    }
    /**
     * Create a fonl in key-value structure. Here, key is a vertex id and value is an array which first element of it
     * stores degree of the key vertex and the other elements are neighbor vertices with higher degree than the key
     * vertex.
     * @param edges undirected edges of graph.
     * @param partition number of partition to the final fonl.
     * @return Key: vertex id, Value: degree, Neighbor vertices sorted by their degree.
     */
    public static JavaPairRDD<Long, long[]> createFonlDegreeBased(JavaPairRDD<Long, Long> edges, int partition) {
        return edges.groupByKey()
                .flatMapToPair((PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long, GraphUtils.VertexDegree>) t -> {
                    HashSet<Long> neighborSet = new HashSet<>();
                    for (Long neighbor : t._2) {
                        neighborSet.add(neighbor);
                    }

                    int degree = neighborSet.size();

                    GraphUtils.VertexDegree vd = new GraphUtils.VertexDegree(t._1, degree);

                    List<Tuple2<Long, GraphUtils.VertexDegree>> degreeList = new ArrayList<>(degree);

                    // Add degree information of the current vertex to its neighbor
                    for (Long neighbor : neighborSet) {
                        degreeList.add(new Tuple2<>(neighbor, vd));
                    }
                    return degreeList;
                }).groupByKey()
                .mapToPair(new PairFunction<Tuple2<Long, Iterable<GraphUtils.VertexDegree>>, Long, long[]>() {
                    @Override
                    public Tuple2<Long, long[]> call(Tuple2<Long, Iterable<GraphUtils.VertexDegree>> v) throws Exception {
                        int degree = 0;
                        // Iterate over higherIds to calculate degree of the current vertex
                        for (GraphUtils.VertexDegree vd : v._2) {
                            degree++;
                        }

                        List<GraphUtils.VertexDegree> list = new ArrayList<>();
                        for (GraphUtils.VertexDegree vd : v._2)
                            if (vd.degree > degree || (vd.degree == degree && vd.vertex > v._1))
                                list.add(vd);


                        Collections.sort(list, (vd1, vd2) ->
                            (vd1.degree != vd2.degree) ? vd1.degree - vd2.degree :
                                ((int) vd1.vertex >> 32) - ((int) vd2.vertex >> 32));

                        long[] higherDegs = new long[list.size() + 1];
                        higherDegs[0] = degree;
                        for (int i = 1 ; i < higherDegs.length ; i ++)
                            higherDegs[i] = list.get(i - 1).vertex;

                        return new Tuple2<>(v._1, higherDegs);
                    }
                }).reduceByKey((a, b) -> a, partition)
                .persist(StorageLevel.MEMORY_ONLY_SER());
    }

    /**
     * Create a fonl in key-value structure. Here, key is a vertex id and value is an array which first element of it
     * stores degree of the key vertex and the other elements are neighbor vertices with higher ids than the key vertex.
     * @param edges undirected edges of graph.
     * @param partition number of partition to the final fonl.
     * @return Key: vertex id, Value: degree, Neighbor vertices sorted by their ids.
     */
    public static JavaPairRDD<Integer, int[]> createFonlIdBasedInt(JavaPairRDD<Integer, Integer> edges, int partition) {
        return edges.groupByKey()
            .mapToPair(new PairFunction<Tuple2<Integer, Iterable<Integer>>, Integer, int[]>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<Integer, int[]> call(Tuple2<Integer, Iterable<Integer>> tuple) throws Exception {
                    Iterable<Integer> iterable = tuple._2;
                    int vertex = tuple._1;
                    int degree = 0;
                    HashSet<Integer> hash = new HashSet<>();
                    for (int neighbor : iterable) {
                        if (neighbor > vertex)
                            hash.add(neighbor);
                        degree ++;
                    }

                    int[] higherIds = new int[hash.size() + 1];
                    higherIds[0] = degree;
                    int index = 1;
                    for (int neighbor : hash)
                        higherIds[index++] = neighbor;

                    // Sort based on vertex ids
                    Arrays.sort(higherIds, 1, higherIds.length);

                    return new Tuple2<>(vertex, higherIds);
                }
            }).reduceByKey((a, b) -> a).repartition(partition).cache();
    }

    public static JavaPairRDD<Long, long[]> loadFonl(JavaSparkContext sc, String inputPath, int partition) {
        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Long, Long> edges = GraphUtils.loadUndirectedEdges(input);
        return FonlUtils.createFonlDegreeBased(edges, partition);
    }
}
