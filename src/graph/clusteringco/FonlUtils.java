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

    /**
     * This utility function create fonl such that more load balancing is achieved and tries to prevent out of memory
     * error by reducing the size of required memory when calling reduce by key function for long tail vertex neighbors.
     * It does its work by first finding degree of each vertex. Then it create an edge list which in it each edge
     * has vertices such that lower degree vertex is the key and higher degree vertex is value. The final data
     * structure is fonl which in it key is a vertex and value is its neighbors sorted by their degree.
     * @return
     */
    public static JavaPairRDD<Long, long[]> createWith2Join(JavaPairRDD<Long, Long> edges, int partition) {
        // edges direction from low node id to high node id to prevent edge repetition.
        JavaRDD<Tuple2<Long, Long>> lthEdge =
            edges.map(t -> t._1 < t._2 ? new Tuple2<>(t._1, t._2) : new Tuple2<>(t._2, t._1)).distinct().cache();

        // Get degree of each vertex.
        JavaPairRDD<Long, Long> vertexDegree = lthEdge
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Long>, Long, Long>() {
                @Override
                public Iterable<Tuple2<Long, Long>> call(Tuple2<Long, Long> t) throws Exception {
                    List<Tuple2<Long, Long>> list = new ArrayList<>(2);
                    list.add(new Tuple2<>(t._1, 1L));
                    list.add(new Tuple2<>(t._2, 1L));
                    return list;
                }
            }).reduceByKey((a, b) -> a + b).cache();

        // Each vertex in the each edge has its degree beside itself. Each edge direction is from lower node degree
        // to higher node degree.
        JavaPairRDD<Tuple2<Long, Long>, Tuple2<Long, Long>> degreeSortedEdges =
            lthEdge.keyBy(t -> t._1).join(vertexDegree) // send vertex degree to left vertex in the edge.
            .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<Long, Long>, Long>>, Long, Tuple2<Long, Long>>() {

                @Override
                public Tuple2<Long, Tuple2<Long, Long>> call(Tuple2<Long, Tuple2<Tuple2<Long, Long>, Long>> t)
                    throws Exception {
                    return new Tuple2<>(t._2._1._2, new Tuple2<>(t._1, t._2._2));
                }
            }).join(vertexDegree) // send vertex degree to the right vertex in the edge.
                .mapToPair(new PairFunction<Tuple2<Long, Tuple2<Tuple2<Long, Long>, Long>>, Tuple2<Long, Long>,
                    Tuple2<Long, Long>>() {
                @Override
                public Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> call(Tuple2<Long, Tuple2<Tuple2<Long, Long>, Long>> t) throws Exception {
                    long v1 = t._1;
                    long d1 = t._2._2;
                    long v2 = t._2._1._1;
                    long d2 = t._2._1._2;
                    Tuple2<Long, Long> t1 = new Tuple2<>(v1, d1);
                    Tuple2<Long, Long> t2 = t._2._1;
                    if (d1 < d2)
                        return new Tuple2<>(t1, t2);
                    if (d1 > d2)
                        return new Tuple2<>(t2, t1);
                    if (v1 < v2)
                        return new Tuple2<>(t1, t2);
                    return new Tuple2<>(t2, t1);
                }
            });
        lthEdge.unpersist();
        vertexDegree.unpersist();

        // Since vertices of each edge are from lower degree to higher degree then its neighbors are already sorted.
        return degreeSortedEdges.groupByKey().mapToPair(new PairFunction<Tuple2<Tuple2<Long,Long>,Iterable<Tuple2<Long,
            Long>>>, Long, long[]>() {
            @Override
            public Tuple2<Long, long[]> call(Tuple2<Tuple2<Long, Long>, Iterable<Tuple2<Long, Long>>> t) throws
                Exception {
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                for(Tuple2<Long, Long> vd : t._2)
                    list.add(vd);

                // Sort neighbors based on their degree.
                Collections.sort(list, (a, b) -> {
                    int x, y;
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
                higherDegs[0] = list.size();
                for (int i = 1; i < higherDegs.length; i++)
                    higherDegs[i] = list.get(i - 1)._1;

                return new Tuple2<>(t._1._1, higherDegs);
            }
        }).repartition(partition).persist(StorageLevel.MEMORY_ONLY_2());
    }

    /**
     * Create a fonl in key-value structure. Here, key is a vertex id and value is an array which first element of it
     * stores degree of the key vertex and the other elements are neighbor vertices with higher degree than the key
     * vertex.
     *
     * @param edges     undirected edges of graph.
     * @param partition number of partition to the final fonl.
     * @return Key: vertex id, Value: degree, Neighbor vertices sorted by their degree.
     */
    public static JavaPairRDD<Long, long[]> createWith2Reduce(JavaPairRDD<Long, Long> edges, int partition) {
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


                    Collections.sort(list, (a, b) -> {
                        int x, y;
                        if (a.degree != b.degree) {
                            x = a.degree;
                            y = b.degree;
                        } else {
                            x = (int) (a.vertex >> 32);
                            y = (int) (b.vertex >> 32);
                            if (x == y) {
                                x = (int) ((long) a.vertex);
                                y = (int) ((long) b.vertex);
                            }
                        }
                        return x - y;
                    });

                    long[] higherDegs = new long[list.size() + 1];
                    higherDegs[0] = degree;
                    for (int i = 1; i < higherDegs.length; i++)
                        higherDegs[i] = list.get(i - 1).vertex;

                    return new Tuple2<>(v._1, higherDegs);
                }
            }).repartition(partition).persist(StorageLevel.DISK_ONLY_2());
    }

    /**
     * Create a fonl in key-value structure. Here, key is a vertex id and value is an array which first element of it
     * stores degree of the key vertex and the other elements are neighbor vertices with higher degree than the key
     * vertex.
     *
     * @param edges     undirected edges of graph.
     * @param partition number of partition to the final fonl.
     * @return Key: vertex id, Value: degree, Neighbor vertices sorted by their degree.
     */
    public static JavaPairRDD<Long, long[]> createWith2ReduceNoSort(JavaPairRDD<Long, Long> edges, int partition) {
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

                    SortedSet<GraphUtils.VertexDegree> list = new TreeSet<>((a, b) -> {
                        int x, y;
                        if (a.degree != b.degree) {
                            x = a.degree;
                            y = b.degree;
                        } else {
                            x = (int) (a.vertex >> 32);
                            y = (int) (b.vertex >> 32);
                            if (x == y) {
                                x = (int) ((long) a.vertex);
                                y = (int) ((long) b.vertex);
                            }
                        }
                        return x - y;
                    });

                    for (GraphUtils.VertexDegree vd : v._2)
                        if (vd.degree > degree || (vd.degree == degree && vd.vertex > v._1))
                            list.add(vd);

                    long[] higherDegs = new long[list.size() + 1];
                    higherDegs[0] = degree;
                    int i = 0;
                    for(GraphUtils.VertexDegree vertex : list) {
                        higherDegs[++i] = vertex.vertex;
                    }

                    return new Tuple2<>(v._1, higherDegs);
                }
            }).persist(StorageLevel.DISK_ONLY_2());
    }

    /**
     * Create a fonl in key-value structure. Here, key is a vertex id and value is an array which first element of it
     * stores degree of the key vertex and the other elements are neighbor vertices with higher ids than the key vertex.
     *
     * @param edges     undirected edges of graph.
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
                        degree++;
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
            }).repartition(partition).cache();
    }

    public static JavaPairRDD<Long, long[]> loadFonl(JavaSparkContext sc, String inputPath, int partition) {
        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Long, Long> edges = GraphUtils.loadUndirectedEdges(input);
        return FonlUtils.createWith2Reduce(edges, partition);
    }
}
