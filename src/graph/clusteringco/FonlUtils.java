package graph.clusteringco;

import graph.GraphUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Utility class to create fonl sorted based on degree or ids
 */
public class FonlUtils implements Serializable {

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
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long, GraphUtils.VertexDegree>() {
                    @Override
                    public Iterable<Tuple2<Long, GraphUtils.VertexDegree>> call(Tuple2<Long, Iterable<Long>> t) throws Exception {
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
                    }
                }).groupByKey()
                .mapToPair(new PairFunction<Tuple2<Long, Iterable<GraphUtils.VertexDegree>>, Long, long[]>() {
                    @Override
                    public Tuple2<Long, long[]> call(Tuple2<Long, Iterable<GraphUtils.VertexDegree>> v) throws Exception {
                        int degree = 0;
                        // Iterate over higherIds to calculate degree of the current vertex
                        for (GraphUtils.VertexDegree vd : v._2) {
                            degree++;
                        }

                        List<GraphUtils.VertexDegree> list = new ArrayList<GraphUtils.VertexDegree>();
                        for (GraphUtils.VertexDegree vd : v._2)
                            if (vd.degree > degree || (vd.degree == degree && vd.vertex > v._1))
                                list.add(vd);

                        Collections.sort(list, (vd1, vd2) ->
                            (vd1.degree != vd2.degree) ? vd1.degree - vd2.degree :(int) (vd1.vertex - vd2.vertex));

                        long[] higherDegs = new long[list.size() + 1];
                        higherDegs[0] = degree;
                        for (int i = 1 ; i < higherDegs.length ; i ++)
                            higherDegs[i] = list.get(i - 1).vertex;

                        return new Tuple2<>(v._1, higherDegs);
                    }
                }).reduceByKey((a, b) -> a)
                .cache();
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
