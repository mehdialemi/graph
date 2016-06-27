package graph.clusteringco;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class GraphUtils {

    public static JavaPairRDD<Long, Long> loadUndirectedEdges(JavaRDD<String> input) {
        JavaPairRDD<Long, Long> edges = input.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {

            @Override
            public Iterable<Tuple2<Long, Long>> call(String line) throws Exception {
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                if (line.startsWith("#"))
                    return list;
                String[] s = line.split("\\s+");
                long e1 = Long.parseLong(s[0]);
                long e2 = Long.parseLong(s[1]);
                list.add(new Tuple2<>(e1, e2));
                list.add(new Tuple2<>(e2, e1));
                return list;
            }
        });
        return edges;
    }

    // Key: vertex id, Value: degree, neighbors sorted by degree
    public static JavaPairRDD<Long, long[]> createFonl(JavaPairRDD<Long, Long> edges, int partition) {
        JavaPairRDD<Long, long[]> fonl =
            edges.groupByKey()
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, Long, VertexDegree>() {
                    @Override
                    public Iterable<Tuple2<Long, VertexDegree>> call(Tuple2<Long, Iterable<Long>> t) throws Exception {
                        HashSet<Long> neighborSet = new HashSet<>();
                        for (Long neighbor : t._2) {
                            neighborSet.add(neighbor);
                        }

                        int degree = neighborSet.size();

                        VertexDegree vd = new VertexDegree(t._1, degree);

                        List<Tuple2<Long, VertexDegree>> degreeList = new ArrayList<>(degree);

                        // Add degree information of the current vertex to its neighbor
                        for (Long neighbor : neighborSet) {
                            degreeList.add(new Tuple2<>(neighbor, vd));
                        }
                        return degreeList;
                    }
                }).groupByKey()
                .repartition(partition)
                .mapToPair(new PairFunction<Tuple2<Long, Iterable<VertexDegree>>, Long, long[]>() {
                    @Override
                    public Tuple2<Long, long[]> call(Tuple2<Long, Iterable<VertexDegree>> v) throws Exception {
                        int degree = 0;
                        // Iterate over neighbors to calculate degree of the current vertex
                        for (VertexDegree vd : v._2) {
                            degree++;
                        }

                        List<VertexDegree> list = new ArrayList<VertexDegree>();
                        for (VertexDegree vd : v._2)
                            if (vd.degree > degree || (vd.degree == degree && vd.vertex > v._1))
                                list.add(vd);

                        Collections.sort(list, new Comparator<VertexDegree> () {
                            @Override
                            public int compare(VertexDegree vd1, VertexDegree vd2) {
                                return (vd1.degree != vd2.degree) ? vd1.degree - vd2.degree :
                                    (int) (vd1.vertex - vd2.vertex);
                            }
                        });

                        long[] hDegs = new long[list.size() + 1];
                        hDegs[0] = degree;
                        for (int i = 1 ; i < hDegs.length ; i ++)
                            hDegs[i] = list.get(i - 1).vertex;

                        return new Tuple2<>(v._1, hDegs);
                    }
                });
        return fonl;
    }

    static int sortedIntersectionCount(long[] hDegs, long[] forward) {
        return sortedIntersectionCount(hDegs, forward, null, 0, 0);
    }

    static int sortedIntersectionCount(long[] hDegs, long[] forward, List<Tuple2<Long, Integer>> output, int hIndex,
                                  int fIndex) {
        int fLen = forward.length;
        int hLen = hDegs.length;

        if (hDegs.length == 0 || fLen == 0)
            return 0;

        boolean leftRead = true;
        boolean rightRead = true;

        long h = 0;
        long f = 0;
        int count = 0;

        boolean finish = false;
        while (!finish) {

            if (hIndex >= hLen && fIndex >= fLen)
                break;

            if ((hIndex >= hLen && !rightRead) || (fIndex >= fLen && !leftRead))
                break;

            if (leftRead && hIndex < hLen) {
                h = hDegs[hIndex++];
            }

            if (rightRead && fIndex < fLen) {
                f = forward[fIndex++];
            }

            if (h == f) {
                if (output != null)
                    output.add(new Tuple2<> (h, 1));
                count++;
                leftRead = true;
                rightRead = true;
            }
            else if (h < f) {
                leftRead = true;
                rightRead = false;
            }
            else {
                leftRead = false;
                rightRead = true;
            }
        }
        return count;
    }

    public static class VertexDegree {
        long vertex;
        int degree;
        public VertexDegree(long vertex, int degree) {
            this.vertex = vertex;
            this.degree = degree;
        }
    }


}
