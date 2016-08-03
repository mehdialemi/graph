package graph.clusteringco;

import graph.GraphUtils;
import graph.OutputUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * Calculate Global Clustering Coefficient (GCC) using fonl structure which is sorted based on degree of nodes. This
 * causes that in all steps of program we could have a balanced workload. In finding GCC we only require total
 * triangle count and we don't need to maintain triangle count per node. So, we could have better performance in
 * comparison with Local Clustering Coefficient (LCC) which we should have number of triangles per node.
 */
public class FonlDegGCC {

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
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Long, Long> edges = GraphUtils.loadUndirectedEdges(input);

        JavaPairRDD<Long, long[]> fonl = edges.groupByKey()
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

//        JavaPairRDD<Long, long[]> fonl = FonlUtils.createFonlDegreeBased(edges, partition);

        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of higherIds (high deg) would be allocated besides vertex with
        // low number of higherIds (high deg)
        JavaPairRDD<Long, long[]> candidates = fonl
            .filter(t -> t._2.length > 2)
            .flatMapToPair((PairFlatMapFunction<Tuple2<Long, long[]>, Long, long[]>) t -> {
                int size = t._2.length - 1;
                List<Tuple2<Long, long[]>> output = new ArrayList<>(size - 1);
                for (int index = 1; index < size; index++) {
                    int len = size - index;
                    long[] forward = new long[len];
                    System.arraycopy(t._2, index + 1, forward, 0, len);
                    output.add(new Tuple2<>(t._2[index], forward));
                }
                return output;
            }).mapValues(t -> { // sort candidates
                    Arrays.sort(t);
                    return t;
                });

        long totalTriangles = candidates.cogroup(fonl, partition).map(t -> {
            Iterator<long[]> iterator = t._2._2.iterator();
            if (!iterator.hasNext()) {
                    return 0L;
                }
            long[] hDegs = iterator.next();

            iterator = t._2._1.iterator();
            if (!iterator.hasNext()) {
                    return 0L;
                }

            Arrays.sort(hDegs, 1, hDegs.length);
            long sum = 0;

            do {
                    long[] forward = iterator.next();
                    int count = GraphUtils.sortedIntersectionCount(hDegs, forward, null, 1, 0);
                    sum += count;
                } while (iterator.hasNext());

            return sum;
        }).reduce((a, b) -> a + b);

        long totalNodes = fonl.count();
        float globalCC = totalTriangles / (float) (totalNodes * (totalNodes - 1));
        OutputUtils.printOutputGCC(totalNodes, totalTriangles, globalCC);
        sc.close();
    }
}
