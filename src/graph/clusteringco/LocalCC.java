package graph.clusteringco;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 *
 */
public class LocalCC {

    public static void main(String[] args) {
        String inputPath = "input.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        conf.setAppName("LCC-Fonl-" + partition );
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[] {GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

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

        JavaPairRDD<Long, long[]> fonl =
            edges.groupByKey()
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
                        // Iterate over neighbors to calculate degree of the current vertex
                        for (GraphUtils.VertexDegree vd : v._2) {
                            degree++;
                        }

                        List<GraphUtils.VertexDegree> list = new ArrayList<GraphUtils.VertexDegree>();
                        for (GraphUtils.VertexDegree vd : v._2)
                            if (vd.degree > degree || (vd.degree == degree && vd.vertex > v._1))
                                list.add(vd);

                        Collections.sort(list, new Comparator<GraphUtils.VertexDegree> () {
                            @Override
                            public int compare(GraphUtils.VertexDegree vd1, GraphUtils.VertexDegree vd2) {
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
                }).reduceByKey((a, b) -> a)
                .cache();
//        JavaPairRDD<Long, Integer> vertexDegree = fonl.mapValues(t -> (int) t[0]);
//        vertexDegree.cache();

        // Partition based on degree. To balance workload, it is better to have a partitioning mechanism that
        // for example a vertex with high number of neighbors (high deg) would be allocated besides vertex with
        // low number of neighbors (high deg)
        JavaPairRDD<Long, long[]> candidates = fonl
            .filter(t -> t._2.length > 2)
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, long[]>, Long, long[]>() {
            @Override
            public Iterable<Tuple2<Long, long[]>> call(Tuple2<Long, long[]> t) throws Exception {
                int size = t._2.length - 1;
                List<Tuple2<Long, long[]>> output = new ArrayList<>(size);
                for (int index = 1; index < size; index++) {
                    int len = size - index;
                    long[] forward = new long[len + 1];
                    forward[0] = t._1; // First vertex in the triangle
                    System.arraycopy(t._2, index + 1, forward, 1, len);
                    Arrays.sort(forward, 1, forward.length); // sort to comfort with fonl
                    output.add(new Tuple2<>(t._2[index], forward));
                }
                return output;
            }
        });

        JavaPairRDD<Long, Integer> localTriangleCount = candidates.cogroup(fonl, partition)
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<Iterable<long[]>, Iterable<long[]>>>, Long, Integer>() {
                @Override
                public Iterable<Tuple2<Long, Integer>> call(Tuple2<Long, Tuple2<Iterable<long[]>, Iterable<long[]>>> t)
                    throws Exception {
                    Iterator<long[]> iterator = t._2._2.iterator();
                    List<Tuple2<Long, Integer>> output = new ArrayList<>();
                    if (!iterator.hasNext())
                        return output;

                    long[] hDegs = iterator.next();

                    iterator = t._2._1.iterator();
                    if (!iterator.hasNext())
                        return output;

                    Arrays.sort(hDegs, 1, hDegs.length);
                    int sum = 0;

                    do {
                        long[] forward = iterator.next();
                        int count = GraphUtils.sortedIntersectionCount(hDegs, forward, output, 1, 1);
                        if (count > 0) {
                            sum += count;
                            output.add(new Tuple2<>(forward[0], count));
                        }

                    } while (iterator.hasNext());

                    if (sum > 0) {
                        output.add(new Tuple2<>(t._1, sum));
                    }
                    return output;
                }
            })
            .reduceByKey((a, b) -> a + b);

//        Float avg = localTriangleCount.filter(t -> t._2 > 0).join(vertexDegree, partition)
//            .mapValues(t -> t._1 / (float) (t._2 * (t._2 -1)))
//            .map(t -> t._2)
//            .reduce((a, b) -> a + b);

        Float avg = localTriangleCount.filter(t -> t._2 > 0).join(fonl, partition)
            .mapValues(t -> 2 * t._1 / (float) (t._2[0] * (t._2[0] - 1)))
            .map(t -> t._2)
            .reduce((a, b) -> a + b);

        long vertexCount = fonl.count();
        float lcc = avg / vertexCount;
        System.out.println("Nodes = " + vertexCount + ", Sum_Avg_LCC = " + avg + ", Avg_LCC = " + lcc);
    }
}
