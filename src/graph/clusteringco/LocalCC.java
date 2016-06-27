package graph.clusteringco;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
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

        int batchSize = 10;
        if (args.length > 2)
            batchSize = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]");
        conf.setAppName("CC-Fonl");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(new Class[] {GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});

        JavaSparkContext sc = new JavaSparkContext(conf);
        Broadcast<Integer> bBatchSize = sc.broadcast(batchSize);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Long, Long> edges = GraphUtils.loadUndirectedEdges(input);

        JavaPairRDD<Long, long[]> fonl = GraphUtils.createFonl(edges, partition);
        fonl.cache();

        JavaPairRDD<Long, Integer> vertexDegree = fonl.mapValues(t -> (int) t[0]);
        vertexDegree.cache();

        System.out.println("FONL:");
        fonl.collect().forEach(t -> System.out.println("Key: " + t._1 + ", Value: " + lts(t._2)));

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

        System.out.println("CANDIDATES:");
        candidates.collect().forEach(t -> {
            System.out.println("Key: " + t._1 + ", Forward: " + lts(t._2));
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

        System.out.println("Local Triangle:");
        localTriangleCount.collect().forEach(t -> {
            System.out.println("Key: " + t._1 + ", Triangles: " + t._2);
        });

        System.out.println("FONL:");
        fonl.collect().forEach(t -> System.out.println("Key: " + t._1 + ", Value: " + lts(t._2)));

        System.out.println("VERTEX_DEGREE:");
        vertexDegree.collect().forEach(t -> System.out.println("Key: " + t._1 + ", Value: " + t._2));

        Float avg = localTriangleCount.filter(t -> t._2 > 0).join(vertexDegree, partition)
            .mapValues(t -> t._1 / (float) (t._2 * (t._2 -1)))
            .map(t -> t._2)
            .reduce((a, b) -> a + b);
        long vertexCount = fonl.count();
        float lcc = avg / vertexCount;
        System.out.println("Nodes = " + vertexCount + ", Sum_Avg_LCC = " + avg + ", Avg_LCC = " + lcc);
        Integer totalTriangles = localTriangleCount.map(t -> t._2).reduce((a, b) -> a + b);
        System.out.println("Total triangles = " + totalTriangles / 3);
    }

    static String lts(long[] array) {
        String str = "";
        for(long l : array)
            str += " " + l;
        return str;
    }
}
