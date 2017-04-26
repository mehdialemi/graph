package ir.ac.sbu.graph.kcore;

import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class KCore {
    public static final int[] EMPTY_ARRAY = new int[]{};

    public KCore() {
    }

    public static JavaPairRDD<Integer, int[]> runNeighborList(JavaPairRDD<Integer, int[]> neighborList, int k, Partitioner partitioner) {
        long t1, t2;
        while (true) {
            t1 = System.currentTimeMillis();
            JavaPairRDD<Integer, Integer> update = neighborList.filter(nl -> nl._2.length < k && nl._2.length > 0)
                    .flatMapToPair(nl -> {
                        List<Tuple2<Integer, Integer>> out = new ArrayList<>(nl._2.length);
                        for (int v : nl._2) {
                            out.add(new Tuple2<>(v, nl._1));
                        }
                        return out.iterator();
                    }).partitionBy(partitioner).cache();

            long count = update.count();
            t2 = System.currentTimeMillis();
            log("K-core, current invalid count: " + count, t1, t2);
            if (count == 0)
                break;

            neighborList = neighborList.cogroup(update).mapValues(value -> {
                int[] allNeighbors = value._1().iterator().next();
                if (allNeighbors.length < k) {
                    return EMPTY_ARRAY;
                }
                IntSet set = new IntOpenHashSet(allNeighbors);
                for (Integer v : value._2) {
                    set.remove(v.intValue());
                }
                return set.toIntArray();
            }).cache();
        }
        return neighborList.filter(t -> t._2.length != 0);
    }

    public static JavaPairRDD<Integer, Integer> runDegInfo(JavaPairRDD<Integer, int[]> neighborList, int k, Partitioner partitioner) {
        JavaPairRDD<Integer, Integer> degInfo = neighborList.mapValues(v -> v.length).cache();
        long t1, t2;
        while (true) {
            t1 = System.currentTimeMillis();
            // Get invalid vertices and partition by the partitioner used to partition neighbors
            JavaPairRDD<Integer, Integer> invalids = degInfo.filter(v -> v._2 < k).partitionBy(partitioner);

            long count = invalids.count();
            t2 = System.currentTimeMillis();
            log("K-core, current invalid count: " + count, t1, t2);
            if (count == 0)
                break;

            // Find the amount of value to be subtracted from each vertex degrees.
            JavaPairRDD<Integer, Integer> vSubtract = neighborList.rightOuterJoin(invalids)
                    .filter(t -> t._2._1.isPresent())  // filter only invalid vertices to get their neighbors
                    .flatMapToPair(t -> {
                        int[] allNeighbors = t._2._1.get();
                        List<Tuple2<Integer, Integer>> out = new ArrayList<>(allNeighbors.length);
                        for (int allNeighbor : allNeighbors) {
                            out.add(new Tuple2<>(allNeighbor, 1));
                        }
                        return out.iterator();
                    })
                    .reduceByKey((a, b) -> a + b)  // aggregate all subtract info per vertex
                    .partitionBy(partitioner);

            // Update vInfo to remove invalid vertices and subtract the degree of other remaining vertices using the vSubtract map
            degInfo = degInfo
                    .leftOuterJoin(vSubtract)
                    .filter(t -> t._2._1 >= k)
                    .mapValues(t -> t._2.isPresent() ? t._1 - t._2.get() : t._1).filter(v -> v._2 > 0)
                    .cache();


        }
        return degInfo;
//            // Update vInfo to remove invalid vertices and subtract the degree of other remaining vertices using the vSubtract map
//            degInfo = degInfo
//                    .leftOuterJoin(vSubtract)
//                    .filter(t -> t._2._1 >= k)
//                    .mapValues(t -> t._2.isPresent() ? t._1 - t._2.get() : t._1).filter(v -> v._2 > 0)
//                    .cache();
    }

    protected static void log(String msg, long start, long end) {
        log(msg, (end - start));
    }

    protected static void log(String msg) {
        log(msg, -1);
    }

    protected static void log(String msg, long duration) {
        if (duration == -1)
            System.out.println("KCore " + msg);
        else
            System.out.println("KCore " + msg + ", duration: " + duration + " ms");
    }

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 10;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");

        GraphUtils.setAppName(conf, "KCore-" + k, partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.VertexDegree.class, int[].class, Iterable.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);
        JavaPairRDD<Integer, int[]> neighbors = neighborList(new HashPartitioner(partition), edges);
        
        Partitioner partitioner = new HashPartitioner(partition);
//        neighbors = runNeighborList(neighbors, k, partitioner);
        JavaPairRDD<Integer, Integer> degInfo = runDegInfo(neighbors, k, partitioner);
        log("Vertex count: " + degInfo.count());

        sc.close();

    }

    public static JavaPairRDD<Integer, int[]> neighborList(Partitioner partitioner, JavaPairRDD<Integer, Integer> edges) {
        return edges.groupByKey(partitioner).mapToPair(t -> {
            IntSet set = new IntOpenHashSet();
            for (Integer v : t._2) {
                set.add(v.intValue());
            }
            return new Tuple2<>(t._1, set.toIntArray());
        }).cache();
    }

}
