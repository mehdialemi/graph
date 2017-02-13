package ir.ac.sbu.graph.ktruss.distributed;

import ir.ac.sbu.graph.clusteringco.FonlDegTC;
import ir.ac.sbu.graph.clusteringco.FonlUtils;
import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class KTrussSparkPartitionBySup {

    public static void main(String[] args) {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 10;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        int k = 4; // k-truss
        if (args.length > 2)
            k = Integer.parseInt(args[2]);
        final int minSup = k - 2;

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "KTrussSparkPartitionBySup-" + k + "-MultiSteps", partition, inputPath);
        conf.registerKryoClasses(new Class[]{int[].class, List.class, IntSet.class, IntOpenHashSet.class});
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        Partitioner partitionerSmall = new HashPartitioner(partition);
        Partitioner partitionerBig = new HashPartitioner(partition * 3);

        JavaPairRDD<Integer, Integer> edges = GraphLoader.loadEdgesInt(input);

        JavaPairRDD<Integer, int[]> fonl = FonlUtils.createWith2ReduceDegreeSortInt(edges, partitionerSmall);

        JavaPairRDD<Integer, int[]> candidates = FonlDegTC.generateCandidatesInteger(fonl).partitionBy(partitionerBig);

        // Generate kv such that key is an edge and value is its triangle vertices.
        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> edgeVertices = candidates.cogroup(fonl).flatMapToPair(t -> {
            int[] fVal = t._2._2.iterator().next();
            Arrays.sort(fVal, 1, fVal.length);
            int v = t._1;
            List<Tuple2<Tuple2<Integer, Integer>, Integer>> output = new ArrayList<>();
            for (int[] cVal : t._2._1) {
                int u = cVal[0];
                Tuple2<Integer, Integer> uv;
                if (u < v)
                    uv = new Tuple2<>(u, v);
                else
                    uv = new Tuple2<>(v, u);

                // The intersection determines triangles which u and v are two of their vertices.
                // Always generate and edge (u, v) such that u < v.
                int fi = 1;
                int ci = 1;
                while (fi < fVal.length && ci < cVal.length) {
                    if (fVal[fi] < cVal[ci])
                        fi ++;
                    else if (fVal[fi] > cVal[ci])
                        ci ++;
                    else {
                        int w = fVal[fi];
                        output.add(new Tuple2<>(uv, w));
                        if (u < w)
                            output.add(new Tuple2<>(new Tuple2<>(u, w), v));
                        else
                            output.add(new Tuple2<>(new Tuple2<>(w, u), v));

                        if (v < w)
                            output.add(new Tuple2<>(new Tuple2<>(v, w), u));
                        else
                            output.add(new Tuple2<>(new Tuple2<>(w, v), u));
                        fi ++;
                        ci ++;
                    }
                }
            }

            return output.iterator();
        }).groupByKey()
            .mapValues(values -> {
                // TODO use iterator here instead of creating a set and filling it
                int count = 0;
                for (int v : values) {
                    count ++;
                }

                IntSet set = new IntOpenHashSet(count);
                for (int v : values) {
                    set.add(v);
                }
                return set;
            });

        JavaPairRDD<Integer, Tuple2<Tuple2<Integer, Integer>, IntSet>> supEdgeVertices =
            edgeVertices.mapToPair(f -> new Tuple2<>(f._2.size(), new Tuple2<>(f._1, f._2)));
        List<Tuple2<Integer, Integer>> supCount = supEdgeVertices.groupByKey().mapValues(t -> {
            int count = 0;
            for (Tuple2<Tuple2<Integer, Integer>, IntSet> x : t) {
                count++;
            }
            return count;
        }).collect();

        Map<Integer, Tuple2<Integer, Integer>> map = new HashMap<>(supCount.size());
        int start = 0;
        int partitionsPerSup = 1000;
        for (Tuple2<Integer, Integer> sup : supCount) {
            int end = start + partitionsPerSup;
            map.put(sup._1, new Tuple2<>(start, end));
            start = end;
        }

        final Broadcast<Integer> totalPartitions = sc.broadcast(start);
        final Broadcast<Map<Integer, Tuple2<Integer, Integer>>> mapBroadcast = sc.broadcast(map);

        JavaPairRDD<Tuple2<Integer, Integer>, IntSet> partitionedEdgeVertices = supEdgeVertices.partitionBy(new Partitioner() {

            @Override
            public int numPartitions() {
                return totalPartitions.getValue();
            }

            @Override
            public int getPartition(Object key) {
                int k = (int) key;
                Tuple2<Integer, Integer> range = mapBroadcast.getValue().get(k);
                return ThreadLocalRandom.current().nextInt(range._1, range._2);
            }
        }).mapToPair(kv -> kv._2).cache();


        sc.close();
    }

    static void log(String text) {
        System.out.println("KTRUSS [" + new Date() + "] " + text);
    }

    static void logDuration(String text, long millis) {
        log(text + " (" + millis / 1000 + " sec)");
    }
}
