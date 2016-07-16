package graph.clusteringco;

import graph.GraphUtils;
import graph.OutputUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * This class implements the node iterator++ triangle counter in the spark.
 */
public class NodeIteratorPlusTC_Spark {

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
        GraphUtils.setAppName(conf, "NodeIterPlus-TC-Spark", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);
        JavaPairRDD<Long, Long> edges = input.mapToPair(new PairFunction<String, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(String line) throws Exception {
                if (line.startsWith("#"))
                    return null;

                StringTokenizer tokenizer = new StringTokenizer(line);
                long e1, e2;

                if (tokenizer.hasMoreTokens()) {
                    e1 = Long.parseLong(tokenizer.nextToken());
                    if (!tokenizer.hasMoreTokens())
                        throw new RuntimeException("invalid edge line " + line);
                    e2 = Long.parseLong(tokenizer.nextToken());
                    // Input contains reciprocal edges, only need one.
                    if (e2 < e1) {
                        return new Tuple2<>(e2, e1);
                    }
                    return new Tuple2<>(e1, e2);
                }
                return null;
            }
        }).filter(t -> t != null);

        JavaPairRDD<Long, Iterable<Long>> neighborList = edges.groupByKey();

        long nodes = neighborList.count();

        JavaPairRDD<String, Long> triads = neighborList
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<Long>>, String, Long>() {
            long zero = 0;
            int size = 0;
            long one = 1;
            long[] vArray = new long[4096];

            @Override
            public Iterable<Tuple2<String, Long>> call(Tuple2<Long, Iterable<Long>> tuple) throws Exception {
                // Produce triads - all permutations of pairs where e1 < e2 (value=1).
                // And all original edges (value=0).
                // Sorted by value.
                List<Tuple2<String, Long>> output = new ArrayList<>();
                Iterable<Long> values = tuple._2;
                Long key = tuple._1;

                Iterator<Long> vs = values.iterator();
                for (size = 0; vs.hasNext(); ) {
                    if (vArray.length == size) {
                        vArray = Arrays.copyOf(vArray, vArray.length * 2);
                    }

                    long e = vs.next();
                    vArray[size++] = e;

                    // Original edge.
                    output.add(new Tuple2<>(key + " " + Long.toString(e), zero));
                }

                Arrays.sort(vArray, 0, size);

                // Generate triads.
                for (int i = 0; i < size; ++i) {
                    for (int j = i + 1; j < size; ++j) {
                        output.add(new Tuple2<>(Long.toString(vArray[i]) + " " + Long.toString(vArray[j]), one));
                    }
                }
                return output;
            }
        });

        JavaRDD<Long> triangles = triads.groupByKey().map(t -> {
            long count = 0L;
            boolean triangleFound = false;
            for (Long value : t._2) {
                count++;
                if (value == 0) { // we have edge here
                    triangleFound = true;
                    count--;
                }
            }

            if (triangleFound)
                return count;
            return 0L;
        });

        long count = triangles.reduce((a, b) -> a + b);

        OutputUtils.printOutputTC(count);
        sc.close();
    }
}
