package ir.ac.sbu.graph.clusteringco;

import ir.ac.sbu.graph.utils.GraphLoader;
import ir.ac.sbu.graph.utils.GraphUtils;
import ir.ac.sbu.graph.utils.OutUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * This class implements the node iterator++ triangle counter in the spark.
 */
public class NodeIteratorPlusGCC_Spark {

    public static void main(String[] args) {
//        String inputPath = "input.txt";
        String inputPath = "/home/mehdi/ir.ac.sbu.graph-data/com-amazon.ungraph.txt";
        if (args.length > 0)
            inputPath = args[0];

        int partition = 2;
        if (args.length > 1)
            partition = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf();
        if (args.length == 0)
            conf.setMaster("local[2]");
        GraphUtils.setAppName(conf, "NodeIter-GCC-Spark", partition, inputPath);
        conf.registerKryoClasses(new Class[]{GraphUtils.class, GraphUtils.VertexDegree.class, long[].class});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> input = sc.textFile(inputPath, partition);

        JavaPairRDD<Long, Long> uEdges = GraphLoader.loadEdges(input);
        JavaPairRDD<Long, Long[]> neighborList = uEdges.groupByKey().mapToPair(tuple -> {
            HashSet<Long> set = new HashSet<>();
            for (long neighbor : tuple._2)
                set.add(neighbor);
            return new Tuple2<>(tuple._1, set.toArray(new Long[0]));
        });

        long nodes = neighborList.count();

        JavaPairRDD<String, Long> triads = neighborList
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Long[]>, String, Long>() {
            long zero = 0;
            int size = 0;
            long one = 1;
            long[] vArray = new long[4096];

            @Override
            public Iterator<Tuple2<String, Long>> call(Tuple2<Long, Long[]> tuple) throws Exception {
                // Produce triads - all permutations of pairs where e1 < e2 (value=1).
                // And all original edges (value=0).
                // Sorted by value.
                List<Tuple2<String, Long>> output = new ArrayList<>();
                Long[] vs = tuple._2;
                Long key = tuple._1;

                size = 0;
                for (long e: vs) {
                    if (e > key) {
                        if (vArray.length == size) {
                            vArray = Arrays.copyOf(vArray, vArray.length * 2);
                        }

                        vArray[size++] = e;

                        // Original edge.
                        output.add(new Tuple2<>(key + " " + Long.toString(e), zero));
                    }
                }

                Arrays.sort(vArray, 0, size);

                // Generate triads.
                for (int i = 0; i < size; ++i) {
                    for (int j = i + 1; j < size; ++j) {
                        output.add(new Tuple2<>(Long.toString(vArray[i]) + " " + Long.toString(vArray[j]), one));
                    }
                }
                return output.iterator();
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

        long totalTriangles = triangles.reduce((a, b) -> a + b);
        float globalCC = totalTriangles / (float) (nodes * (nodes - 1));

        OutUtils.printOutputGCC(nodes, totalTriangles, globalCC);
        sc.close();
    }
}
