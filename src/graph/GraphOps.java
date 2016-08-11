package graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import graph.ktruss.old.FdValue;
import graph.ktruss.old.Fdonl.VertexDegree;
import graph.ktruss.old.Triangle;
import graph.ktruss.old.TriangleListGenerator.CandidateState;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

public class GraphOps {
    /**
     * Setup a distributed spark context
     * @param appName application name
     * @return A configured JavaSparkContext
     */
    public static JavaSparkContext setupContext(String appName) {
        SparkConf conf = new SparkConf();
        conf.setAppName(appName);
        registerSerilizer(conf);
        return new JavaSparkContext(conf);
    }

    /**
     * Register spark serializer and classes that need to be serialized by that serializer
     * Here we register kryoSerializer
     * @param conf serializer configuration will be added to this configuration
     */
    private static void registerSerilizer(SparkConf conf) {
        conf.registerKryoClasses(new Class[] { FdValue.class, VertexDegree.class, Triangle.class, CandidateState.class, long[].class });
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    }

    public static JavaSparkContext setupLocalContext(String appName) {
        return setupLocalContext(appName, 1);
    }

    /**
     * Setup a local spark context
     * @param appName application name
     * @param threadNum number of local workers
     * @return A configured JavaSparkContext
     */
    public static JavaSparkContext setupLocalContext(String appName, int threadNum) {
        SparkConf conf = new SparkConf();
        registerSerilizer(conf);

        if (threadNum < 0)
            threadNum *= -1;
        if (threadNum > 4)
            threadNum = 4;
        conf.setMaster("local[" + threadNum + "]");

        conf.setAppName(appName);
        return new JavaSparkContext(conf);
    }

    public static JavaRDD<String> loadInput(JavaSparkContext sc, String inputPath) {
        return sc.textFile(inputPath);
    }

    public static JavaRDD<String> loadInput(JavaSparkContext sc, String inputPath, int minPartitions) {
        return sc.textFile(inputPath, minPartitions);
    }

    public static JavaPairRDD<Long, Iterable<Long>> getNeighbors(JavaRDD<String> lines) {
        JavaPairRDD<Long, Long> edges = createEdgeList(lines);
        return edges.groupByKey();
    }

    public static JavaPairRDD<Long, Long> createEdgeList(JavaRDD<String> lines) {
        JavaPairRDD<Long, Long> edges = lines.map(line -> line.split("\\s+")).flatMapToPair(new PairFlatMapFunction<String[], Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(String[] str) throws Exception {
                List<Tuple2<Long, Long>> list = new ArrayList<> ();
                list.add(new Tuple2<>(Long.parseLong(str[0]), Long.parseLong(str[1])));
                list.add(new Tuple2<>(Long.parseLong(str[1]), Long.parseLong(str[0])));
                return list.iterator();
            }
        });
        return edges;
    }
}
