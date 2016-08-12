package graph;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tries to load graph into a key value based structure.
 */
public class GraphLoader {

    public static JavaPairRDD<Long, Long> loadEdges(JavaRDD<String> input) {
        JavaPairRDD<Long, Long> edges = input.flatMapToPair(new PairFlatMapFunction<String, Long, Long>() {
            @Override
            public Iterator<Tuple2<Long, Long>> call(String line) throws Exception {
                List<Tuple2<Long, Long>> list = new ArrayList<>();
                if (line.startsWith("#"))
                    return list.iterator();
                String[] s = line.split("\\s+");
                try {
                    long e1 = Long.parseLong(s[0]);
                    long e2 = Long.parseLong(s[1]);
                    list.add(new Tuple2<>(e1, e2));
                    list.add(new Tuple2<>(e2, e1));
                } catch (Throwable e) {
                    System.out.println("Could not parse line " + line);
                }
                return list.iterator();
            }
        });
        return edges;
    }

    public static JavaPairRDD<Integer, Integer> loadEdgesInt(JavaRDD<String> input) {
        JavaPairRDD<Integer, Integer> edges = input.flatMapToPair(new PairFlatMapFunction<String, Integer, Integer>() {

            @Override
            public Iterator<Tuple2<Integer, Integer>> call(String line) throws Exception {
                List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                if (line.startsWith("#"))
                    return list.iterator();
                String[] s = line.split("\\s+");
                int e1 = Integer.parseInt(s[0]);
                int e2 = Integer.parseInt(s[1]);
                list.add(new Tuple2<>(e1, e2));
                list.add(new Tuple2<>(e2, e1));
                return list.iterator();
            }
        });
        return edges;
    }

    public static JavaRDD<Tuple2<Long, Long>> loadEdgeListSorted(JavaRDD<String> input) {
        return input.flatMap(line -> {
            if (line.startsWith("#"))
                return new ArrayList<Tuple2<Long, Long>>().iterator();
            List<Tuple2<Long, Long>> list = new ArrayList<>(1);
            String[] e = line.split("\\s+");
            long v1 = Long.parseLong(e[0]);
            long v2 = Long.parseLong(e[1]);
            if (v1 < v2)
                list.add(new Tuple2<>(v1, v2));
            else if (v1 > v2)
                list.add(new Tuple2<>(v2, v1));
            return list.iterator(); // no self loop is accepted
        });
    }
}
