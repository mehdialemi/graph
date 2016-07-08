package graph.clusteringco;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 *
 */
public class GraphUtils implements Serializable {

    public static void setAppName(SparkConf conf, String name, int defaultPartition, String inputPath) {
        conf.setAppName(name + "-" + defaultPartition + "-" + new File(inputPath).getName());
    }

    public static void printOutputLCC(long nodes, float sumLCC, float avgLCC) {
        System.out.println("Nodes = " + nodes + ", Sum_LCC = " + sumLCC + ", AVG_LCC = " + avgLCC);
    }

    public static void printOutputGCC(long nodes, long triangles, float gcc) {
        System.out.println("Nodes = " + nodes + ", Triangles = " + triangles + ", GCC = " + gcc);
    }

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

    public static JavaPairRDD<Integer, Integer> loadUndirectedEdgesInt(JavaRDD<String> input) {
        JavaPairRDD<Integer, Integer> edges = input.flatMapToPair(new PairFlatMapFunction<String, Integer, Integer>() {

            @Override
            public Iterable<Tuple2<Integer, Integer>> call(String line) throws Exception {
                List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                if (line.startsWith("#"))
                    return list;
                String[] s = line.split("\\s+");
                int e1 = Integer.parseInt(s[0]);
                int e2 = Integer.parseInt(s[1]);
                list.add(new Tuple2<>(e1, e2));
                list.add(new Tuple2<>(e2, e1));
                return list;
            }
        });
        return edges;
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

    static int sortedIntersectionCountInt(int[] hDegs, int[] forward, List<Tuple2<Integer, Integer>> output, int hIndex,
                                       int fIndex) {
        int fLen = forward.length;
        int hLen = hDegs.length;

        if (hDegs.length == 0 || fLen == 0)
            return 0;

        boolean leftRead = true;
        boolean rightRead = true;

        int h = 0;
        int f = 0;
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

    public static class CandidateState {
        int[] higherIds;
        int firstNodeId;

        public CandidateState(int nodeId, int[] neighbors) {
            this.higherIds = neighbors;
            this.firstNodeId = nodeId;
        }
    }

}
