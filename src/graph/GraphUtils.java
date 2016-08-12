package graph;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class GraphUtils implements Serializable {

    public static void setAppName(SparkConf conf, String name, int defaultPartition, String inputPath) {
        conf.setAppName(name + "-" + defaultPartition + "-" + new File(inputPath).getName());
    }

     public static int sortedIntersectionCount(long[] hDegs, long[] forward, List<Tuple2<Long, Integer>> output, int
        hIndex,
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

    public static List<Long> sortedIntersection(long[] hDegs, long[] forward, int hIndex, int fIndex) {
        int fLen = forward.length;
        int hLen = hDegs.length;

        List<Long> list = new ArrayList<>();

        if (hDegs.length == 0 || fLen == 0)
            return list;

        boolean leftRead = true;
        boolean rightRead = true;

        long h = 0;
        long f = 0;

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
                list.add(h);
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
        return list;
    }

    public static int sortedIntersectionCountInt(int[] hDegs, int[] forward, List<Tuple2<Integer, Integer>> output, int
        hIndex,
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
        public long vertex;
        public int degree;

        public VertexDegree() {}

        public VertexDegree(long vertex, int degree) {
            this.vertex = vertex;
            this.degree = degree;
        }
    }

    public static class CandidateState {
        public int[] higherIds;
        public int firstNodeId;

        public CandidateState(int nodeId, int[] neighbors) {
            this.higherIds = neighbors;
            this.firstNodeId = nodeId;
        }
    }

    public static Tuple3<Long, Long, Long> createSorted(long u, long v, long w) {
        long first = u, second = v, third = w;
        if (u < v) {
            if (v < w) {
                first = u;
                second = v;
                third = w;
            } else {
                third = v;
                if (u < w) {
                    first = u;
                    second = w;
                } else {
                    first = w;
                    second = u;
                }
            }
        } else { // u > v
            if (v > w) { // u > v > w
                first = w;
                second = v;
                third = u;
            } else { // u > v & v < w
                first = v;
                if (u < w) {
                    second = u;
                    third = w;
                } else {
                    second = w;
                    third = u;
                }
            }
        }

        return new Tuple3<>(first, second, third);
    }
}
