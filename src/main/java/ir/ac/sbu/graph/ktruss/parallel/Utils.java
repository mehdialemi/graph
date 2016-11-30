
package ir.ac.sbu.graph.ktruss.parallel;

import scala.Tuple2;
import scala.Tuple3;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static ir.ac.sbu.graph.MultiCoreUtils.createBuckets;

/**
 * Utility used in parallel ktruss
 */
public class Utils {
    public static final double MAX_CHECK_RATIO = 0.3;
    private static final Tuple3<Integer, Integer, Integer> INVALID_TUPLE3 = new Tuple3<>(-1, -1, -1);

    public static Tuple2<Integer, int[]> sort(final Set<Integer>[] eTriangles,
                                              final Tuple2<Integer, int[]> prevEdges,
                                              int threads, int minSup) throws Exception {

        final boolean usePrev = prevEdges == null || prevEdges._2 == null ||
            (prevEdges != null && prevEdges._1 < prevEdges._2.length * MAX_CHECK_RATIO) ? false : true;
        List<Tuple2<Integer, Integer>> buckets =
            usePrev ? createBuckets(threads, prevEdges._1, prevEdges._2.length) : createBuckets(threads, eTriangles.length);
        Tuple3<Integer, Integer, Integer> minMaxLen = buckets.parallelStream().map(bucket -> {
            int min = eTriangles.length;
            int max = 0;
            int len = 0;
            if (usePrev) {
                for (int i = bucket._1; i < bucket._2; i++) {
                    if (eTriangles[prevEdges._2[i]] == null || eTriangles[prevEdges._2[i]].isEmpty())
                        continue;
                    int num = eTriangles[prevEdges._2[i]].size();
                    if (num < min) {
                        min = num;
                    } else if (num > max) {
                        max = num;
                    }
                    len++;
                }
            } else {
                for (int i = bucket._1; i < bucket._2; i++) {
                    if (eTriangles[i] == null || eTriangles[i].isEmpty())
                        continue;
                    int num = eTriangles[i].size();
                    if (num < min) {
                        min = num;
                    } else if (num > max) {
                        max = num;
                    }
                    len++;
                }
            }
            return new Tuple3<>(min, max, len);
        }).reduce((r1, r2) -> new Tuple3<>(Math.min(r1._1(), r2._1()), Math.max(r1._2(), r2._2()), r1._3() + r2._3()))
            .orElse(INVALID_TUPLE3);

        if (minMaxLen == INVALID_TUPLE3)
            throw new Exception("Invalid tuple");

        int min = minMaxLen._1();
        int max = minMaxLen._2();
        int length = minMaxLen._3();

        // init the frequencies
        AtomicInteger[] counts = new AtomicInteger[max - min + 1];
        for (int i = 0; i < counts.length; i++)
            counts[i] = new AtomicInteger(0);

        buckets.parallelStream().forEach(bucket -> {
            if (usePrev) {
                for (int i = bucket._1; i < bucket._2; i++) {
                    Set<Integer> et = eTriangles[prevEdges._2[i]];
                    if (et == null || et.isEmpty())
                        continue;
                    counts[eTriangles[prevEdges._2[i]].size() - min].incrementAndGet();
                }
            } else {
                for (int i = bucket._1; i < bucket._2; i++) {
                    Set<Integer> et = eTriangles[i];
                    if (et == null || et.isEmpty())
                        continue;
                    counts[et.size() - min].incrementAndGet();
                }
            }
        });

        counts[0].decrementAndGet();
        for (int i = 1; i < counts.length; i++) {
            counts[i].addAndGet(counts[i - 1].get());
        }

        int minSupIndex = 0;
        int[] edges = new int[length];
        if (usePrev) {
            for (int i = prevEdges._1; i < prevEdges._2.length; i++) {
                Set<Integer> et = eTriangles[prevEdges._2[i]];
                if (et == null || et.isEmpty())
                    continue;
                int sup = et.size();
                int index = counts[sup - min].getAndDecrement();
                edges[index] = prevEdges._2[i];
                if (sup < minSup)
                    minSupIndex++;
            }
        } else {
            for (int i = eTriangles.length - 1; i >= 0; i--) {
                Set<Integer> et = eTriangles[i];
                if (et == null || et.isEmpty())
                    continue;
                int sup = et.size();
                int index = counts[sup - min].getAndDecrement();
                edges[index] = i;
                if (sup < minSup)
                    minSupIndex++;
            }
        }
        return new Tuple2<>(minSupIndex, edges);
    }

    public static void updateEdgeMap(Map<Long, Set<Integer>> eMap, Map<Long, Set<Integer>> eVerticesMap) {
        eVerticesMap.entrySet().forEach(eVertices -> {
            Set<Integer> tv = eMap.get(eVertices.getKey());
            if (tv == null)
                eMap.put(eVertices.getKey(), eVertices.getValue());
            else
                tv.addAll(eVertices.getValue());
        });
    }
}
