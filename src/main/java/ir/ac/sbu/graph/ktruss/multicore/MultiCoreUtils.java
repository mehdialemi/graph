package ir.ac.sbu.graph.ktruss.multicore;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for multi-core programming
 */
public class MultiCoreUtils {

    public static List<Tuple2<Integer, Integer>> createBuckets(int threads, int len) {
        List<Tuple2<Integer, Integer>> starts = new ArrayList<>(threads);
        int bucket = len / threads;
        for (int i = 0; i < threads; i++) {
            int start = i * bucket;
            if (i == threads - 1)
                starts.add(new Tuple2<>(start, len));
            else
                starts.add(new Tuple2<>(start, start + bucket));
        }
        return starts;
    }

    public static <T> List<Tuple2<Integer, Integer>> createBuckets(int threads, T[] list) {
        List<Tuple2<Integer, Integer>> starts = new ArrayList<>(threads);
        int bucket = list.length / threads;
        for (int i = 0; i < threads; i++) {
            int start = i * bucket;
            if (i == threads - 1)
                starts.add(new Tuple2<>(start, list.length));
            else
                starts.add(new Tuple2<>(start, start + bucket));
        }
        return starts;
    }

    public static List<Tuple2<Integer, Integer>> createBuckets(int threads, int[] list) {
        return createBuckets(threads, list, 0);
    }

    public static List<Tuple2<Integer, Integer>> createBuckets(int threads, int[] list, int start) {
        List<Tuple2<Integer, Integer>> starts = new ArrayList<>(threads);
        int bucket = (list.length - start) / threads;
        for (int i = 0; i < threads; i++) {
            int s = i * bucket;
            if (i == threads - 1)
                starts.add(new Tuple2<>(s, list.length));
            else
                starts.add(new Tuple2<>(s, s + bucket));
        }
        return starts;
    }

    public static long toLong(int a, int b) {
        return (long)a << 32 | b & 0xFFFFFFFFL;
    }
}
