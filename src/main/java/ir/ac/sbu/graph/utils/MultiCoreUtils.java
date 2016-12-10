package ir.ac.sbu.graph.utils;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for multi-core programming
 */
public class MultiCoreUtils {

    public static List<Tuple2<Integer, Integer>> createBuckets(int threads, int len) {
        return createBuckets(threads, 0, len);
    }

    public static List<Tuple2<Integer, Integer>> createBuckets(int threads, int offset, int len) {
        List<Tuple2<Integer, Integer>> starts = new ArrayList<>(threads);
        int bucketLen = (len - offset) / threads;
        int startIndex = offset;
        for (int i = 0; i < threads; i++) {
            if (i == threads - 1)
                starts.add(new Tuple2<>(startIndex, len));
            else
                starts.add(new Tuple2<>(startIndex, startIndex + bucketLen));
            startIndex += bucketLen;
        }
        return starts;
    }
}
