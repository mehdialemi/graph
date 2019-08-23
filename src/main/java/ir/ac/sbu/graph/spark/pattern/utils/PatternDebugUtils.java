package ir.ac.sbu.graph.spark.pattern.utils;

import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleFonlValue;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.Tuple3;

public class PatternDebugUtils {

    private final static boolean ENABLE_LOG = false;
    public static void printFonlLabelDegreeTriangleFonlValue(JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> fonl) {
        if (!ENABLE_LOG)
            return;
        for (Tuple2<Integer, LabelDegreeTriangleFonlValue> t : fonl.collect()) {
            System.out.println("LabelDegreeTriangleFonlValue: " + t);
        }
    }

    public static void printMatchCounter(JavaPairRDD<Integer, Tuple2<Integer, Long>> matchCounter, int node) {
        if (!ENABLE_LOG)
            return;
        for (Tuple2<Integer, Tuple2<Integer, Long>> t : matchCounter.collect()) {
            System.out.println("NODE: " + node + ", MATCH_COUNTER: " + t);
        }
    }

    public static void printSliceMatch(JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> sliceMatch, int node) {
        if (!ENABLE_LOG)
            return;
        for (Tuple2<Integer, Tuple3<Integer, Integer, Integer>> t : sliceMatch.collect()) {
            System.out.println("NODE: " + node + ", SLICE_MATCH: " + t);
        }
    }

    public static void printFonlLeft( JavaPairRDD<Integer, Iterable<Tuple3<Integer, Integer, Integer>>> fonlLeftKeys ) {
        if (!ENABLE_LOG)
            return;
        for (Tuple2<Integer, Iterable<Tuple3<Integer, Integer, Integer>>> t : fonlLeftKeys.collect()) {
            System.out.println("FONL_LEFT: " + t);
        }
    }

    public static void printFonlSubset(JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> fonlSubset) {
        if (!ENABLE_LOG)
            return;
        for (Tuple2<Integer, LabelDegreeTriangleFonlValue> t : fonlSubset.collect()) {
            System.out.println("FONL_SUBSET: " + t);
        }
    }

    public static void printLinkSliceMatch(JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> linkSliceMatch) {
        if (!ENABLE_LOG)
            return;
        for (Tuple2<Integer, Tuple3<Integer, Integer, Integer>> t : linkSliceMatch.collect()) {
            System.out.println("LINK_SLICE_MATCH: " + t);
        }
    }
}
