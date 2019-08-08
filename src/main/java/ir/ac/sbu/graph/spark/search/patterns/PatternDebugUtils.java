package ir.ac.sbu.graph.spark.search.patterns;

import ir.ac.sbu.graph.spark.search.fonl.value.LabelDegreeTriangleFonlValue;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import scala.Tuple3;

public class PatternDebugUtils {

    public static void printFonlLabelDegreeTriangleFonlValue(JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> fonl) {
        for (Tuple2<Integer, LabelDegreeTriangleFonlValue> t : fonl.collect()) {
            System.out.println("LabelDegreeTriangleFonlValue: " + t);
        }
    }

    public static void printMatchCounter(JavaPairRDD<Integer, Long> matchCounter, int v) {
        for (Tuple2<Integer, Long> t : matchCounter.collect()) {
            System.out.println("V: " + v + ", MATCH_COUNTER: " + t);
        }
    }

    public static void printSliceMatch(JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> sliceMatch, int v) {
        for (Tuple2<Integer, Tuple3<Integer, Integer, Integer>> t : sliceMatch.collect()) {
            System.out.println("V: " + v + ", SLICE_MATCH: " + t);
        }
    }
}
