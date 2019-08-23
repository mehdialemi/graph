package ir.ac.sbu.graph.spark.pattern.utils;

import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleFonlValue;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

public class PatternDebugUtils {
    private static final Logger logger = LoggerFactory.getLogger(PatternDebugUtils.class);

    public static void printFonlLabelDegreeTriangleFonlValue(JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> fonl) {
        for (Tuple2<Integer, LabelDegreeTriangleFonlValue> t : fonl.collect()) {
            logger.debug("LabelDegreeTriangleFonlValue: {}", t);
        }
    }

    public static void printMatchCounter(JavaPairRDD<Integer, Tuple2<Integer, Long>> matchCounter, int node) {
        for (Tuple2<Integer, Tuple2<Integer, Long>> t : matchCounter.collect()) {
            logger.debug("NODE: {}, MATCH_COUNTER: {}", node, t);
        }
    }

    public static void printSliceMatch(JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> sliceMatch, int node) {
        for (Tuple2<Integer, Tuple3<Integer, Integer, Integer>> t : sliceMatch.collect()) {
            logger.debug("NODE: {}, SLICE_MATCH: {}", node, t);
        }
    }

    public static void printFonlLeft( JavaPairRDD<Integer, Iterable<Tuple3<Integer, Integer, Integer>>> fonlLeftKeys ) {
        for (Tuple2<Integer, Iterable<Tuple3<Integer, Integer, Integer>>> t : fonlLeftKeys.collect()) {
            logger.debug("FONL_LEFT: {}", t);
        }
    }

    public static void printFonlSubset(JavaPairRDD<Integer, LabelDegreeTriangleFonlValue> fonlSubset) {
        for (Tuple2<Integer, LabelDegreeTriangleFonlValue> t : fonlSubset.collect()) {
            logger.debug("FONL_SUBSET: ", t);
        }
    }

    public static void printLinkSliceMatch(JavaPairRDD<Integer, Tuple3<Integer, Integer, Integer>> linkSliceMatch) {
        for (Tuple2<Integer, Tuple3<Integer, Integer, Integer>> t : linkSliceMatch.collect()) {
            logger.debug("LINK_SLICE_MATCH: {}", t);
        }
    }
}
