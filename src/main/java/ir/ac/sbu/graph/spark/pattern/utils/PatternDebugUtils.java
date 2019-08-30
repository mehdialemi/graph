package ir.ac.sbu.graph.spark.pattern.utils;

import ir.ac.sbu.graph.spark.pattern.index.IndexRow;
import ir.ac.sbu.graph.spark.pattern.search.MatchCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class PatternDebugUtils {
    private static final Logger logger = LoggerFactory.getLogger(PatternDebugUtils.class);

    public static void printFonlLabelDegreeTriangleFonlValue(JavaPairRDD<Integer, MatchCount> fonl) {
        for (Tuple2<Integer, MatchCount> t : fonl.collect()) {
            logger.debug("IndexValue: {}", t);
        }
    }

    public static void printMatchCounter(JavaPairRDD<Integer, Tuple2<Integer, Long>> matchCounter,
                                         int node) {
        for (Tuple2<Integer, Tuple2<Integer, Long>> t : matchCounter.collect()) {
            logger.debug("NODE: {}, MATCH_COUNTER: {}", node, t);
        }
    }

    public static void printSliceMatch(JavaPairRDD<Integer,MatchCount> sliceMatch, int node) {
        for (Tuple2<Integer, MatchCount> t : sliceMatch.collect()) {
            logger.debug("NODE: {}, SLICE_MATCH: {}", node, t);
        }
    }

    public static void printFonlLeft( JavaPairRDD<Integer, Iterable<MatchCount>> fonlLeftKeys ) {
        for (Tuple2<Integer, Iterable<MatchCount>> t : fonlLeftKeys.collect()) {
            logger.debug("FONL_LEFT: {}", t);
        }
    }

    public static void printFonlSubset(JavaPairRDD<Integer, IndexRow> fonlSubset) {
        for (Tuple2<Integer, IndexRow> t : fonlSubset.collect()) {
            logger.debug("FONL_SUBSET: ", t);
        }
    }

    public static void printLinkSliceMatch(JavaPairRDD<Integer, MatchCount> linkSliceMatch) {
        for (Tuple2<Integer, MatchCount> t : linkSliceMatch.collect()) {
            logger.debug("LINK_SLICE_MATCH: {}", t);
        }
    }
}
