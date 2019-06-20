package ir.ac.sbu.graph.spark.search.patterns;

import ir.ac.sbu.graph.spark.search.fonl.value.TriangleFonlValue;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class PatternDebugUtils {

    public static void printMatches(String title, JavaPairRDD<Integer, Tuple2<long[], int[][]>> candidates) {
        List<Tuple2 <Integer, Tuple2 <long[], int[][]>>> collect = candidates.collect();
        System.out.println("((((((((((((( Candidates (count: " + collect.size() + ") ** " + title + " ** ))))))))))))))");
        for (Tuple2 <Integer, Tuple2 <long[], int[][]>> entry : collect) {
            StringBuilder str = new StringBuilder("Key: " + entry._1 + ", Values => ");
            str.append("counts: ").append(Arrays.toString(entry._2._1)).append(" ");
            for (int[] array : entry._2._2) {
                str.append(Arrays.toString(array)).append(" , ");
            }
            System.out.println(str.toString());
        }
    }

    public static void printSubMatches(String title, JavaPairRDD <Integer, Tuple2 <Long, int[]>> partial) {
        List <Tuple2 <Integer, Tuple2 <Long, int[]>>> collect = partial.collect();
        System.out.println("((((((((((((( Partials (count: " + collect.size() + ") ** " + title + " ** ))))))))))))))");
        for (Tuple2 <Integer, Tuple2 <Long, int[]>> entry : collect) {
            StringBuilder sb = new StringBuilder("Key: " + entry._1 + ", Values: ");
            sb.append(Arrays.toString(entry._2._2));
            System.out.println(sb);
        }
    }

    public static void printFonl(JavaPairRDD <Integer, TriangleFonlValue> labelFonl) {
        List <Tuple2 <Integer, TriangleFonlValue>> collect = labelFonl.collect();
        for (Tuple2 <Integer, TriangleFonlValue> t : collect) {
            System.out.println(t);
        }
    }
}
