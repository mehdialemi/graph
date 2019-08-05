package ir.ac.sbu.graph.spark.search.patterns;

import ir.ac.sbu.graph.spark.search.fonl.creator.LocalFonlCreator;
import ir.ac.sbu.graph.spark.search.fonl.local.QFonl;

import java.util.*;

public class Samples {

    public static QFonl mySample() {
        Map<Integer, List<Integer>> neighbors = new HashMap<>();
        neighbors.put(1, Arrays.asList(4, 2, 3));
        neighbors.put(2, Arrays.asList(3, 1));
        neighbors.put(3, Arrays.asList(2, 1));
        neighbors.put(4, Collections.singletonList(1));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "B");
        labelMap.put(3, "C");
        labelMap.put(4, "A");

        return LocalFonlCreator.createQFonl(neighbors, labelMap);
    }

    public static Query mySampleQuery() {
        Map<Integer, List<Integer>> neighbors = new HashMap<>();
        neighbors.put(1, Arrays.asList(4, 2, 3));
        neighbors.put(2, Arrays.asList(3, 1));
        neighbors.put(3, Arrays.asList(2, 1));
        neighbors.put(4, Collections.singletonList(1));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "B");
        labelMap.put(3, "C");
        labelMap.put(4, "A");

        return new Query(neighbors, labelMap);
    }

    public static QFonl mySampleEmptyLabel() {
        Map<Integer, List<Integer>> neighbors = new HashMap<>();
        neighbors.put(1, Arrays.asList(4, 2, 3));
        neighbors.put(2, Arrays.asList(3, 1));
        neighbors.put(3, Arrays.asList(2, 1));
        neighbors.put(4, Collections.singletonList(1));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "_");
        labelMap.put(2, "_");
        labelMap.put(3, "_");
        labelMap.put(4, "_");

        return LocalFonlCreator.createQFonl(neighbors, labelMap);
    }
}
