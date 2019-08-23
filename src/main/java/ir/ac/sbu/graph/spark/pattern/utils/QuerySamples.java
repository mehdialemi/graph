package ir.ac.sbu.graph.spark.pattern.utils;

import ir.ac.sbu.graph.spark.pattern.query.Query;

import java.util.*;

public class QuerySamples {

    public static Query getSample(String name) {
        switch (name) {
            case "sample1": return simple1();
            case "sample2" : return simple2();
        }
        throw new RuntimeException("Invalid query sample name: " + name);
    }

    private static Query simple1() {
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

    private static Query simple2() {
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

        return new Query(neighbors, labelMap);
    }
}
