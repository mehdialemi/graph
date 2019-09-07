package ir.ac.sbu.graph.spark.pattern.utils;

import ir.ac.sbu.graph.spark.pattern.query.Query;

import java.util.*;

public class QuerySamples {

    public static Query getSample(String name) {
        switch (name) {
            case "Q1": return q1();
            case "Q2" : return q2();
            case "Q3" : return q3();
            case "Q4" : return q4();
            case "Q5" : return q5();
        }

        throw new RuntimeException("Invalid query sample name: " + name);
    }

    private static Query q1() {
        Map<Integer, List<Integer>> neighbors = new HashMap<>();
        neighbors.put(1, Arrays.asList(2, 3));
        neighbors.put(2, Arrays.asList(3, 1));
        neighbors.put(3, Arrays.asList(2, 1));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "B");
        labelMap.put(3, "C");

        return new Query(neighbors, labelMap);
    }

    private static Query q2() {
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

    private static Query q3() {
        Map<Integer, List<Integer>> neighbors = new HashMap<>();
        neighbors.put(1, Arrays.asList(2, 3, 4));
        neighbors.put(2, Arrays.asList(1, 3, 4, 5));
        neighbors.put(3, Arrays.asList(1, 2, 4, 5));
        neighbors.put(4, Arrays.asList(1, 2, 3));
        neighbors.put(5, Arrays.asList(2, 3));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "A");
        labelMap.put(3, "A");
        labelMap.put(4, "A");
        labelMap.put(5, "A");

        return new Query(neighbors, labelMap);
    }

    private static Query q4() {
        Map<Integer, List<Integer>> neighbors = new HashMap<>();
        neighbors.put(1, Arrays.asList(2, 3, 4, 5));
        neighbors.put(2, Arrays.asList(1, 3, 4, 5));
        neighbors.put(3, Arrays.asList(1, 2, 4, 5));
        neighbors.put(4, Arrays.asList(1, 2, 3, 5));
        neighbors.put(5, Arrays.asList(1, 2, 3, 4));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "B");
        labelMap.put(3, "C");
        labelMap.put(4, "D");
        labelMap.put(5, "E");

        return new Query(neighbors, labelMap);
    }

    private static Query q5() {
        Map<Integer, List<Integer>> neighbors = new HashMap<>();
        neighbors.put(1, Arrays.asList(2, 3));
        neighbors.put(2, Arrays.asList(1, 3, 4));
        neighbors.put(3, Arrays.asList(1, 2, 6));
        neighbors.put(4, Arrays.asList(2, 5));
        neighbors.put(5, Arrays.asList(4, 6));
        neighbors.put(6, Arrays.asList(3, 4));

        Map <Integer, String> labelMap = new HashMap <>();
        labelMap.put(1, "A");
        labelMap.put(2, "B");
        labelMap.put(3, "C");
        labelMap.put(4, "D");
        labelMap.put(5, "A");
        labelMap.put(6, "A");

        return new Query(neighbors, labelMap);
    }

    private static Query q2Default() {
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
