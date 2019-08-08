package ir.ac.sbu.graph.spark.search.patterns;

import java.util.*;

public class Samples {


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

    public static Query mySampleEmptyLabel() {
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
