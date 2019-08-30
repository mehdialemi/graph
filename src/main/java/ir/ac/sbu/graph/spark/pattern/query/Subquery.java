package ir.ac.sbu.graph.spark.pattern.query;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A part of query like a fonl row of query graph. In this class, we have arrays of:
 * - vertices: index 0 is key vertex, other sorted filtered neighbors of key vertex
 * - labels: labels corresponding to vertices
 * - degrees: degrees corresponding to vertices
 * - tc: triangle counts corresponding to vertices
 * - links: index of vertices to connect to other sub-queries
 * - cliques: index of vertices which are connected all together
 */
public class Subquery implements Serializable {
    public int[] vertices; // vertex in index 0 is connected with all other vertices
    public String[] labels;
    public int[] degrees; // sorted ascending
    public int[] tc;
    public IntSet links = new IntOpenHashSet(); // index vertices
    private int[][] cliques; // index of vertices connected with each other
    public Int2ObjectMap<IntSet> parentIndices = new Int2ObjectOpenHashMap<>();
    public Int2ObjectMap<IntSet> idx2Indices = new Int2ObjectOpenHashMap<>();

    public Subquery() { }

    public Subquery(int size, int linkSize, List<Tuple2<Integer, Integer>> triangleIndex) {
        vertices = new int[size];
        labels = new String[size];
        degrees = new int[size];

        if (triangleIndex.size() > 0) {
            tc = new int[size];
            for (Tuple2<Integer, Integer> t : triangleIndex) {
                parentIndices.computeIfAbsent(t._1, v -> new IntOpenHashSet()).add(0);
                parentIndices.computeIfAbsent(t._2, v -> new IntOpenHashSet()).add(0);
                parentIndices.computeIfAbsent(t._2, v -> new IntOpenHashSet()).add(t._1);

                parentIndices.computeIfAbsent(0, v -> new IntOpenHashSet()).add(t._1);
                parentIndices.computeIfAbsent(0, v -> new IntOpenHashSet()).add(t._2);
                parentIndices.computeIfAbsent(t._1, v -> new IntOpenHashSet()).add(t._2);

                tc[0] ++; tc[t._1] ++; tc[t._2] ++;
            }
        }

        if (linkSize > 0) {
            links = new IntOpenHashSet();
        }
    }

    public int maxDegree() {
        return degrees[degrees.length - 1];
    }

    public int size() {
        return vertices.length;
    }

    public boolean hasLink() {
        return !links.isEmpty();
    }

    /**
     * Check if the given index is not connected to other vertices, except index 0.
     * To do this, tc array is taken into account. So that, if tc[index] > 0 then the result is false.
     * Otherwise, the result is true
     * @param index of vertex in the vertices array
     * @return true if the corresponding vertex of the index has no more than one neighbor
     */
    private boolean isFree(int index) {
        return tc[index] > 0;
    }

    public void addCliques(List<Tuple2<Integer, Integer>> triangleIndex) {
        // vertex in index 0 is connected to all other vertices in index 1 .. len - 1
        Int2ObjectMap<IntSet> cMapSet = new Int2ObjectOpenHashMap<>();
        for (Tuple2<Integer, Integer> edge : triangleIndex) {
            cMapSet.computeIfAbsent(edge._1, v -> new IntOpenHashSet()).add(edge._2);
        }
        Int2ObjectMap<IntSet> cMapSetBackup = new Int2ObjectOpenHashMap<>(cMapSet);

        int[] indices = cMapSet.keySet().toIntArray();
        for (int i = 0; i < indices.length; i++) {
            int index = indices[i];
            IntSet vIdxNeighbors = cMapSet.get(index);
            for (int vIdxNeighbor : vIdxNeighbors.toIntArray()) {
                IntSet vIdxNeighbor2 = cMapSet.get(vIdxNeighbor);
                vIdxNeighbors.retainAll(vIdxNeighbor2);
                vIdxNeighbors.add(vIdxNeighbor);
            }
        }

        for (int index : indices) {
            IntSet vIdxNeighbors = cMapSetBackup.get(index);
            vIdxNeighbors.removeAll(cMapSet.get(index));
        }

        List<int[]> list = createIdxCliques(cMapSet);
        list.addAll(createIdxCliques(cMapSetBackup));

        if (list.size() > 0) {
            cliques = new int[list.size()][];
            for (int i = 0; i < list.size(); i++) {
                cliques[i] = list.get(i);
            }
        }
    }

    private List<int[]> createIdxCliques(Int2ObjectMap<IntSet> map) {
        List<int[]> cSet = new ArrayList<>();
        for (Map.Entry<Integer, IntSet> entry : map.entrySet()) {
            IntSet set = entry.getValue();
            if (set.isEmpty())
                continue;
            set.add(entry.getKey());
            cSet.add(set.toIntArray());
        }
        return cSet;
    }

    public boolean hasCliques() {
        return cliques != null;
    }

    public int tc(int idx) {
        if (tc == null)
            return 0;
        return tc[idx];
    }
}
