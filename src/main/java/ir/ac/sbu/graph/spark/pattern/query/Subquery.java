package ir.ac.sbu.graph.spark.pattern.query;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * A part of query like a fonl row of query graph. In this class, we have arrays of:
 * - vertices: index 0 is key vertex, other sorted filtered neighbors of key vertex
 * - labels: labels corresponding to vertices
 * - degrees: degrees corresponding to vertices
 * - tc: triangle counts corresponding to vertices
 * - linkIndices: index of vertices to connect to other sub-queries
 * - cliques: index of vertices which are connected all together
 */
public class Subquery implements Serializable {
    public int[] vertices; // vertex in index 0 is connected with all other vertices
    public String[] labels;
    public int[] degrees; // sorted ascending
    public int[] tc;
    public IntSet linkIndices = new IntOpenHashSet(); // index vertices
    public Int2ObjectMap<IntSet> right2Left = new Int2ObjectOpenHashMap<>();
    public Int2ObjectMap<IntSet> l2rIndex = new Int2ObjectOpenHashMap<>();

    public Subquery() {
    }

    public Subquery(int size, List<Tuple2<Integer, Integer>> triangleIndex) {
        vertices = new int[size];
        labels = new String[size];
        degrees = new int[size];

        int v1Index = 0;
        tc = new int[size];
        for (Tuple2<Integer, Integer> t : triangleIndex) {
            int v2Index = t._1 + 1;
            int v3Index = t._2 + 1;

            if (v3Index != v2Index) {
                right2Left.computeIfAbsent(v3Index, v -> new IntOpenHashSet()).add(v2Index);
                l2rIndex.computeIfAbsent(v2Index, v -> new IntOpenHashSet()).add(v3Index);
            }

            tc[v1Index]++;
            tc[v2Index]++;
            tc[v3Index]++;
        }

        linkIndices = new IntOpenHashSet();
    }

    public int maxDegree() {
        return degrees[degrees.length - 1];
    }

    public int size() {
        return vertices.length;
    }

    public int tc(int idx) {
        if (tc == null)
            return 0;
        return tc[idx];
    }

    @Override
    public String toString() {
        return "vertices: " + Arrays.toString(vertices) + ", labels: " + Arrays.toString(labels) +
                ", degrees: " + Arrays.toString(degrees) + ", tc: " + Arrays.toString(tc) +
                ", linkIndices: " + linkIndices + ", right2Left: " + right2Left;
    }
}
