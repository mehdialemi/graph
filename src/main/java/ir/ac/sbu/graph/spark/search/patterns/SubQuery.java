package ir.ac.sbu.graph.spark.search.patterns;

import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class SubQuery implements Serializable {
    public int vertex;
    public String label;
    public int degree;
    public int[] fonl;
    public String[] labels;
    public int[] degrees;
    public int[] vTc;
    public int tc;
    public Set<Edge> edges = new HashSet <>();
    public Int2IntMap v2i;
    public int[] anchors;

    public void addEdge(Edge edge) {
        if (edges == null)
            edges = new HashSet <>();
        edges.add(edge);
    }

    public int maxDegree() {
        return degrees[degrees.length - 1];
    }
}
