package ir.ac.sbu.graph.fonl;

import ir.ac.sbu.graph.types.Edge;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LocalTriangle extends LocalFonl {
    public Map<Integer, Set<Edge>> edges = new HashMap <>();
    public int[][] nArray;
    public int[][] vArray;
}
