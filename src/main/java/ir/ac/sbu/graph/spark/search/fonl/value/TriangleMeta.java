package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class TriangleMeta extends Meta {

    protected int[] v1Array;
    protected int[] v2Array;

    public TriangleMeta() { }

    public TriangleMeta(int degree, Iterable<Edge> edges) {
        super(degree);
        IntList v1List = new IntArrayList();
        IntList v2List = new IntArrayList();
        for (Edge edge : edges) {
            v1List.add(edge.v1);
            v2List.add(edge.v2);
        }

        v1Array = v1List.toIntArray();
        v2Array = v2List.toIntArray();
    }
}
