package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.types.Edge;

import java.util.Set;

public class TriangleDegreeMeta extends DegreeMeta {

    private int[] e1Array;
    private int[] e2Array;

    public TriangleDegreeMeta() {}

    public TriangleDegreeMeta(int degree, int size) {
        super(degree, size);
    }

    public TriangleDegreeMeta(LabelMeta labelMeta) {
        super(labelMeta);
    }

    public void setEdges(Set<Edge> edges) {
        this.e1Array = new int[edges.size()];
        this.e2Array = new int[edges.size()];

        int i = 0;
        for (Edge edge : edges) {
            e1Array[i] = edge.v1;
            e2Array[i] = edge.v2;
            i ++;
        }
    }
}
