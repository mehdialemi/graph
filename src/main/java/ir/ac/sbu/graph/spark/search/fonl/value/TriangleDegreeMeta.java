package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.types.Edge;

import java.util.Set;

public class TriangleDegreeMeta extends LabelMeta {

    public Set<Edge> edges;
    public int[] tcArray;
    public int tc;

    public TriangleDegreeMeta() {}

    public TriangleDegreeMeta(int deg, String label, int fonlValueSize) {
        super(deg, label, fonlValueSize);
    }

    public TriangleDegreeMeta(LabelMeta labelMeta) {
        super(labelMeta);
    }

    public void setEdges(Set<Edge> edges) {
        this.edges = edges;
        this.tc = edges.size();
    }

    public void setTcArray(int[] tcArray) {
        this.tcArray = tcArray;
    }
}
