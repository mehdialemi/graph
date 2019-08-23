package ir.ac.sbu.graph.spark.pattern.index.fonl.value;

import ir.ac.sbu.graph.types.Edge;

public class TriangleFonlValue extends  FonlValue<TriangleMeta>  {

    public TriangleFonlValue() { }

    public TriangleFonlValue(int degree, int[] fonl, Iterable<Edge> triangleEdges) {
        this.fonl = fonl;
        this.meta = new TriangleMeta(degree, fonl, triangleEdges);
    }
}
