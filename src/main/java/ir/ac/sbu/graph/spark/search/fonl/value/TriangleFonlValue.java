package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.types.Edge;

public class TriangleFonlValue extends  FonlValue<TriangleMeta>  {

    public TriangleFonlValue() { }

    public TriangleFonlValue(int degree, int[] fonl, Iterable<Edge> edges) {
        this.fonl = fonl;
        this.meta = new TriangleMeta(degree, edges, fonl);
    }
}
