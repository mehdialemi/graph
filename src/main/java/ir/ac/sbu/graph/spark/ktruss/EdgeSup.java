package ir.ac.sbu.graph.spark.ktruss;

import ir.ac.sbu.graph.types.Edge;

public class EdgeSup extends Edge {
    public int sup;

    public EdgeSup(int v1, int v2, int sup) {
        super(v1, v2);
        this.sup = sup;
    }
}
