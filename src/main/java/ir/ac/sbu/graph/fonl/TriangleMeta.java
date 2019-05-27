package ir.ac.sbu.graph.fonl;

import ir.ac.sbu.graph.spark.search.fonl.LabelMeta;
import ir.ac.sbu.graph.types.Edge;

import java.util.HashSet;
import java.util.Set;

public class TriangleMeta extends LabelMeta {
    public Set<Edge> edges = new HashSet <>();

    public TriangleMeta(LabelMeta labelMeta) {
        super(labelMeta);
    }
}
