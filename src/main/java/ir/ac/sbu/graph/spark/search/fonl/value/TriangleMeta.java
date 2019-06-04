package ir.ac.sbu.graph.spark.search.fonl.value;

import it.unimi.dsi.fastutil.ints.*;

public class TriangleMeta extends LabelMeta {

    public Int2ObjectMap<IntSet> v2n; // for triangle (u, v, w) and key = u, stores v -> (..., w, ...)

    public TriangleMeta() {}

    public TriangleMeta(int deg, String label, int fonlValueSize) {
        super(deg, label, fonlValueSize);
    }

    public TriangleMeta(LabelMeta labelMeta) {
        super(labelMeta);
    }

    public void addV2V(int v1, int v2) {
        if (v2n == null)
            v2n = new Int2ObjectOpenHashMap <>();

        IntSet intSet = v2n.get(v1);
        if (intSet == null) {
            intSet = new IntOpenHashSet();
            v2n.put(v1, intSet);
        }
        intSet.add(v2);
    }
}
