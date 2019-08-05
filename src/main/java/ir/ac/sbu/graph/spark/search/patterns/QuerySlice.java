package ir.ac.sbu.graph.spark.search.patterns;

import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QuerySlice {
    private final int v;
    private final int[] neighbors;
    private final List<Triangle> triangles;
    private Map<Integer, String> labelMap;
    private Int2IntMap vDegs;
    private QuerySlice parent = null;
    private boolean processed = false;
    private List<QuerySlice> children = new ArrayList<>();

    public QuerySlice(int v, int[] neighbors, List<Triangle> triangles, Map<Integer, String> labelMap, Int2IntMap vDegs) {
        this.v = v;
        this.neighbors = neighbors;
        this.triangles = triangles;
        this.labelMap = labelMap;
        this.vDegs = vDegs;
    }

    public int getV() {
        return this.v;
    }

    public void setParent(QuerySlice parent) {
        this.parent = parent;
    }

    public QuerySlice getParent() {
        return this.parent;
    }

    public void addChild(QuerySlice querySlice) {
        children.add(querySlice);
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }
}
