package ir.ac.sbu.graph.spark.pattern.index.fonl.value;

import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongAVLTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;

import java.util.Arrays;
import java.util.Map;

public class TriangleMeta extends Meta {

    private long[] triangleEdges;
    private int[] vTc;
    private int tc;

    TriangleMeta() { }

    TriangleMeta(TriangleMeta triangleMeta) {
        super(triangleMeta);
        triangleEdges = triangleMeta.triangleEdges;
        tc = triangleMeta.tc;
        vTc = triangleMeta.vTc;
    }

    TriangleMeta(int degree, int[] fonl, Iterable<Edge> triangleEdges) {
        super(degree);

        Int2IntMap v2Index = new Int2IntOpenHashMap();
        for (int i = 0; i < fonl.length; i++) {
            v2Index.put(fonl[i], i);
        }

        LongSortedSet edgeSet = new LongAVLTreeSet();
        Int2IntOpenHashMap tcMap = new Int2IntOpenHashMap();
        for (Edge edge : triangleEdges) {
            edgeSet.add((long)edge.v1 << 32 | edge.v2 & 0xFFFFFFFFL);
            tcMap.addTo(edge.v1, 1);
            tcMap.addTo(edge.v2, 1);
            tc ++;
        }

        this.triangleEdges = edgeSet.toLongArray();
        vTc = new int[fonl.length];
        for (Map.Entry <Integer, Integer> entry : tcMap.entrySet()) {
            int vertex = entry.getKey();
            int count = entry.getValue();
            int index = v2Index.get(vertex);
            vTc[index] = count;
        }
    }

    public int tc(int index) {
        return vTc[index];
    }

    public int tc() { return tc; }

    boolean hasEdge(Edge edge) {
        return hasEdge(edge.v1, edge.v2);
    }

    boolean hasEdge(int v1, int v2) {
        long e = (long)v1 << 32 | v2 & 0xFFFFFFFFL;
        return Arrays.binarySearch(triangleEdges, e) >= 0;
    }

    boolean hasTriangle() {
        return triangleEdges != null;
    }

    @Override
    public String toString() {
        return super.toString() + " TriangleMeta(tc: " + tc + ", triangleEdges: " + Arrays.toString(triangleEdges) +
                ", vTc" + Arrays.toString(vTc);
    }
}
