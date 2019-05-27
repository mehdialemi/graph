package ir.ac.sbu.graph.fonl.matcher;

import ir.ac.sbu.graph.fonl.LocalFonl;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Arrays;

public class QFonl extends LocalFonl {
    // Per fonl vertex an index exist
    // Per index a map from i=Int to iSet=Set
    // i is index of fonl (determines fonl[i] as a vertex)
    // iSet is a set of fonl indices (determines neighbors of fonl[i])
    public Int2ObjectMap<IntSet>[] edgeArray;
    public QSplit[] splits;

    public QFonl() { }

    public QFonl(LocalFonl localFonl) {
        this.degIndices = localFonl.degIndices;
        this.vIndices = localFonl.vIndices;
        this.labels = localFonl.labels;
        this.fonl = localFonl.fonl;
        this.edgeArray = new Int2ObjectMap[vIndices.length];
        for (int i = 0; i < this.edgeArray.length; i++) {
            edgeArray[i] = new Int2ObjectOpenHashMap <>();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("vIndices: ").append(Arrays.toString(vIndices)).append("\n");
        sb.append("degIndices: ").append(Arrays.toString(degIndices)).append("\n");
        sb.append("labels: ").append(Arrays.toString(labels)).append("\n");
        sb.append("fonl: ").append("\n");
        for (int[] f : fonl) {
            sb.append(Arrays.toString(f)).append("\n");
        }
        sb.append("splits: ").append("\n");
        for (QSplit split : splits) {
            sb.append(split).append("\n");
        }
        return sb.toString();
    }
}
