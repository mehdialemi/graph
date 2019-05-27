package ir.ac.sbu.graph.fonl.matcher;

import ir.ac.sbu.graph.fonl.LocalFonl;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

}
