package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.spark.search.fonl.local.Subquery;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TriangleFonlValue extends Fvalue <TriangleMeta> {

    public TriangleFonlValue() {
    }

    public TriangleFonlValue(LabledFonlValue labledFonlValue) {
        super.fonl = labledFonlValue.fonl;
        super.meta = new TriangleMeta(labledFonlValue.meta);
    }

    public void remove(IntSet removes) {
        int fonlValueSize = fonl.length - removes.size();

        if (fonlValueSize == 0) {
            fonl = new int[0];
            meta = new TriangleMeta(meta.deg, meta.label, 0);
            return;
        }

        int[] uFonl = new int[fonlValueSize];
        TriangleMeta triangleMeta = new TriangleMeta(meta.deg, meta.label, fonlValueSize);
        triangleMeta.v2n = meta.v2n;

        int index = 0;
        for (int i = 0; i < fonl.length; i++) {
            if (removes.contains(fonl[i]))
                continue;

            uFonl[index] = fonl[i];
            triangleMeta.labels[index] = meta.labels[i];
            triangleMeta.degs[index] = meta.degs[i];
            index ++;
        }

        fonl = uFonl;
        meta = triangleMeta;
    }

    public void addNeighborEdge(Edge edge) {
        meta.addV2V(edge.v1, edge.v2);
//        meta.addV2V(edge.v2, edge.v1);
    }

    public Int2IntMap matches(int fonlKey, Subquery subquery) {
        Int2IntOpenHashMap v2count = new Int2IntOpenHashMap();

        IntSet keySet = new IntOpenHashSet();
        IntSet[] setArray = new IntSet[subquery.fonl.length];

        // search for key candidates
        if (meta.label.equals(subquery.label) && meta.deg >= subquery.degree)
            keySet.add(-1);

        for (int j = 0; j < fonl.length; j++) {
            if (meta.labels[j].equals(subquery.label) && meta.degs[j] >= subquery.degree)
                keySet.add(j);
        }

        for (int i = 0; i < subquery.fonl.length; i++) {
            IntSet set = new IntOpenHashSet();

            if (meta.label.equals(subquery.labels[i]) && meta.deg >= subquery.degrees[i])
                set.add(-1);

            for (int j = 0; j < fonl.length; j++) {
                if (meta.labels[j].equals(subquery.labels[i]) && meta.degs[j] >= subquery.degrees[i])
                    set.add(j);
            }
            if (set.isEmpty())
                return v2count;

            setArray[i] = set;
        }

        Set<int[]> results = new HashSet <>();
        for (Integer key : keySet) {
            IntOpenHashSet rSet = new IntOpenHashSet();
            rSet.add(key);
            join(0, rSet, setArray, results, subquery);
        }

        if (results.isEmpty())
            return v2count;

        for (int[] result : results) {
            for (int index : result) {
                if (index == -1) {
                    v2count.addTo(fonlKey, 1);
                } else {
                    v2count.addTo(fonl[index], 1);
                }
            }
        }

        return v2count;
    }

    private void join(int idx, IntSet iSet, IntSet[] setArray, Set<int[]> result, Subquery subquery) {

        if (idx == setArray.length) {

            int[] c = iSet.toIntArray();
            for (Map.Entry <Integer, IntSet> entry : subquery.vi2List.entrySet()) {
                int vi = entry.getKey();
                int index1 = subquery.v2i.get(vi);
                int v1 = fonl[index1];

                if (v1 == -1)
                    continue;

                IntSet v1Connections = null;

                for (Integer v2i : entry.getValue()) {
                    int index2 = subquery.v2i.get(v2i);

                    int v2 = fonl[index2];
                    if (v2 == -1)
                        continue;

                    if (v1Connections == null)
                        v1Connections = meta.v2n.getOrDefault(v1, new IntOpenHashSet());

                    if (!v1Connections.contains(v2))
                        return;
                }
            }
            for (int i : c) {

            }
            result.add(iSet.toIntArray());
            return;
        }

        for (int index : setArray[idx]) {
            if (iSet.contains(index))
                continue;


            IntOpenHashSet rSet = new IntOpenHashSet(iSet);
            rSet.add(index);

            join(idx + 1, rSet, setArray, result, subquery);
        }
    }
}

