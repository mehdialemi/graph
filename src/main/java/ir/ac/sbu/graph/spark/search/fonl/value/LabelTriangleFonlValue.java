package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.spark.search.fonl.local.SubQuery;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.*;

import java.util.HashSet;
import java.util.Set;

/**
 * Represent fonl value including labels, degrees, and triangle information
 */
public class LabelTriangleFonlValue extends FonlValue<TriangleDegreeMeta> {

    public LabelTriangleFonlValue() {
    }

    public LabelTriangleFonlValue(LabelFonlValue labelFonlValue) {
        super.fonl = labelFonlValue.fonl;
        super.meta = new LabelTriangleDegreeMeta();
    }

    /**
     * Note: after this function triangle count and edges should be calculated
     */
    public void remove(IntSet removes) {
        int fonlValueSize = fonl.length - removes.size();

        if (fonlValueSize == 0) {
            fonl = new int[0];
            meta = new TriangleDegreeMeta(meta.degree, meta.label, 0);
            return;
        }

        int[] uFonl = new int[fonlValueSize];
        TriangleDegreeMeta triangleDegreeMeta = new TriangleDegreeMeta(meta.degree, meta.label, fonlValueSize);

        int index = 0;
        for (int i = 0; i < fonl.length; i++) {
            if (removes.contains(fonl[i]))
                continue;

            uFonl[index] = fonl[i];
            triangleDegreeMeta.labels[index] = meta.labels[i];
            triangleDegreeMeta.degrees[index] = meta.degrees[i];
            index++;
        }

        fonl = uFonl;
        meta = triangleDegreeMeta;
    }

    public void setEdges(Set <Edge> edges, Int2IntOpenHashMap tcMap) {
        if (!edges.isEmpty()) {
            meta.setEdges(edges);
        }

        int[] tcArray = new int[fonl.length];
        for (int i = 0; i < fonl.length; i++) {
            tcArray[i] = tcMap.get(fonl[i]);
        }
        meta.setTcArray(tcArray);
    }

    public Int2LongMap matches(int fonlKey, SubQuery subquery) {
        Int2LongOpenHashMap v2count = new Int2LongOpenHashMap();

        IntSet keySet = new IntOpenHashSet();
        IntSet[] setArray = new IntSet[subquery.fonl.length];

        // search for key candidates
        String subQueryLabel = subquery.label;
        if (meta.label.equals(subQueryLabel) && meta.degree >= subquery.degree)
            keySet.add(-1);

        for (int i = 0; i < fonl.length; i++) {
            String label = meta.labels[i];
            if (label == null) {
                System.out.println(this.toString());
            }

            if (label.equals(subQueryLabel) && meta.degrees[i] >= subquery.degree)
                if (subquery.tc == 0 || (meta.tcArray != null && meta.tcArray[i] >= subquery.tc))
                    keySet.add(i);
        }

        for (int i = 0; i < subquery.fonl.length; i++) {
            IntSet set = new IntOpenHashSet();

            if (meta.label.equals(subquery.labels[i]) && meta.degree >= subquery.degrees[i])
                set.add(-1);

            for (int j = 0; j < fonl.length; j++) {
                if (meta.labels[j].equals(subquery.labels[i]) && meta.degrees[j] >= subquery.degrees[i])
                    set.add(j);
            }
            if (set.isEmpty())
                return v2count;

            setArray[i] = set;
        }

        Set <int[]> results = new HashSet <>();
        for (int keyIndex : keySet) {
            IntOpenHashSet rSet = new IntOpenHashSet();
            rSet.add(keyIndex);
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

    private void join(int idx, IntSet indexSet, IntSet[] setArray, Set <int[]> result, SubQuery subquery) {

        if (idx == setArray.length) {
            int[] indexArray = indexSet.toIntArray();

            int keyIndex = indexArray[0];
            if (keyIndex != -1) {
                for (int i = 1; i < indexArray.length; i++) {
                    int index = indexArray[i];
                    if (index == -1)
                        continue;

                    Edge e = keyIndex < index ?
                            new Edge(fonl[keyIndex], fonl[index]) :
                            new Edge(fonl[index], fonl[keyIndex]);
                    if(meta.edges == null || !meta.edges.contains(e))
                        return;
                }
            }

            // check other connections
            if (!subquery.edges.isEmpty()) {
                if (meta.edges == null)
                    return;

                for (Edge edge : subquery.edges) {
                    int i1 = subquery.v2i.get(edge.v1);
                    int i2 = subquery.v2i.get(edge.v2);

                    int fIndex1 = indexArray[i1];
                    int fIndex2 = indexArray[i2];
                    if (fIndex1 == -1 || fIndex2 == -1)
                        continue;

                    int v1 = fonl[fIndex1];
                    int v2 = fonl[fIndex2];

                    if (!meta.edges.contains(new Edge(v1, v2)))
                        return;
                }
            }

            result.add(indexArray);
            return;
        }

        for (int index : setArray[idx]) {
            if (indexSet.contains(index))
                continue;

            IntOpenHashSet rSet = new IntOpenHashSet(indexSet);
            rSet.add(index);

            join(idx + 1, rSet, setArray, result, subquery);
        }
    }
}

