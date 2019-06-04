package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.spark.search.fonl.local.Subquery;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.*;

import java.util.HashSet;
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
    }

    public Int2IntMap matchCount(int fonlKey, Subquery subquery) {
        Set <int[]> results = new HashSet <>();

        // First check => using current fonl.key as subquery.key
        Set <int[]> rSet = matchOffset(meta.deg, meta.label, 0, subquery);
        if (rSet != null)
            results.addAll(rSet);

        for (int start = 0; start < fonl.length - subquery.fonl.length; start++) {
            int deg = meta.degs[start];
            String label = meta.labels[start];

            rSet = matchOffset(deg, label, start + 1, subquery);
            if (rSet != null)
                results.addAll(rSet);
        }

        Int2IntMap v2count = new Int2IntOpenHashMap();

        if (!results.isEmpty())
            ((Int2IntOpenHashMap) v2count).addTo(fonlKey, results.size());

        for (int[] indexes : results) {
            for (int index : indexes) {
                ((Int2IntOpenHashMap) v2count).addTo(fonl[index], 1);
            }
        }

        return v2count;
    }

    private Set <int[]> matchOffset(int deg, String label, int start, Subquery subquery) {
        if (!label.equals(subquery.label) || deg < subquery.degree)
            return null;

        Set <int[]> result = new HashSet <>();

        IntList indexes = new IntArrayList();
        int len = fonl.length - subquery.fonl.length;
        for (int offset = start; offset <= len; offset++) {
            indexes.clear();

            if (!meta.labels[offset].equals(subquery.labels[0]) || meta.degs[offset] < subquery.degrees[0])
                continue;

            indexes.add(offset);
            if (subquery.fonl.length == 1) {
                result.add(indexes.toIntArray());
                indexes.clear();
                continue;
            }

            int pointer = 1;
            int index = offset + 1;
            while (pointer > 0 && indexes.size() > 0 && len >= (index - pointer)) {

                int matchIdx = matchNext(subquery, pointer, index);

                if (matchIdx == -1) {
                    // No neighbor in the fonl found for the current index
                    int lastIdx = indexes.size() - 1;
                    index = indexes.get(lastIdx);
                    indexes.removeElements(lastIdx, lastIdx + 1);
                    pointer --;
                } else {
                    indexes.add(matchIdx);
                    if (indexes.size() == subquery.fonl.length) {
                        if (validateEdge(subquery, indexes)) {
                            result.add(indexes.toIntArray());
                        }

                        if (matchIdx < fonl.length) {
                            indexes.removeInt(matchIdx);
                        } else {
                            break;
                        }

                    } else {
                        pointer ++;
                        index = matchIdx + 1;
                    }

                }

            }
        }

        return result;
    }

    int matchNext(Subquery subquery, int pointer, int startIndex) {
        int remain = subquery.fonl.length - pointer;
        int max = fonl.length - (remain + startIndex);
        for (int index = startIndex; index < max; index++) {
            String label1 = meta.labels[index];
            int deg1 = meta.degs[index];

            String label2 = subquery.labels[pointer];
            int deg2 = subquery.degrees[pointer];

            if (label1.equals(label2) && deg1 >= deg2)
                return index;
        }
        return -1;
    }

    boolean validateEdge(Subquery subquery, IntList indexes) {

        // check for connectivity
        for (int index = 0; index < indexes.size(); index++) {
            int idx = indexes.getInt(index);
            int v1 = fonl[idx];

            int sv1 = subquery.fonl[index];
            IntSet set = subquery.vi2List.get(sv1);
            if (set == null)
                continue;

            IntIterator iterator = set.iterator();
            while (iterator.hasNext()) {

                int sv2 = iterator.nextInt();
                int idx2 = subquery.v2i.get(sv2);
                int v2 = fonl[idx2];

                IntSet neighbors = meta.v2n.get(v1);
                if (neighbors == null || !neighbors.contains(v2)) {
                    return false;
                }
            }
        }

        return true;
    }
}

