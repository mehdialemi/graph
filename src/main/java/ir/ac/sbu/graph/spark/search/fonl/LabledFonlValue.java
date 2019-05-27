package ir.ac.sbu.graph.spark.search.fonl;

import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.spark.search.VLabelDeg;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LabledFonlValue extends Fvalue<LabelMeta> {
    private static Int2IntOpenHashMap tEmpty = new Int2IntOpenHashMap();

    public LabledFonlValue() {}

    public LabledFonlValue(int degree, List<VLabelDeg> list) {
        meta = new LabelMeta(degree, list.size());
        fonl = new int[list.size()];
        meta.labels = new String[list.size()];

        for (int i = 0; i < list.size(); i++) {
            fonl[i] = list.get(i).vertex;
            meta.degs[i] = list.get(i).degree;
            meta.labels[i] = list.get(i).label;
        }

        ifonl = new int[fonl.length];
        System.arraycopy(fonl, 0, ifonl, 0, ifonl.length);
        Arrays.sort(ifonl);
    }

    public Int2IntOpenHashMap matchSubquery(int vertex, Subquery subquery) {

        Set<int[]> partials = matchAllFonls(subquery);

        if (partials == null || partials.isEmpty())
            return tEmpty;

        Int2IntOpenHashMap counter = new Int2IntOpenHashMap();
        for (int[] partialMatch : partials) {
            for (int match : partialMatch) {
                counter.addTo(match, 1);
            }
        }
        counter.addTo(vertex, partials.size());

        return counter;
    }

    public Set<int[]> matchAllFonls(Subquery subquery) {
        Set <int[]> resultSet = new HashSet<>();
        matchPartial(meta.label, meta.deg, 0, subquery, resultSet);

        for (int offset = 0; offset < fonl.length; offset++) {
            if (fonl.length - offset < subquery.fonlValue.length)
                break;

            int deg = meta.degs[offset];
            String label = meta.labels[offset];

            matchPartial(label, deg, offset + 1, subquery, resultSet);
        }

        return resultSet;
    }

    public Set<int[]> matchPartial(String label, int deg, int fonlOffset, Subquery subquery, Set <int[]> resultSet) {
        if (!subquery.label.equals(label))
            return null;

        if (subquery.degree > deg)
            return null;

        IntSet[] set = new IntSet[subquery.fonlValue.length];

        for (int i = 0; i < subquery.fonlValue.length; i++) {
            String qvLabel = subquery.labels[i];
            int qvDegree = subquery.degrees[i];

            for (int j = fonlOffset; j < this.fonl.length; j++) {
                if (!meta.labels[j].equals(qvLabel))
                    continue;

                if (meta.degs[j] < qvDegree)
                    continue;

                int querySize = subquery.fonlValue.length - i;
                int graphSize = this.fonl.length - j;
                if (querySize> graphSize)
                    break;

                if (set[i] == null)
                    set[i] = new IntOpenHashSet();
                set[i].add(this.fonl[j]);
            }

            if (set[i] == null)
                return null;
        }

        int[][] selects = new int[set.length][];
        for (int k = 0; k < set.length; k++) {
            selects[k] = set[k].toIntArray();
        }

        int[] indexes = new int[subquery.fonlValue.length];

        for (int vertexIndex = 0; vertexIndex < selects[0].length; vertexIndex++) {
            joinCurrentMatches(0, vertexIndex, indexes, selects, resultSet);
        }

        if (resultSet.size() == 0)
            return null;

        return resultSet;
    }

    public void joinCurrentMatches(int selectIndex, int currentVertexIndex, int[] partialMatch,
                     int[][] selects, Set <int[]> resultSet) {

        partialMatch[selectIndex] = this.fonl[currentVertexIndex];

        if (selectIndex >= selects.length - 1) {
            int[] pMatch = new int[partialMatch.length];
            System.arraycopy(partialMatch, 0, pMatch, 0, pMatch.length);
            resultSet.add(pMatch);
            return;
        }

        // go to the right index array
        int nextIndex = selectIndex + 1;
        for (int vertexIndex = 0; vertexIndex < selects[nextIndex].length; vertexIndex++) {
            joinCurrentMatches(nextIndex, vertexIndex, partialMatch, selects, resultSet);
        }
    }


}
