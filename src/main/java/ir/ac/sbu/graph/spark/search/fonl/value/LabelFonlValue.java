package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.spark.search.fonl.creator.VLabelDeg;
import ir.ac.sbu.graph.spark.search.patterns.SubQuery;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LabelFonlValue extends FonlValue<LabelMeta> {
    private static Int2IntOpenHashMap tEmpty = new Int2IntOpenHashMap();

    public LabelFonlValue() {}

    public LabelFonlValue(int degree, List<VLabelDeg> list) {
        meta = new LabelMeta(degree, list.size());
        fonl = new int[list.size()];
        meta.labels = new String[list.size()];

        for (int i = 0; i < list.size(); i++) {
            fonl[i] = list.get(i).vertex;
            meta.degrees[i] = list.get(i).degree;
            meta.labels[i] = list.get(i).label;
        }
    }

    public Int2IntOpenHashMap matchSubquery(int vertex, SubQuery subquery) {

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

    private Set<int[]> matchAllFonls(SubQuery subquery) {
        Set <int[]> resultSet = new HashSet<>();
        Set <int[]> result = matchPartial(meta.label, meta.degree, 0, subquery);
        if (result != null)
            resultSet.addAll(result);

        for (int offset = 0; offset < fonl.length - subquery.fonl.length; offset++) {
            int deg = meta.degrees[offset];
            String label = meta.labels[offset];

            result = matchPartial(label, deg, offset + 1, subquery);
            if (result != null)
                resultSet.addAll(result);
        }

        return resultSet;
    }

    private Set<int[]> matchPartial(String label, int deg, int fonlOffset, SubQuery subquery) {
        if (!subquery.label.equals(label))
            return null;

        if (subquery.degree > deg)
            return null;

        IntSet[] set = new IntSet[subquery.fonl.length];

        for (int i = 0; i < subquery.fonl.length; i++) {
            String qvLabel = subquery.labels[i];
            int qvDegree = subquery.degrees[i];

            int max = this.fonl.length - subquery.fonl.length + i + 1;
            for (int j = fonlOffset; j < max; j++) {
                if (!meta.labels[j].equals(qvLabel))
                    continue;

                if (meta.degrees[j] < qvDegree)
                    continue;

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

        int[] indexes = new int[subquery.fonl.length];

        Set <int[]> resultSet = new HashSet <>();
        for (int vertexIndex = 0; vertexIndex < selects[0].length; vertexIndex++) {
            joinCurrentMatches(0, vertexIndex, indexes, selects, resultSet);
        }

        if (resultSet.size() == 0)
            return null;

        return resultSet;
    }

    private void joinCurrentMatches(int selectIndex, int currentVertexIndex, int[] partialMatch,
                     int[][] selects, Set <int[]> resultSet) {

        partialMatch[selectIndex] = this.fonl[currentVertexIndex];

        if (selectIndex == selects.length - 1) {
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
