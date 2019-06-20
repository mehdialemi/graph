package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.spark.search.fonl.local.QFonl;
import ir.ac.sbu.graph.spark.search.fonl.local.QSplit;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.HashSet;
import java.util.Set;

public class LabeledFonlValueOld  extends Fvalue<LabelMeta> {
    private static Int2IntOpenHashMap tEmpty = new Int2IntOpenHashMap();

    public Int2IntOpenHashMap expands(int vertex, int splitIndex, QFonl qFonl) {

        Set<int[]> partials = allPartials(splitIndex, qFonl);

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

    public Set<int[]> allPartials(int splitIndex, QFonl qFonl) {
        QSplit qSplit = qFonl.splits[splitIndex];
        int vIndex = qSplit.vIndex;
        int[] qFonlValue = qFonl.fonl[vIndex];

        Set <int[]> resultSet = new HashSet<>();
        partial(meta.label, meta.deg, 0, qSplit, qFonl, resultSet);

        for (int offset = 0; offset < fonl.length; offset++) {
            if (fonl.length - offset < qFonlValue.length)
                break;

            int deg = meta.degs[offset];
            String label = meta.labels[offset];

            partial(label, deg, offset + 1, qSplit, qFonl, resultSet);
        }

        return resultSet;
    }

    public Set<int[]> partial(String label, int deg, int fonlOffset, QSplit qSplit, QFonl qFonl, Set <int[]> resultSet) {
        int vIndex = qSplit.vIndex;

        if (!qFonl.labels[vIndex].equals(label))
            return null;

        if (qFonl.degIndices[vIndex] > deg)
            return null;

        int[] qFonlValue = qFonl.fonl[vIndex];
        IntSet[] set = new IntSet[qFonlValue.length];

        int sIndex = 0;
        for (int qvIndex : qFonlValue) {
            // skip triangle fonl
            for (int i = fonlOffset; i < this.fonl.length; i++) {
                if (!meta.labels[i].equals(qFonl.labels[qvIndex]))
                    continue;

                if (meta.degs[i] < qFonl.degIndices[qvIndex])
                    continue;

                int qRemains = qFonlValue.length - sIndex;
                int gRemains = this.fonl.length - i;

                if (qRemains > gRemains)
                    break;

                if (set[sIndex] == null)
                    set[sIndex] = new IntOpenHashSet();

                set[sIndex].add(this.fonl[i]);
            }

            if (set[sIndex] == null)
                return null;

            sIndex ++;
        }

        int[][] selects = new int[set.length][];
        for (int k = 0; k < set.length; k++) {
            selects[k] = set[k].toIntArray();
        }

        int[] indexes = new int[qFonlValue.length];

        for (int vertexIndex = 0; vertexIndex < selects[0].length; vertexIndex++) {
            join(0, vertexIndex, indexes, selects, qFonl, qSplit, resultSet);
        }

        if (resultSet.size() == 0)
            return null;

        return resultSet;
    }

    public void join(int selectIndex, int currentVertexIndex, int[] partialMatch,
                     int[][] selects, QFonl qFonl, QSplit qSplit, Set <int[]> resultSet) {

        partialMatch[selectIndex] = this.fonl[currentVertexIndex];

        if (selectIndex >= selects.length - 1) {
            int[] pMatch = new int[partialMatch.length];
            System.arraycopy(partialMatch, 0, pMatch, 0, pMatch.length);
            resultSet.add(pMatch);
            return;
        }

        // go to the right array
        int nextIndex = selectIndex + 1;

        for (int vertexIndex = 0; vertexIndex < selects[nextIndex].length; vertexIndex++) {
            join(nextIndex, vertexIndex, partialMatch, selects, qFonl, qSplit, resultSet);
        }
    }
}