package ir.ac.sbu.graph.fonl.matcher;

import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.fonl.TriangleMeta;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.HashSet;
import java.util.Set;

public class TFonlValue extends Fvalue <TriangleMeta> {

    private static int[] EMPTY = new int[0];

    public int[] expands(int splitIndex, QFonl qFonl) {

        Set <int[]> partials = partialMatches(splitIndex, qFonl);

        if (partials == null)
            return EMPTY;

        IntSet intSet = new IntOpenHashSet();
        for (int[] partialMatch : partials) {
            for (int match : partialMatch) {
                intSet.add(match);
            }
        }

        return intSet.toIntArray();
    }

    /**
     * @return -1 if no valid subgraph is found, otherwise return
     */
    public Set <int[]> partialMatches(int splitIndex, QFonl qFonl) {
        QSplit split = qFonl.splits[splitIndex];
        int vIndex = split.vIndex;

        if (!qFonl.labels[vIndex].equals(meta.label))
            return null;

        IntSet[] set = new IntSet[split.fonlIndex.length];

        int c = 0;
        int[] qvFonl = qFonl.fonl[vIndex];
        for (int fonlIndex : split.fonlIndex) {
            int qvIndex = qvFonl[fonlIndex];

            for (int i = 0; i < this.fonl.length; i++) {
                if (!meta.labels[i].equals(qFonl.labels[qvIndex]))
                    continue;

                if (meta.degs[i] < qFonl.degIndices[qvIndex])
                    continue;

                int qRemains = qvFonl.length - fonlIndex;
                int gRemains = this.fonl.length - i;

                if (qRemains > gRemains)
                    break;

                if (set[c] == null)
                    set[c] = new IntOpenHashSet();

                set[c].add(this.fonl[i]);
            }

            if (set[c] == null)
                return null;

            c++;
        }

        int[][] selects = new int[set.length][];
        for (int k = 0; k < selects.length; k++) {
            selects[k] = set[k].toIntArray();
        }

        int[] indexes = new int[selects.length];
        Set <int[]> resultSet = new HashSet <>();
        for (int vertexIndex = 0; vertexIndex < selects[0].length; vertexIndex++) {
            join(0, vertexIndex, indexes, selects, qFonl, split, resultSet);
        }

        if (resultSet.size() == 0)
            return null;

        return resultSet;
    }

    public void join(int selectIndex, int currentVertexIndex, int[] partialMatch, int[][] selects,
                     QFonl qFonl, QSplit qSplit, Set <int[]> resultSet) {
        partialMatch[selectIndex] = currentVertexIndex;

        if (selectIndex == selects.length - 1) {
            int[] pMatch = new int[partialMatch.length];
            System.arraycopy(partialMatch, 0, pMatch, 0, pMatch.length);
            resultSet.add(pMatch);
            return;
        }

        // go to the right array
        int nextIndex = selectIndex + 1;

        int index = qSplit.fonlIndex[selectIndex];
        int qVertexIdx1 = qFonl.fonl[qSplit.vIndex][index];

        index = qSplit.fonlIndex[nextIndex];
        int qVertexIdx2 = qFonl.fonl[qSplit.vIndex][index];

        boolean verifyEdge = qFonl
                .edgeArray[qSplit.vIndex]
                .getOrDefault(qVertexIdx1, new IntOpenHashSet())
                .contains(qVertexIdx2);

        int id1 = selects[selectIndex][currentVertexIndex];
        for (int vertexIndex = 0; vertexIndex < selects[nextIndex].length; vertexIndex++) {
            int id2 = selects[nextIndex][vertexIndex];

            if (verifyEdge && !hasEdge(id1, id2))
                continue;

            join(nextIndex, vertexIndex, partialMatch, selects, qFonl, qSplit, resultSet);
        }
    }

    public boolean hasEdge(int v1, int v2) {
        return meta.edges.contains(new Edge(v1, v2));
    }
}

