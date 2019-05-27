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

        if (qFonl.degIndices[vIndex] > meta.deg)
            return null;

        int[] qFonlValue = qFonl.fonl[vIndex];
        IntSet[] set = new IntSet[qFonlValue.length];

        int sIndex = 0;
        for (int qvIndex : qFonlValue) {
            for (int i = 0; i < this.fonl.length; i++) {
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

        int[] indexes = new int[split.fonlIndex.length];
        Set <int[]> resultSet = new HashSet <>();
        for (int vertexIndex = 0; vertexIndex < selects[0].length; vertexIndex++) {
            join(0, vertexIndex, split.fonlIndex[0], indexes, selects, qFonl, split, resultSet);
        }

        if (resultSet.size() == 0)
            return null;

        return resultSet;
    }

    public void join(int selectIndex, int currentVertexIndex, int pIndex, int[] partialMatch,
                     int[][] selects, QFonl qFonl, QSplit qSplit, Set <int[]> resultSet) {

        int[] qFonlValue = qFonl.fonl[qSplit.vIndex];
        if (qFonlValue[pIndex] == selectIndex) {
            partialMatch[pIndex] = currentVertexIndex;
            pIndex ++;
        }

        if (selectIndex >= selects.length - 1) {
            int[] pMatch = new int[partialMatch.length];
            System.arraycopy(partialMatch, 0, pMatch, 0, pMatch.length);
            resultSet.add(pMatch);
            return;
        }

        // go to the right array
        int nextIndex = selectIndex + 1;

        boolean verifyEdge = qFonl
                .edgeArray[qSplit.vIndex]
                .getOrDefault(qFonlValue[selectIndex], new IntOpenHashSet())
                .contains(qFonlValue[nextIndex]);

        int id1 = selects[selectIndex][currentVertexIndex];
        for (int vertexIndex = 0; vertexIndex < selects[nextIndex].length; vertexIndex++) {
            int id2 = selects[nextIndex][vertexIndex];

            if (verifyEdge && !hasEdge(id1, id2))
                continue;

            join(nextIndex, vertexIndex, pIndex, partialMatch, selects, qFonl, qSplit, resultSet);
        }
    }

    public boolean hasEdge(int v1, int v2) {
        return meta.edges.contains(new Edge(v1, v2));
    }
}

