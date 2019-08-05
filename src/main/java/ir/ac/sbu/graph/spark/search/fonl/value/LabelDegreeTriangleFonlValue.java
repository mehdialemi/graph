package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.spark.search.patterns.SubQuery;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import scala.Tuple2;

import java.util.HashSet;
import java.util.Set;

public class LabelDegreeTriangleFonlValue extends FonlValue <LabelDegreeTriangleMeta> {

    public LabelDegreeTriangleFonlValue() {  }

    public LabelDegreeTriangleFonlValue(int[] fonl, LabelDegreeTriangleMeta meta) {
        this.fonl = fonl;
        this.meta = meta;
    }

    public Set<Tuple2<String, Integer>> matches(int key, SubQuery subquery) {
        Set<int[]> result = matchesAll(key, subquery);
        if (result == null)
            return null;

        Set<Tuple2<String, Integer>> out = new HashSet<>();
        for (int[] matches : result) {
            StringBuilder sb = new StringBuilder();
            for (int match : matches) {
                sb.append(match).append(" ");
            }
            String matchStr = sb.toString();
            for (int anchorIndex : subquery.anchors) {
                int vertex = matches[anchorIndex];
                if (vertex == key) {
                    out.add(new Tuple2<>(matchStr, key));
                } else {
                    out.add(new Tuple2<>(matchStr, vertex));
                }
            }
        }
        return out;
    }

    private Set<int[]> matchesAll(int key, SubQuery subquery) {

        // Per sub-query fonl find the potential candidates using label, degree, tc
        // Vertices are sorted by their degree ascendingly.
        // Candidate vertices should have higher degree than that of sub-query

        if (fonl.length < subquery.fonl.length || meta.maxDegree() < subquery.maxDegree())
            return null;

        // Find candidates for the key related properties of sub-query
        IntList candidates = candidates(subquery.label, subquery.degree, subquery.tc);
        if (candidates == null)
            return null;

        IntList[] cIndices = new IntList[subquery.fonl.length + 1];
        cIndices[0] = candidates;

        // Iterate over vertices of sub-query and find all of the candidates per one separately.
        for (int i = 0; i < subquery.fonl.length; i++) {
            candidates = candidates(subquery.labels[i], subquery.degrees[i], subquery.vTc[i]);
            if (candidates == null)
                return null;
            cIndices[i + 1] = candidates;
        }

        int[][] selects = new int[cIndices.length][];
        for (int k = 0; k < selects.length; k++) {
            selects[k] = cIndices[k].toIntArray();
        }

        int[] indexes = new int[subquery.fonl.length];
        Set <int[]> resultSet = new HashSet<>();
        for (int vertexIndex = 0; vertexIndex < selects[0].length; vertexIndex++) {
            join(0, vertexIndex, indexes, selects, resultSet, key);
        }

        // Check the connectivity (edges, triangles) to find the true matches
        if (resultSet.size() == 0)
            return null;

        return resultSet;
    }

    /**
     * Generate all combination of selected vertices to fill sub-query match.
     * @param selectIndex the index of current subquery array
     * @param currentVertexIndex the index of current vertex of fonl
     * @param partialMatch the selected indices of the current vertex fonl are maintained here
     * @param selects all the selected indices from the fonl per subquery index
     * @param resultSet all of the completed matches
     */
    private void join(int selectIndex, int currentVertexIndex, int[] partialMatch,
                      int[][] selects, Set<int[]> resultSet, int key) {

        if (selectIndex == selects.length - 1) {
            int[] pMatch = new int[partialMatch.length];
            System.arraycopy(partialMatch, 0, pMatch, 0, pMatch.length);
            resultSet.add(pMatch);
            return;
        }
        int value = currentVertexIndex == -1 ? key : fonl[currentVertexIndex];
        for (int i = 0; i < selectIndex; i ++) {
            if (this.fonl[i] == value)
                return;
        }
        partialMatch[selectIndex] = value;

        // go to the right index array
        int nextIndex = selectIndex + 1;
        for (int vertexIndex = 0; vertexIndex < selects[nextIndex].length; vertexIndex++) {
            join(nextIndex, vertexIndex, partialMatch, selects, resultSet, key);
        }
    }

    /**
     * Here vq prefix means vertex query
     *
     * @param vqLabel  given vertex label of the sub-query
     * @param vqDegree given vertex degree of the sub-query
     * @param vqTc     triangle count for the given vertex of the sub-query
     * @return an IntList including the index of the potential candidates
     * which have been passed the exclusion filters successfully.
     */
    public IntList candidates(String vqLabel, int vqDegree, int vqTc) {
        IntList list = null;

        // check for key
        if (meta.getLabel().equals(vqLabel) && meta.degree >= vqDegree && meta.tc() >= vqTc) {
            list = new IntArrayList();
            list.add(-1); // -1 means index of fonl key
        }

        for (int i = 0; i < fonl.length; i++) {
            if (meta.getLabels()[i].equals(vqLabel) && meta.getDegrees()[i] >= vqDegree && meta.tc(i) >= vqTc) {
                if (list == null)
                    list = new IntArrayList();
                list.add(i);
            }
        }

        return list;
    }
}
