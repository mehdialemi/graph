package ir.ac.sbu.graph.spark.pattern.index.fonl.value;

import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class LabelDegreeTriangleFonlValue extends FonlValue <LabelDegreeTriangleMeta> {

    private int source;

    public LabelDegreeTriangleFonlValue() {  }

    public LabelDegreeTriangleFonlValue(int source, int[] fonl, LabelDegreeTriangleMeta meta) {
        this.source = source;
        this.fonl = fonl;
        this.meta = meta;
    }

    public LabelDegreeTriangleFonlValue setSource(int source) {
        this.source = source;
        return this;
    }

    /**
     * The set of all matchIndices of this fonl for the given subquery
     * @param subquery is the subquery to be find its matchIndices in the current fonl
     * @return a set of matchIndices. each match is an array of integer which
     *                  index 0 of match is for key of the subquery
     *                  and index 1 of match is for the index 0 of subquery.fonlValue
     *                  and so on
     */
    public Set<int[]> matchIndices(Subquery subquery) {

        // Per sub-query fonl find the potential candidates using label, degree, tc
        // Vertices are sorted by their degree accordingly.
        // Candidate vertices should have higher degree than that of sub-query

        if (fonl.length < subquery.fonlValue.length || meta.maxDegree() < subquery.maxDegree())
            return null;

        // Find candidates for the key related properties of sub-query
        IntList candidates = candidates(subquery.label, subquery.degree, subquery.tc);

        // If no candidate found for the key then return without result
        if (candidates == null)
            return null;

        IntList[] cIndices = new IntList[subquery.fonlValue.length + 1];
        cIndices[0] = candidates;

        // Iterate over vertices of sub-query and find all of the candidates per one separately.
        for (int i = 0; i < subquery.fonlValue.length; i++) {
            candidates = candidates(subquery.labels[i], subquery.degrees[i], subquery.vTc[i]);

            // if no candidate found for index i of sub query fonl value then return without result
            if (candidates == null)
                return null;
            cIndices[i + 1] = candidates;
        }

        int[][] selects = new int[cIndices.length][];
        for (int k = 0; k < selects.length; k++) {
            selects[k] = cIndices[k].toIntArray();
        }

        // This matchIndex will be filled at the end of recursive. It contains the value of the fonls which are matched
        // index 0 of matchIndex is for key of the subquery
        // and index 1 of matchIndex is for the index 0 of subquery.fonlValue and so on
        int[] matchIndex = new int[selects.length];
        Set <int[]> resultSet = new HashSet<>();

        IntSet set = new IntOpenHashSet();
        // Start with the subquery key
        for (int vertexIndex = 0; vertexIndex < selects[0].length; vertexIndex++) {
            join(0, vertexIndex, matchIndex, selects, resultSet, set);
        }

        // Check the connectivity (edges, triangles) to find the true matchIndices
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
     * @param resultSet all of the completed matchIndices
     */
    private void join(int selectIndex, int currentVertexIndex, int[] partialMatch,
                      int[][] selects, Set<int[]> resultSet, IntSet set) {
        int matchIndex = selects[selectIndex][currentVertexIndex];
        if (set.contains(matchIndex))
            return;

        partialMatch[selectIndex] = matchIndex;
        set.add(matchIndex);

        if (selectIndex == selects.length - 1) {
            int[] pMatch = new int[partialMatch.length];
            System.arraycopy(partialMatch, 0, pMatch, 0, pMatch.length);
            resultSet.add(pMatch);
            return;
        }

        // go to the right index array
        int nextIndex = selectIndex + 1;
        set.add(matchIndex);
        for (int vertexIndex = 0; vertexIndex < selects[nextIndex].length; vertexIndex++) {
            join(nextIndex, vertexIndex, partialMatch, selects, resultSet, set);
        }
        set.remove(matchIndex);
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

        for (int index = 0; index < fonl.length; index++) {
            if (meta.getLabels()[index].equals(vqLabel) && meta.getDegrees()[index] >= vqDegree && meta.tc(index) >= vqTc) {
                if (list == null)
                    list = new IntArrayList();
                list.add(index);
            }
        }

        return list;
    }

    public int getSource() {
        return source;
    }

    @Override
    public String toString() {
        return "LabelDegreeTriangleFonlValue(source: " + source + ", meta: " + meta + ", fonl: " + Arrays.toString(fonl);
    }
}
