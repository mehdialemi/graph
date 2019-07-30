package ir.ac.sbu.graph.spark.search.fonl.value;

import ir.ac.sbu.graph.spark.search.fonl.local.Subquery;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public class LabelDegreeTriangleFonlValue extends FonlValue <LabelDegreeTriangleMeta> {

    public LabelDegreeTriangleFonlValue() {
    }

    public LabelDegreeTriangleFonlValue(int[] fonl, LabelDegreeTriangleMeta meta) {
        this.fonl = fonl;
        this.meta = meta;
    }

    public Int2LongMap matches(Subquery subquery) {

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

        // Check connectivity (edges, triangles) to find the true matches


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
