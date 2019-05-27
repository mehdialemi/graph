package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.fonl.matcher.LabelMeta;
import ir.ac.sbu.graph.fonl.matcher.Sonl;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CandidGen {

    private final int vertex;
    private final Fvalue <LabelMeta> fvalue;
    private final Sonl query;

    public CandidGen(int vertex, Fvalue <LabelMeta> fvalue, Sonl query) {

        this.vertex = vertex;
        this.fvalue = fvalue;
        this.query = query;
    }

    public Map <Integer, Set <Candidate>> newCandidates() {
        return newCandidates(new Candidate(vertex, query.size()), true);
    }

    public Map <Integer, Set <Candidate>> newCandidates(Candidate candidate) {
        return newCandidates(candidate, false);
    }

    public Map <Integer, Set <Candidate>> newCandidates(Candidate candidate, boolean includeSelf) {
        int vDeg = fvalue.meta.deg;
        String label = fvalue.meta.label;

        IntList list = null;
        if (includeSelf) {
            for (int i = 0; i < query.dArray.length; i++) {
                if (query.dArray[i] > vDeg)
                    break;
                if (candidate.isNotEmpty(i))
                    continue;
                if (query.label(i).equals(label)) {
                    if (list == null)
                        list = new IntArrayList();
                    list.add(i);
                }
            }

            if (list == null)
                return null;
        }

        int[] indexes;
        if (includeSelf) {
            indexes = list.toIntArray();
        } else {
            int index = candidate.index(vertex);
            if (index == -1)
                return null;
            indexes = new int[]{index};
        }

        int[] nDegs = fvalue.meta.degs;
        String[] nLabels = fvalue.meta.labels;

        Map <Integer, Set <Candidate>> map = new HashMap <>();
        for (int i = 0; i < nDegs.length; i++) {
            int nId = fvalue.fonl[i];
            for (int vIndex : indexes) {
                int[] nIndexes = query.nIndexes(vIndex, nDegs[i], nLabels[i], candidate);

                if (nIndexes == null)
                    continue;

                for (int idx : nIndexes) {
                    // check correct extension of candidate by adding idx by the following check
                    // check neighborhood of idx neighbors in the big graph

//                    if (candidate.canExtend(query.dsnArray[idx], fvalue)) {
//                    IntList intList = new IntArrayList();
//                    for (int idx2 : query.isnArray[idx]) {
//                        if(candidate.vIndices[idx2] != 0) {
//                            intList.add(candidate.vIndices[idx2]);
//                        }
//                    }

                    Candidate cand = new Candidate(candidate);
                    cand.set(vIndex, vertex);
                    cand.set(idx, nId);
                    addToMap(nId, cand, map);
//                    }
                }
            }
        }

        return map;
    }

    public static void addToMap(int kVertex, Candidate candidate, Map <Integer, Set <Candidate>> map) {
        map.compute(kVertex, (k, v) -> {
            if (v == null)
                v = new HashSet <>();
            v.add(candidate);
            return v;
        });
    }

    public static void addToMap(int kVertex, int vVertex, Candidate candidate, Map <Integer, Set <Candidate>> map) {
        map.compute(kVertex, (k, v) -> {
            if (v == null)
                v = new HashSet <>();
            v.add(candidate);
            return v;
        });
    }
}
