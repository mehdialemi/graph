package ir.ac.sbu.graph.spark.search;

import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.fonl.LabelFonl;
import ir.ac.sbu.graph.fonl.LabelMeta;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CandidGen {

    private final int vertex;
    private final Fvalue <LabelMeta> fvalue;
    private final LabelFonl query;

    public CandidGen(int vertex, Fvalue<LabelMeta> fvalue, LabelFonl query) {

        this.vertex = vertex;
        this.fvalue = fvalue;
        this.query = query;
    }

    public List <Tuple2<Integer, Candidate>> newCandidates() {
        return newCandidates(new Candidate(query.size()), true);
    }

    public List <Tuple2<Integer, Candidate>> newCandidates(Candidate candidate) {
        return newCandidates(candidate, false);
    }

    public List <Tuple2<Integer, Candidate>> newCandidates(Candidate candidate, boolean includeSelf) {
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
                return Collections.emptyList();
        }

        int[] indexes;
        if (includeSelf) {
            indexes = list.toIntArray();
        } else {
            indexes = new int[1];
            indexes[0] = candidate.index(vertex);
        }

//        if (candidate.oneEmptyRemain()) {
//            candidate.set(indexes[0], vertex);
//            Tuple2 <Integer, Candidate> tuple = new Tuple2 <>(vertex, candidate);
//            return Arrays.asList(tuple);
//        }

        List <Tuple2 <Integer, Candidate>> newCandidates = new ArrayList<>();
        int[] nDegs = fvalue.meta.degs;
        String[] nLabels = fvalue.meta.labels;


        for (int i = 0; i < nDegs.length; i++) {
            int nId = fvalue.fonl[i];
            for (int vIndex : indexes) {
                int[] vNeighborIndexes = query.nIndexes(vIndex, nDegs[i], nLabels[i], candidate);

                if (vNeighborIndexes == null)
                    continue;

                Candidate cand = new Candidate(candidate);
                cand.set(vIndex, vertex);
                for (int neighborIndex : vNeighborIndexes) {
                    cand.set(neighborIndex, nId);
                }

                newCandidates.add(new Tuple2 <>(nId, cand));
            }
        }

        return newCandidates;

    }
}
