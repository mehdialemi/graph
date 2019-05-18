package ir.ac.sbu.graph.fonl;

import ir.ac.sbu.graph.spark.search.Candidate;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Arrays;

public class LabelFonl extends Fonl<LabelMeta> {

    int index = 0;
    public LabelFonl(int vSize) {
        vArray = new int[vSize];
        dArray = new int[vSize];
        fvalues = new Fvalue[vSize];
    }

    public int size() {
        return vArray.length;
    }

    public void add(int vertex, int degree, Fvalue<LabelMeta> fvalue) {
        vArray[index] = vertex;
        dArray[index] = degree;
        fvalues[index ++] = fvalue;
    }

    public int lowerDegIndex(int degree) {
        int i = Arrays.binarySearch(dArray, degree);
        if (i == -1)
            return -1;

        int index = i >= 0 ? i : -i;
        return Math.min(index, vArray.length - 1);
    }

    public int[] vIndexes(int hIndex, String label, Candidate candidate) {
        IntList list = null;
        for (int i = hIndex; i >= 0; i--) {
            if (candidate.isNotEmpty(i))
                continue;

            if (fvalues[i].meta.label.equals(label)) {
                if (list == null)
                    list = new IntArrayList();
                list.add(i);
            }
        }

        if (list == null)
            return null;

        return list.toIntArray();
    }

    public int[] nIndexes(int vIndex, int deg, String label, Candidate candidate) {
        IntList list = null;
        int[] fonl = fvalues[vIndex].fonl;
        LabelMeta meta = fvalues[vIndex].meta;

        for (int i = 0; i < fonl.length; i++) {
            if (meta.degs[i] > deg)
                break;

            if (candidate.isNotEmpty(fonl[i]))
                continue;

            if (meta.labels[i].equals(label)) {
                if (list == null)
                    list = new IntArrayList();

                list.add(fonl[i]);
            }
        }

        if (list == null)
            return null;

        return list.toIntArray();
    }

    public String label(int index) {
        return fvalues[index].meta.label;
    }

    public int degree(int index) {
        return fvalues[index].meta.deg;
    }
}
