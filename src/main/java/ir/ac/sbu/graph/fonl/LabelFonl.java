package ir.ac.sbu.graph.fonl;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Arrays;

public class LabelFonl extends Fonl<LabelMeta> {

    int index = 0;
    public LabelFonl(int vSize) {
        vArray = new int[vSize];
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
        if (i >= 0)
            return i;

        if (i < -1)
            return -i;

        return -1;
    }

    public int[] vIndexes(int hIndex, String label) {
        IntList list = null;
        for (int i = hIndex; i >= 0; i--) {
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

    public int[] nIndexes(int vIndex, int deg, String label) {
        int i = Arrays.binarySearch(fvalues[vIndex].meta.degs, deg);
        if (i == -1)
            return null;
        int index = i >= 0 ? i + 1 : -i;

        IntList list = null;
        for (int j = 0; j < index; j++) {
            if (fvalues[vIndex].meta.labels[j].equals(label)) {
                if (list == null)
                    list = new IntArrayList();
                list.add(fvalues[vIndex].fonl[j]);
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
