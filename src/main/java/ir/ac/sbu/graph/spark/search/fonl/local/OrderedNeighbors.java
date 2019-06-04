package ir.ac.sbu.graph.spark.search.fonl.local;

import java.io.Serializable;
import java.util.Arrays;

public class OrderedNeighbors implements Serializable {

    private int index = 0;
    public int[] vArray;
    public int[] dArray;
    public String[] labels;
    public int[][] dsnArray;
    public int[][] isnArray;

    public OrderedNeighbors(int vSize) {
        vArray = new int[vSize];
        dArray = new int[vSize];
        labels = new String[vSize];
        dsnArray = new int[vSize][];
        isnArray = new int[vSize][];
    }

    public void finalize() {

        for (int i = 0; i < dsnArray.length; i++) {
            isnArray[i] = new int[dsnArray[i].length];
            System.arraycopy(dsnArray[i], 0, isnArray[i], 0, isnArray[i].length);
            Arrays.sort(isnArray[i]);
        }
    }

    public int size() {
        return vArray.length;
    }

    public void add(int vertex, int degree, String label, int[] onArray) {
        vArray[index] = vertex;
        dArray[index] = degree;
        labels[index] = label;
        dsnArray[index] = onArray;
        index ++;
    }

    public String label(int index) {
        return labels[index];
    }

    public int degree(int index) {
        return dArray[index];
    }
}
