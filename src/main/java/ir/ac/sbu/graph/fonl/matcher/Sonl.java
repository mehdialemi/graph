package ir.ac.sbu.graph.fonl.matcher;

import ir.ac.sbu.graph.spark.search.Candidate;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.Queue;

public class Sonl implements Serializable {

    private int index = 0;
    public int[] vArray;
    public int[] dArray;
    public String[] labels;
    public int[][] dsnArray;
    public int[][] isnArray;

    Sonl(int vSize) {
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

    public int[] neighborIndex(int idx, int[] vArray, int[] orders) {
        IntList neighbors = new IntArrayList();
        for (int order : orders) {
            if (order == 0)
                break;
            int neighbor = vArray[order];
            int result = Arrays.binarySearch(isnArray[idx], neighbor);
            if (result < 0)
                continue;
            neighbors.add(neighbor);
        }

        if (neighbors.isEmpty())
            return null;
        return neighbors.toIntArray();
    }

    public int diameter() {
        BitSet bitSet = new BitSet(vArray.length);
        int d = 0;
        Queue<Integer> queue = new LinkedList <>();
        queue.add(0);
        bitSet.set(0);
        while (bitSet.cardinality() < vArray.length) {
            int v = queue.remove();
            boolean set = false;
            for (int n : dsnArray[v]) {
                if (bitSet.get(n))
                    continue;

                bitSet.set(n);
                queue.add(n);

                if (!set) {
                    d++;
                    set = true;
                }
            }

            if (queue.isEmpty()) {
                for (int i = 0; i < vArray.length; i++) {
                    if (!bitSet.get(i)) {
                        queue.add(i);
                        bitSet.set(i);
                        break;
                    }
                }
            }
        }
        return d;
    }

    public int[] nIndexes(int vIndex, int deg, String label, Candidate candidate) {
        IntList list = null;
        int[] onl = dsnArray[vIndex];

        for (int n : onl) {
            if (dArray[n] > deg)
                break;

            if (candidate.isNotEmpty(n))
                continue;

            if (labels[n].equals(label)) {
                if (list == null)
                    list = new IntArrayList();
                list.add(n);
            }
        }

        if (list == null)
            return null;

        return list.toIntArray();
    }

    public String label(int index) {
        return labels[index];
    }

    public int degree(int index) {
        return dArray[index];
    }
}
