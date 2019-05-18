package ir.ac.sbu.graph.fonl;

import ir.ac.sbu.graph.spark.search.Candidate;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntPriorityQueue;

import java.util.BitSet;

public class SortedNeighbors extends Fonl<LabelMeta> {

    int index = 0;
    public SortedNeighbors(int vSize) {
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

    public int diameter() {
        BitSet bitSet = new BitSet(vArray.length);
        int d = 0;
        IntPriorityQueue queue = new IntArrayFIFOQueue();
        queue.enqueue(0);
        bitSet.set(0);
        while (queue.isEmpty()) {
            int v = queue.dequeueInt();
            boolean set = false;
            for (int n : fvalues[v].fonl) {
                if (bitSet.get(v))
                    continue;

                queue.enqueue(n);

                if (!set)
                    d ++;
                set = true;
            }
        }
        return d;
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
