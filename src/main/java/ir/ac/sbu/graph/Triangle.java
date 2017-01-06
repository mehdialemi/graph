package ir.ac.sbu.graph;

import java.util.Arrays;

/**
 *
 */
public class Triangle {

    public int[] keys;
    public int[][] values;

    public Triangle() {}

    public void reset(int[] keys, int[][] values) {
        this.keys = keys;
        this.values = values;
    }

    public boolean contains(final int k, final int v) {
        int keyIndex = Arrays.binarySearch(keys, k);
        if (keyIndex < 0)
            return false;
        int valueIndex = Arrays.binarySearch(values[keyIndex], v);
        if (valueIndex < 0)
            return false;
        return true;
    }

    public boolean containsEnsureKey(final int k, final int v) {
        int keyIndex = Arrays.binarySearch(keys, k);
        int valueIndex = Arrays.binarySearch(values[keyIndex], v);
        if (valueIndex < 0)
            return false;
        return true;
    }

    public boolean containsKeyIndex(final int keyIndex, final int v) {
        int valueIndex = Arrays.binarySearch(values[keyIndex], v);
        if (valueIndex < 0)
            return false;
        return true;
    }
}
