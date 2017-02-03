package ir.ac.sbu.graph;

import java.util.Arrays;

/**
 *
 */
public class TriangleSubgraph {

    public int[] keys;
    public int[][] values;

    public TriangleSubgraph() {}

    public void reset(int[] keys, int[][] values) {
        this.keys = keys;
        this.values = values;
    }

    public int getKeyIndex(int k) {
        int i = 0;
        while (i < keys.length && keys[i] == -1) i ++;
        return Arrays.binarySearch(keys, i, keys.length, k);
    }

    public void removeKeyInIndex(int keyIndex) {
        for(int i = 0 ; i < keyIndex; i ++) {
            keys[i + 1] = keys[i];
            values[i + 1] = values[i];
        }
        values[keyIndex] = new int[]{};
        keys[keyIndex] = -1;
    }

    public void removeValueIndexInKeyIndex(int keyIndex, int valueIndex) {
        for(int i = 0 ; i < valueIndex; i ++) {
            values[keyIndex][i + 1] = values[keyIndex][i];
        }
        values[keyIndex][valueIndex] = -1;
    }

    public int getValueIndex(final int keyIndex, final int v) {
        int i = 0;
        while (i < values[keyIndex].length && values[keyIndex][i] == -1) i ++;
        return Arrays.binarySearch(values[keyIndex], i, values[keyIndex].length, v);
    }
}
