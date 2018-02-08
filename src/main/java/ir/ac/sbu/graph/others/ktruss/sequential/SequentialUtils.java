package ir.ac.sbu.graph.others.ktruss.sequential;

import java.util.Set;

/**
 * Utility for sequential methods like sort vertices based on their degrees or sort edges based on their supports
 */
public class SequentialUtils {
    public static final double MAX_CHECK_RATIO = 0.3;

    public static int[] sort(Set<Integer>[] eTriangles, int[] eSorted, int eIndex) {
        if (eSorted != null && eIndex < eSorted.length * MAX_CHECK_RATIO)
            eSorted = null;

        int minSup = eTriangles.length;
        int maxSup = 0;
        int length = maxSup;
        if (eSorted == null) {
            eIndex = 0;
            for (int i = eIndex; i < eTriangles.length; i++) {
                if (eTriangles[i] == null)
                    continue;
                int num = eTriangles[i].size();
                if (num < minSup) {
                    minSup = num;
                } else if (num > maxSup) {
                    maxSup = num;
                }
                length++;
            }
        } else {
            for (int i = eIndex; i < eSorted.length; i ++) {
                int num = eTriangles[eSorted[i]].size();
                if (num < minSup) {
                    minSup = num;
                } else if (num > maxSup) {
                    maxSup = num;
                }
                length++;
            }
        }
        // init array of frequencies
        int[] counts = new int[maxSup - minSup + 1];

        // init the frequencies
        int[] edges = new int[length];
        if (eSorted != null) {
            for (int i = eIndex ; i < eSorted.length ; i ++) {
                int index = eTriangles[eSorted[i]].size() - minSup;
                counts[index] ++;
            }
        } else {
            for (int i = eIndex; i < eTriangles.length; i++) {
                if (eTriangles[i] == null)
                    continue;
                int index = eTriangles[i].size() - minSup;
                counts[index]++;
            }
        }

        counts[0]--;
        for (int i = 1; i < counts.length; i++) {
            counts[i] = counts[i] + counts[i - 1];
        }

        if (eSorted != null) {
            for(int i = eIndex ; i < eSorted.length ; i ++) {
                int index = counts[eTriangles[eSorted[i]].size() - minSup]--;
                edges[index] = eSorted[i];
            }
        } else {
            for (int i = eTriangles.length - 1; i >= 0; i--) {
                if (eTriangles[i] == null)
                    continue;
                int index = counts[eTriangles[i].size() - minSup]--;
                edges[index] = i;
            }
        }

        return edges;
    }

    public static int[] sortDegrees(int[] degArray) {
        int min = degArray[0];
        int max = degArray[0];
        int length = max;
        for (int i = 1; i < degArray.length; i++) {
            if (degArray[i] < min) {
                min = degArray[i];
            } else if (degArray[i] > max) {
                max = degArray[i];
            }
            if (degArray[i] != 0)
                length = i + 1;
        }

        // init array of frequencies
        int[] counts = new int[max - min + 1];

        // init the frequencies
        int[] vertices = new int[length];
        for (int i = 0; i < vertices.length; i++) {
            int index = degArray[i] - min;
            counts[index]++;
        }

        counts[0]--;
        for (int i = 1; i < counts.length; i++) {
            counts[i] = counts[i] + counts[i - 1];
        }

        for (int i = vertices.length - 1; i >= 0; i--) {
            int index = counts[degArray[i] - min]--;
            vertices[index] = i;
        }

        return vertices;
    }

}
