package ir.ac.sbu.graph.utils;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Sort based on degree
 */
public class VertexCompare {

    private final int[] deg;

    public VertexCompare(int[] deg) {
        this.deg = deg;
    }

    public int compare(int v1, int v2) {
        if (v1 == v2)
            return 0;
        if (deg[v1] < deg[v2])
            return -1;
        if (deg[v1] > deg[v2])
            return 1;
        if (v1 < v2)
            return -1;
        return 1;
    }

    public void quickSort(IntArrayList neighbor, int low, int high) {
        if (neighbor == null || neighbor.size() == 0)
            return;

        if (low >= high)
            return;

        // pick the pivot
        int middle = low + (high - low) / 2;
        int pivot = neighbor.getInt(middle);

        // make left < pivot and right > pivot
        int i = low, j = high;
        while (i <= j) {
            while (compare(neighbor.getInt(i), pivot) == -1)
                i++;

            while (compare(neighbor.getInt(j), pivot) == 1)
                j--;

            if (i <= j) {
                int temp = neighbor.getInt(i);
                neighbor.set(i, neighbor.getInt(j));
                neighbor.set(j, temp);
                i++;
                j--;
            }
        }

        // recursively quickSort two sub parts
        if (low < j)
            quickSort(neighbor, low, j);

        if (high > i)
            quickSort(neighbor, i, high);
    }


    public void quickSort(int[] neighbor, int low, int high) {
        if (neighbor == null || neighbor.length == 0)
            return;

        if (low >= high)
            return;

        // pick the pivot
        int middle = low + (high - low) / 2;
        int pivot = neighbor[middle];

        // make left < pivot and right > pivot
        int i = low, j = high;
        while (i <= j) {
            while (compare(neighbor[i], pivot) == -1)
                i++;

            while (compare(neighbor[j], pivot) == 1)
                j--;

            if (i <= j) {
                int temp = neighbor[i];
                neighbor[i] = neighbor[j];
                neighbor[j] = temp;
                i++;
                j--;
            }
        }

        // recursively quickSort two sub parts
        if (low < j)
            quickSort(neighbor, low, j);

        if (high > i)
            quickSort(neighbor, i, high);
    }
}
