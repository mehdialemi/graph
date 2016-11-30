package ir.ac.sbu.graph;

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

    public void quickSort(int[] fonl, int low, int high) {
        if (fonl == null || fonl.length == 0)
            return;

        if (low >= high)
            return;

        // pick the pivot
        int middle = low + (high - low) / 2;
        int pivot = fonl[middle];

        // make left < pivot and right > pivot
        int i = low, j = high;
        while (i <= j) {
            while (compare(fonl[i], pivot) == -1)
                i++;

            while (compare(fonl[j], pivot) == 1)
                j--;

            if (i <= j) {
                int temp = fonl[i];
                fonl[i] = fonl[j];
                fonl[j] = temp;
                i++;
                j--;
            }
        }

        // recursively quickSort two sub parts
        if (low < j)
            quickSort(fonl, low, j);

        if (high > i)
            quickSort(fonl, i, high);
    }
}
