package ir.ac.sbu.graph;

/**
 * Utility class to perform some operations on arrays like intersection
 */
public class ArrayUtils {

    /**
     * Perform intersection on two sorted arrays.
     * @return An array containing left array index which an intersection happened with the right array.
     */
    public static int[] sortedIntersection(int[] left, int lOffset, int lLength, int[] right, int rOffset, int rLength) {
        int l = lOffset;
        int r = rOffset;
        int[] common = new int[Math.min(left.length, right.length)];
        int index = 0;
        while (l < lLength && r < rLength) {
            if (left[l] < right[r])
                l ++;
            else if (left[l] > right[r])
                r ++;
            else {
                common[index ++] = l;
                l ++; r ++;
            }
        }

        int[] c = new int[index];
        System.arraycopy(common, 0, c, 0, index);
        return c;
    }
}
