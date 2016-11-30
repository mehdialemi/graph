package ir.ac.sbu.graph;

import java.util.Random;

/**
 *
 */
public class CountingSort {

    public static void main(String[] args) {
        final int SIZE = 100000000;

        Random r = new Random();
        int[] test = new int[SIZE];

        for (int i = 0; i < SIZE; i++){
            test[i] = r.nextInt(SIZE);
        }

        long start = System.currentTimeMillis();
        test = sort(test);
        long end = System.currentTimeMillis();

        System.out.println(end-start);
    }


    public static int[] sort(int[] array) {

        // array to be sorted in, this array is necessary
        // when we quickSort object datatypes, if we don't,
        // we can quickSort directly into the input array
        int[] aux = new int[array.length];

        // find the smallest and the largest value
        int min = array[0];
        int max = array[0];
        for (int i = 1; i < array.length; i++) {
            if (array[i] < min) {
                min = array[i];
            } else if (array[i] > max) {
                max = array[i];
            }
        }

        // init array of frequencies
        int[] counts = new int[max - min + 1];

        // init the frequencies
        for (int i = 0;  i < array.length; i++) {
            counts[array[i] - min]++;
        }

        // recalculate the array - create the array of occurences
        counts[0]--;
        for (int i = 1; i < counts.length; i++) {
            counts[i] = counts[i] + counts[i-1];
        }

    /*
      Sort the array right to the left
      1) Look up in the array of occurences the last occurence of the given value
      2) Place it into the sorted array
      3) Decrement the index of the last occurence of the given value
      4) Continue with the previous value of the input array (goto set1),
         terminate if all values were already sorted
    */
        for (int i = array.length - 1; i >= 0; i--) {
            aux[counts[array[i] - min]--] = array[i];
        }

        return aux;
    }
}
