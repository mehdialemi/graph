package ir.ac.sbu.graph;

import java.util.Random;

/**
 *
 */
public class ProbabilityRandom {

    private final float[] p;
    Random random = new Random();
    int count;
    int[] sizes;
    private final int total;

    public ProbabilityRandom(int len, int total) {
        sizes = new int[len];
        this.total = total;
        count = 0;
        for (int i = 0; i < sizes.length; i++) {
            sizes[i] = total;
            count += total;
        }
        p = new float[sizes.length];
    }

    public void increment(int index) {
        sizes[index] --;
        count--;
    }

    public int getNextRandom() {
        for (int i = 0; i < sizes.length; i++) {
            p[i] = sizes[i] / (float) count;
        }

        for (int i = 1 ; i < sizes.length; i ++)
            p[i] += p[i - 1];

        float d = random.nextInt(count) / (float) count;
        for (int index = 0; index < p.length; index++) {
            if (d < p[index])
                return index;
        }
        return 0;
    }
}
