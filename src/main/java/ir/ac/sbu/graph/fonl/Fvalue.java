package ir.ac.sbu.graph.fonl;

import java.util.Arrays;

public class Fvalue<T extends Meta> {
    public T meta;
    public int[] fonl;
    public int[] ifonl;


    @Override
    public String toString() {
        return "Meta: " + meta.toString() + ", fonl: " + Arrays.toString(fonl);
    }
}
