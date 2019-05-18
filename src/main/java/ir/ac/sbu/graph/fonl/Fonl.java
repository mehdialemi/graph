package ir.ac.sbu.graph.fonl;

import java.io.Serializable;

public class Fonl<T extends Meta> implements Serializable {
    public int[] vArray;
    public int[] dArray;
    public Fvalue<T>[] fvalues;
}
