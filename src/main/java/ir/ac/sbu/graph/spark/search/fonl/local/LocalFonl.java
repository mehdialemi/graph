package ir.ac.sbu.graph.spark.search.fonl.local;

import java.io.Serializable;

public class LocalFonl implements Serializable {
    public int[] vIndices;
    public int[][] fonl;
    public int[] degIndices;
    public String[] labels;

    public int[] getFonl(int index) {
        return fonl[index];
    }
}
