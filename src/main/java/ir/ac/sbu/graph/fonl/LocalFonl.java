package ir.ac.sbu.graph.fonl;

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
