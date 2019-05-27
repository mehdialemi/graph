package ir.ac.sbu.graph.fonl.matcher;

import ir.ac.sbu.graph.types.Edge;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;

public class QSplit implements Serializable {

    public int vIndex; // vertex index

    public int[] fonlIndex;

    // Each eIndexPoint refer to the index of fonlIndex for joining QSplits
    // Note that values should be in range i=[0, fonlIndex.length] such that i = fonlIndex.length refers to
    // joining using vIndex of the current QSplit; otherwise fonlIndex[i] would be used in joining.
    // In fact, we are crea
    public int[] eIndexPoint;

    public QSplit() {
        vIndex = -1;
        fonlIndex = new int[0];
    }

    public QSplit(int vIndex, int[] fonlIndex) {
        this.vIndex = vIndex;
        this.fonlIndex = fonlIndex;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("QSplit");
        sb.append("vIndex: ").append(vIndex).append(", fonlIndex: ").append(Arrays.toString(fonlIndex));
        return sb.toString();
    }
}
