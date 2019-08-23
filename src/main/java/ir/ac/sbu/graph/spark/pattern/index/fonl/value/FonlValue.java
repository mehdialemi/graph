package ir.ac.sbu.graph.spark.pattern.index.fonl.value;

import java.io.Serializable;
import java.util.Arrays;

public class FonlValue<T extends Meta> implements Serializable {
    public T meta;
    public int[] fonl;

    @Override
    public String toString() {
        return "Meta: " + meta + ", fonl: " + Arrays.toString(fonl);
    }
}
