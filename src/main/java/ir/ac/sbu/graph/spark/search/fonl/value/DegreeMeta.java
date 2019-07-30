package ir.ac.sbu.graph.spark.search.fonl.value;

import java.util.Arrays;

public class DegreeMeta extends Meta {
    public int[] degrees;

    public DegreeMeta() {}

    public DegreeMeta(int deg, int size) {
        super(deg);
        this.degrees = new int[size];
    }

    public DegreeMeta(DegreeMeta degreeMeta) {
        super(degreeMeta);
        degrees = degreeMeta.degrees;
    }

    public static FonlValue<DegreeMeta> createEmptyFValue() {
        FonlValue<DegreeMeta> fonlValue = new FonlValue<>();
        fonlValue.meta = new LabelMeta();
        fonlValue.fonl = new int[0];
        fonlValue.meta.degrees = new int[0];
        return fonlValue;
    }

    @Override
    public String toString() {
        return super.toString() + ", degrees: " + Arrays.toString(degrees);
    }
}
