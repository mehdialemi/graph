package ir.ac.sbu.graph.spark.search.fonl;

import ir.ac.sbu.graph.fonl.Fvalue;
import ir.ac.sbu.graph.fonl.Meta;

import java.util.Arrays;

public class DegreeMeta extends Meta {
    public int[] degs;

    public DegreeMeta() {}

    public DegreeMeta(int deg, int fSize) {
        this.deg = deg;
        this.degs = new int[fSize];
    }

    public DegreeMeta(DegreeMeta degreeMeta) {
        super(degreeMeta);
        degs = degreeMeta.degs;
    }

    public static Fvalue<DegreeMeta> createEmptyFValue() {
        Fvalue<DegreeMeta> fvalue = new Fvalue <>();
        fvalue.meta = new LabelMeta();
        fvalue.fonl = new int[0];
        fvalue.meta.degs = new int[0];
        return fvalue;
    }

    @Override
    public String toString() {
        return super.toString() + ", degs: " + Arrays.toString(degs);
    }
}
