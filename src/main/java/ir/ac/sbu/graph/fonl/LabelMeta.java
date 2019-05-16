package ir.ac.sbu.graph.fonl;

import java.util.Arrays;

public class LabelMeta extends DegreeMeta {
    public String labels[];

    public static Fvalue <LabelMeta> emptyLabelMetaValue() {
        Fvalue<LabelMeta> fvalue = new Fvalue <>();
        fvalue.meta = new LabelMeta();
        fvalue.fonl = new int[0];
        fvalue.meta.degs = new int[0];
        fvalue.meta.labels = new String[0];
        return fvalue;
    }

    @Override
    public String toString() {
        return super.toString() + ", labels: " + Arrays.toString(labels);
    }
}
