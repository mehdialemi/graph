package ir.ac.sbu.graph.fonl;

import java.util.Arrays;

public class LabelMeta extends DegreeMeta {
    public String label;
    public String labels[];

    public LabelMeta(){

    }

    public LabelMeta(LabelMeta labelMeta) {
        super(labelMeta);
        labels = labelMeta.labels;
    }

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
        return "Label: " + label + ", " + super.toString() + ", labels: " + Arrays.toString(labels);
    }
}
