package ir.ac.sbu.graph.spark.search.fonl;

import java.util.Arrays;

public class LabelMeta extends DegreeMeta {
    public String label;
    public String labels[];

    public LabelMeta() { }

    public LabelMeta(int deg, int fonlValueSize) {
        super(deg, fonlValueSize);
    }

    public LabelMeta(LabelMeta labelMeta) {
        super(labelMeta);
        this.label = labelMeta.label;
        labels = labelMeta.labels;
    }

    @Override
    public String toString() {
        return "Label: " + label + ", " + super.toString() + ", labels: " + Arrays.toString(labels);
    }
}
