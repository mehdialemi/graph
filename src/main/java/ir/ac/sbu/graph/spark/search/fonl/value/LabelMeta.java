package ir.ac.sbu.graph.spark.search.fonl.value;

import java.util.Arrays;

public class LabelMeta extends DegreeMeta {
    public String label;
    public String labels[];

    public LabelMeta() { }

    public LabelMeta(int deg, int fonlValueSize) {
        this(deg, "", fonlValueSize);
    }

    public LabelMeta(int deg, String label, int fonlValueSize) {
        super(deg, fonlValueSize);
        this.label = label;
        labels = new String[fonlValueSize];
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
