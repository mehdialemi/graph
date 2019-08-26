package ir.ac.sbu.graph.spark.pattern.index.fonl.value;

import java.util.Arrays;

public class LabelDegreeTriangleMeta extends TriangleMeta {

    private String label;
    private String[] labels;
    private int[] degrees;

    public LabelDegreeTriangleMeta() { }

    public LabelDegreeTriangleMeta(TriangleMeta triangleMeta, int size) {
        super(triangleMeta);
        degree = triangleMeta.degree;
        labels = new String[size];
        degrees = new int[size];
    }

    public int maxDegree() {
        return degrees[degrees.length - 1];
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String[] getLabels() {
        return labels;
    }

    public int[] getDegrees() {
        return degrees;
    }

    public void setLabelDegree(int index, String label, int degree) {
        labels[index] = label;
        degrees[index] = degree;
    }

    public void setLabels(String[] labels) {
        this.labels = labels;
    }

    public void setDegrees(int[] degrees) {
        this.degrees = degrees;
    }

    @Override
    public String toString() {
        return super.toString() + " LabelDegreeTriangleMeta(label: " + label + ", labels: " + Arrays.toString(labels) +
                ", degrees: " + Arrays.toString(degrees);
    }
}
