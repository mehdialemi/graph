package ir.ac.sbu.graph.spark.search.fonl.value;

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

    public LabelDegreeTriangleMeta(String label, String[] labels, int[] degrees) {
        this.label = label;
        this.labels = labels;
        this.degrees = degrees;
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
}
