package ir.ac.sbu.graph.spark.search.fonl.creator;

public class VLabelDeg extends VLabel {

    public final int degree;

    public VLabelDeg(VLabel vLabel, int deg) {
        this(vLabel.vertex, vLabel.label, deg);
    }

    public VLabelDeg(int vertex, String label, int deg) {
        super(vertex, label);
        this.degree = deg;
    }
}
