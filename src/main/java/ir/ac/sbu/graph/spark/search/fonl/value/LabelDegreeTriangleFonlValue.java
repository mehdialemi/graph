package ir.ac.sbu.graph.spark.search.fonl.value;

public class LabelDegreeTriangleFonlValue extends FonlValue<LabelDegreeTriangleMeta> {

    public LabelDegreeTriangleFonlValue() { }

    public LabelDegreeTriangleFonlValue(int[] fonl, LabelDegreeTriangleMeta meta) {
        this.fonl = fonl;
        this.meta = meta;
    }
}
