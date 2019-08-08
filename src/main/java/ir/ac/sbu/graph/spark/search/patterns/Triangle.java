package ir.ac.sbu.graph.spark.search.patterns;

public class Triangle {
    private final int v1;
    private final int v2;
    private final int v3;

    public Triangle(int v1, int v2, int v3) {
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
    }

    public int getV1() {
        return v1;
    }

    public int getV2() {
        return v2;
    }

    public int getV3() {
        return v3;
    }
}
