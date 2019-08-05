package ir.ac.sbu.graph.spark.search.patterns;

public class Triangle {
    private final int u, v, w;
    int u1, v1, w1;
    Object processed = new Object();

    public Triangle(int u, int v, int w) {
        this.u = u;
        this.v = v;
        this.w = w;
    }

    public boolean isProcessed() {
        return processed == null;
    }

    public void processed() {
        processed = null;
    }

    public Edge getEdge() {
        return new Edge(v1, w1);
    }

    public Triangle uTriangle() {
        return set(u, v, w);
    }

    public Triangle vTriangle() {
        return set(v, u, w);
    }

    public Triangle wTriangle() {
        return set(w, u, v);
    }

    private Triangle set(int u, int v, int w) {
        this.u1 = u;
        this.v1 = v;
        this.w1 = w;
        return this;
    }
}
