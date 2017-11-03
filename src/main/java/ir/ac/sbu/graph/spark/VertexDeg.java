package ir.ac.sbu.graph.spark;

/**
 * Encapsulate a vertex and its degree
 */
public class VertexDeg {
    public int vertex;
    public int degree;

    public VertexDeg() { }

    public VertexDeg(int vertex, int degree) {
        this.vertex = vertex;
        this.degree = degree;
    }
}
