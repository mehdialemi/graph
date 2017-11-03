package ir.ac.sbu.graph.spark;

/**
 * Encapsulate edge
 */
public class Edge {
    public int v1;
    public int v2;

    public Edge() {}

    public Edge(int v1, int v2) {
        this.v1 = v1;
        this.v2 = v2;
    }
}
