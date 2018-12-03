package ir.ac.sbu.graph.types;

/**
 * Add a vertex and a byte together
 */
public class OrderedVertex {

    public int vertex;
    public byte sign;

    public OrderedVertex() { }

    public OrderedVertex(int vertex, byte sign) {
        this.vertex = vertex;
        this.sign = sign;
    }
}
