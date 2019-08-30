package ir.ac.sbu.graph.types;

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

    public long toLong() {
        return longEdge(v1, v2);
    }

    public static long longEdge(int v1, int v2) {
        if (v1 < v2)
            return (long)v1 << 32 | v2 & 0xFFFFFFFFL;
        return (long)v2 << 32 | v1 & 0xFFFFFFFFL;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return super.equals(obj);
        Edge edge = (Edge) obj;
        return this.v1 == edge.v1 && this.v2 == edge.v2;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(v1);
    }
}
