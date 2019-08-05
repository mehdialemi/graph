package ir.ac.sbu.graph.spark.search.patterns;

public class Edge {
    int v1, v2;
    Edge(int v1, int v2) {
        if (v1 <= v2) {
            this.v1 = v1;
            this.v2 = v2;
        } else {
            this.v1 = v2;
            this.v2 = v1;
        }
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
