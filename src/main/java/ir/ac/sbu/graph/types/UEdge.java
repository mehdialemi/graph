package ir.ac.sbu.graph.types;

/**
 * Here always we have v1<v2
 */
public class UEdge {
    public int v1;
    public int v2;

    public UEdge() {}

    public UEdge(int v1, int v2) {
        if (v1 < v2) {
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
        UEdge edge = (UEdge) obj;
        return this.v1 == edge.v1 && this.v2 == edge.v2;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(v1);
    }
}
