package ir.ac.sbu.biggraph.utils;

/**
 * Edge encapsulation
 */
public class Edge {

    public int v1;
    public int v2;
    public int sup;

    public Edge(int v1, int v2) {
        if (v1 > v2)
            v2 = (v1 + v2) - (v1 = v2);
        this.v1 = v1;
        this.v2 = v2;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        Edge e = (Edge) obj;
        return v1 == e.v1 && v2 == e.v2;
    }

    @Override
    public int hashCode() {
        return v1 ^ v2;
    }

    public Edge updateSup(int len) {
        this.sup = len;
        return this;
    }

    @Override
    public String toString() {
        return "(" + v1 + " , " + v2 + ")";
    }

    public static int compare(Edge e1, Edge e2) {
        if (e1.equals(e2))
            return 0;
        int supDiff = e1.sup - e2.sup;
        return  supDiff != 0 ? supDiff : (e1.v1 != e2.v1 ? e1.v1 - e2.v1 : e1.v2 - e2.v2);
    }
}
