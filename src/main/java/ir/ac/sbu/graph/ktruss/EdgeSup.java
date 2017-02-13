package ir.ac.sbu.graph.ktruss;

/**
 *
 */
public class EdgeSup {
    int v1;
    int v2;
    int sup;

    public EdgeSup() {}

    public EdgeSup(int v1, int v2, int sup) {
        this.v1 = v1;
        this.v2 = v2;
        this.sup = sup;
    }

    @Override
    public int hashCode() {
        return sup;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;

        EdgeSup edgeSup = (EdgeSup) obj;
        return v1 == edgeSup.v1 && v2 == edgeSup.v2;
    }
}
