package ir.ac.sbu.graph.clusteringco;

/**
 *
 */
public class Triangle {
    public long v1;
    public long v2;
    public long v3;

    public Triangle(long v1, long v2, long v3) {
        this(v1, v2, v3, false);
    }

    public Triangle(long v1, long v2, long v3, boolean sorted) {
        if (!sorted) {
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
        } else {
            if (v1 < v2) {
                if (v2 < v3) {
                    this.v1 = v1;
                    this.v2 = v2;
                    this.v3 = v3;
                } else {
                    this.v3 = v2;
                    if (v1 < v3) {
                        this.v1 = v1;
                        this.v2 = v3;
                    } else {
                        this.v1 = v3;
                        this.v2 = v1;
                    }
                }
            } else { // v1 > v2
                if (v2 > v3) { // v1 > v2 > w
                    this.v1 = v3;
                    this.v2 = v2;
                    this.v3 = v1;
                } else { // v1 > v2 & v2 < w
                    this.v1 = v2;
                    if (v1 < v3) {
                        this.v2 = v1;
                        this.v3 = v3;
                    } else {
                        this.v2 = v3;
                        this.v3 = v1;
                    }
                }
            }       
        }
    }

    @Override
    public boolean equals(Object obj) {
        Triangle t = (Triangle) obj;
        return v1 == t.v1 && v2 == t.v2 && v3 == t.v3;
    }
}
