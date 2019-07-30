package ir.ac.sbu.graph.spark.search.fonl.value;

import java.io.Serializable;

public class Meta implements Serializable {
    public int degree;

    public Meta() {}

    public Meta(int degree) {
        this.degree = degree;
    }

    public Meta(Meta meta) {
        degree = meta.degree;
    }

    @Override
    public String toString() {
        return "degree: " + degree;
    }
}
