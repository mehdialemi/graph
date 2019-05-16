package ir.ac.sbu.graph.fonl;

import java.io.Serializable;

public class Meta implements Serializable {
    public int deg;

    public Meta() {}

    public Meta(Meta meta) {
        deg = meta.deg;
    }

    @Override
    public String toString() {
        return "deg: " + deg;
    }
}
