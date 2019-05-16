package ir.ac.sbu.graph.fonl;

import java.io.Serializable;

public class Meta implements Serializable {
    public int deg;

    @Override
    public String toString() {
        return "deg: " + deg;
    }
}
