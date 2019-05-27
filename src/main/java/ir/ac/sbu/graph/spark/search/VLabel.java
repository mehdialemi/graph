package ir.ac.sbu.graph.spark.search;

import java.io.Serializable;

public class VLabel implements Serializable {

    public int vertex;
    public String label;

    public VLabel() {}

    public VLabel(int vertex, String label) {

        this.vertex = vertex;
        this.label = label;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return super.equals(obj);

        VLabel other = (VLabel) obj;
        return vertex == other.vertex;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(vertex);
    }
}
