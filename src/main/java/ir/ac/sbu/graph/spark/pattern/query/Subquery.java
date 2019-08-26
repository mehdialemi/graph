package ir.ac.sbu.graph.spark.pattern.query;

import scala.Serializable;

/**
 * A part of query like a fonl row of query graph. In this class, we have arrays of:
 * - vertices: index 0 is key vertex, other sorted filtered neighbors of key vertex
 * - labels: labels corresponding to vertices
 * - degrees: degrees corresponding to vertices
 * - tc: triangle counts corresponding to vertices
 * - links: index of vertices to connect to other sub-queries
 * - cliques: index of vertices which are connected all together
 */
public class Subquery implements Serializable {
    public int[] vertices;
    public String[] labels;
    public int[] degrees; // sorted ascending
    public int[] tc;
    public int[] links;
    int[][] cliques;

    public Subquery() { }

    public Subquery(int size, boolean hasTc, int linkSize) {
        vertices = new int[size];
        labels = new String[size];
        degrees = new int[size];
        if (hasTc) {
            tc = new int[size];
        }
        if (linkSize > 0) {
            links = new int[linkSize];
        }
    }

    public int maxDegree() {
        return degrees[degrees.length - 1];
    }

    public boolean hasLink() {
        return links != null;
    }

    public boolean hasCliques() {
        return cliques != null;
    }

    public int tc(int idx) {
        if (tc == null)
            return 0;
        return tc[idx];
    }
}
