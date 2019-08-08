package ir.ac.sbu.graph.spark.search.patterns;

import scala.Serializable;
import scala.Tuple2;

public class Subquery implements Serializable {
    public int v;
    public String label;
    public int degree;
    public int[] fonlValue = null;
    public String[] labels;
    public int[] degrees;
    public Tuple2[] triangleIndexes = null;
    public int tc;
    public int[] vTc;
    public int[] linkIndices;

    public int maxDegree() {
        return degrees[degrees.length - 1];
    }
}
