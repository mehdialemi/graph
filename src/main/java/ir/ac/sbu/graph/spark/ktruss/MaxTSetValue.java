package ir.ac.sbu.graph.spark.ktruss;

import java.util.List;

public class MaxTSetValue {
    public int sup;
    public int[] w;
    public int[] v;
    public int[] u;
    public boolean updated;
    public int vSize;

    List<MaxTSetValue> lowTSetValues;
}
