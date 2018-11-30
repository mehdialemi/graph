package ir.ac.sbu.graph.spark.ktruss;

public class MaxTSetValue {
    public int sup;
    public int[] w;
    public int[] kw;
    public int[] v;
    public int[] kv;
    public int[] u;
    public int[] ku;
    public boolean updated;

    public MaxTSetValue() {}

    public MaxTSetValue(int sup) { this.sup = sup; }
}
