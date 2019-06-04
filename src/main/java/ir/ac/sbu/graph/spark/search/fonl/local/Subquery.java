package ir.ac.sbu.graph.spark.search.fonl.local;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntSet;

public class Subquery {
    public int vertex;
    public String label;
    public int degree;
    public int[] fonl;
    public String[] labels;
    public int[] degrees;
    public Int2ObjectMap<IntSet> vi2List;
    public Int2IntMap v2i;

}
