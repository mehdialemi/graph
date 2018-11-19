package ir.ac.sbu.graph.types;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class TSetValue {
    public int support;
    public int wLast;
    public int vLast;
    public int uLast;
    public boolean hasUpdate;
    public int[] vertex;
//    public Int2IntMap lowSupMap = new Int2IntOpenHashMap();
}
