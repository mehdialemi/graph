package ir.ac.sbu.graph.spark.partitioning;

import java.util.Comparator;
import java.util.Map;

class ValueComparator implements Comparator<Integer> {
    Map<Integer, Long> base;

    public ValueComparator(Map<Integer, Long> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with
    // equals.
    public int compare(Integer a, Integer b) {
        Long aVal = base.get(a);
        Long bVal = base.get(b);
        if (aVal == null || bVal == null)
            return -1;

        int diff = (int) (aVal.longValue() - bVal.longValue());
        return diff <= 0 ? -1 : 1;
    }
}