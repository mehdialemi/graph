package ir.ac.sbu.graph.spark.partitioning;

public class PInfo {

    public int low;
    public int high;
    public int partitionNum;
    public int[] sortedDegCounts; // from low to high degree
}
