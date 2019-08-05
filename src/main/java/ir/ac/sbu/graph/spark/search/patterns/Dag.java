package ir.ac.sbu.graph.spark.search.patterns;

public class Dag {
    private int v;
    private Dag[] nexts;

    public Dag(int v, Dag[] nexts) {
        this.v = v;
        this.nexts = nexts;
    }
}
