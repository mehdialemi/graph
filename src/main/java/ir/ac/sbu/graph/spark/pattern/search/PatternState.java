package ir.ac.sbu.graph.spark.pattern.search;

import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import scala.Tuple2;

public class PatternState {

    private int[][] pVertices;
    private long[] triangles;

    public PatternState(int[][] pVertices, long[] triangles) {
        this.pVertices = pVertices;
        this.triangles = triangles;
    }

    public Tuple2<Integer, int[]> countAndLink(Subquery subquery, int link) {
        return new Tuple2<>(0, pVertices[link]);
    }
}
