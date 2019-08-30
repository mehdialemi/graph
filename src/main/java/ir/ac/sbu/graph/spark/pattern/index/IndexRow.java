package ir.ac.sbu.graph.spark.pattern.index;

import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleMeta;
import ir.ac.sbu.graph.spark.pattern.query.Subquery;
import ir.ac.sbu.graph.spark.pattern.search.MatchCount;
import ir.ac.sbu.graph.spark.pattern.search.PatternCounter;
import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class IndexRow implements Serializable {

    public int[] vertices;
    public String[] labels;
    public int[] degrees;
    public int[] tc;
    public long[] edges;

    public IndexRow() {
    }

    public IndexRow(int v, int[] fonl, LabelDegreeTriangleMeta meta) {
        vertices = new int[fonl.length + 1];
        vertices[0] = v;
        System.arraycopy(fonl, 0, vertices, 1, fonl.length);

        labels = new String[vertices.length];
        labels[0] = meta.getLabel();
        System.arraycopy(meta.getLabels(), 0, labels, 1, meta.getLabels().length);

        degrees = new int[vertices.length];
        degrees[0] = meta.getDegree();
        System.arraycopy(meta.getDegrees(), 0, degrees, 1, meta.getDegrees().length);

        tc = new int[vertices.length];
        tc[0] = meta.getTc();
        for (int i = 0; i < meta.getvTc().length; i++) {
            tc[i + 1] = meta.getvTc()[i];
        }

        edges = meta.getTriangleEdges();
        Arrays.sort(edges);
    }

    public Tuple2<Integer, IndexRow> toTuple() {
        return new Tuple2<>(vertices[0], this);
    }

    public boolean hasEdge(int v1, int v2) {
        long edge = Edge.longEdge(v1, v2);
        return Arrays.binarySearch(edges, edge) >= 0;
    }

    public int size() {
        return vertices.length;
    }

    public int maxDegree() {
        return degrees[size() - 1];
    }

    public int[] getVertices() {
        return vertices;
    }

    public void setVertices(int[] vertices) {
        this.vertices = vertices;
    }

    public String[] getLabels() {
        return labels;
    }

    public void setLabels(String[] labels) {
        this.labels = labels;
    }

    public int[] getDegrees() {
        return degrees;
    }

    public void setDegrees(int[] degrees) {
        this.degrees = degrees;
    }

    public int[] getTc() {
        return tc;
    }

    public void setTc(int[] tc) {
        this.tc = tc;
    }

    public long[] getEdges() {
        return edges;
    }

    public void setEdges(long[] edges) {
        this.edges = edges;
    }
}
