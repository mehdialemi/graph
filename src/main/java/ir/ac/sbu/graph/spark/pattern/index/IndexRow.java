package ir.ac.sbu.graph.spark.pattern.index;

import ir.ac.sbu.graph.spark.pattern.index.fonl.value.LabelDegreeTriangleMeta;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

public class IndexRow implements Serializable {

    private int[] vertices;
    private String[] labels;
    private int[] degrees;
    private int[] tCounts;
    private long[] edges;

    public IndexRow() { }

    public IndexRow(Row row) {
        vertices = (int[]) row.get(0);
        labels = (String[]) row.get(1);
        degrees = (int[]) row.get(2);
        tCounts = (int[]) row.get(3);
        edges = (long[]) row.get(4);
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

        if (meta.getTc() > 0) {
            tCounts = new int[vertices.length];
            tCounts[0] = meta.getTc();
            for (int i = 0; i < meta.getvTc().length; i++) {
                tCounts[i + 1] = meta.getvTc()[i];
            }
        }

        edges = meta.getTriangleEdges();
        Arrays.sort(edges);
    }

    public static Tuple2<Integer, IndexRow> toTuple(Row row) {
        IndexRow r = new IndexRow(row);
        return new Tuple2<>(r.vertices[0], r);
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

    public int[] gettCounts() {
        return tCounts;
    }

    public void settCounts(int[] tCounts) {
        this.tCounts = tCounts;
    }

    public long[] getEdges() {
        return edges;
    }

    public void setEdges(long[] edges) {
        this.edges = edges;
    }
}
