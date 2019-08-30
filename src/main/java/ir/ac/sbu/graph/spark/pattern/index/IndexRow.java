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

    private int[] vertices;
    private String[] labels;
    private int[] degrees;
    private int[] tc;
    private long[] edges;

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

    public Iterator<Tuple2<Integer, MatchCount>> counts(Subquery subquery) {

        Int2IntOpenHashMap vCounter = new Int2IntOpenHashMap();
        PatternCounter patternCounter = new PatternCounter(this, subquery);

        if (size() < subquery.size() || maxDegree() < subquery.maxDegree())
            return Collections.emptyIterator();

        for (int index = 0; index < subquery.size(); index++) {
            IntSet srcIndices = subquery.srcEdgeIndices.get(index);

            for (int i = 0; i < size(); i++) {
                if (!labels[i].equals(subquery.labels[index]) ||
                        degrees[i] < subquery.degrees[index])
                    continue;

                if (subquery.tc[index] > 0) {
                    if (tc == null)
                        continue;
                    if (tc[i] < subquery.tc[index])
                        continue;
                }

                int vertex = vertices[i];
                patternCounter.add(index, vertex, srcIndices);
                vCounter.addTo(vertex, 1);
            }

            if (!patternCounter.finalize(index))
                return Collections.emptyIterator();
        }

        Int2IntOpenHashMap counts = patternCounter.counts();
        List<Tuple2<Integer, MatchCount>> out = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : counts.entrySet()) {
            MatchCount matchCount = new MatchCount(vertices[0], entry.getValue(), entry.getKey());
            out.add(new Tuple2<>(vertices[entry.getKey()], matchCount));
        }

        return out.iterator();
    }

    private int size() {
        return vertices.length;
    }

    private int maxDegree() {
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
