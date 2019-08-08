package ir.ac.sbu.graph.spark.search.patterns;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QuerySlice {
    private int v;
    private int degree;
    private String label;
    private int[] fonlValue;
    private int tc;
    private int[] vTc;

    private boolean processed;
    // store the index of neighbors of the vertices in the fonl value
    private List<Tuple2<Integer, Integer>> triangleIndex = new ArrayList<>();
    private Int2IntOpenHashMap triangleIndexCount = new Int2IntOpenHashMap();
    private String[] labels;
    private int[] degrees;
    // for those neighbors that have querySlice, store index of neighbor in the key and querySlice in the value
    // Pair<
    //       Pair<
    //             index of neighbor in the current fonl,
    //             index of the neighbor fonl value (-1 means neighbor fonl key)
    //           >,
    //       query slice of the neighbor
    //     >
    private List<Tuple2<Integer, QuerySlice>> links = new ArrayList<>();
    private boolean hasParent = false;

    public QuerySlice() {}

    public QuerySlice(int v, int[] fonlValue, List<Triangle> triangles,
                      Map<Integer, String> labels, Int2IntMap degrees) {
        this.v = v;
        this.label = labels.get(v);
        this.degree = degrees.get(v);
        this.fonlValue = fonlValue;
        this.tc = triangles.size();
        for (Triangle triangle : triangles) {
            if (triangle.getV1() != v)
                continue;

            int index1 = fonlIndex(triangle.getV2());
            int index2 = fonlIndex(triangle.getV3());
            triangleIndex.add(new Tuple2<>(index1, index2));
            triangleIndexCount.addTo(index1, 1);
            triangleIndexCount.addTo(index2, 1);
        }

        this.labels = new String[fonlValue.length];
        this.degrees = new int[fonlValue.length];
        this.vTc = new int[fonlValue.length];
        for (int i = 0; i < fonlValue.length; i++) {
            this.labels[i] = labels.getOrDefault(fonlValue[i], "_");
            this.degrees[i] = degrees.get(fonlValue[i]);
            this.vTc[i] = triangleIndexCount.getOrDefault(i, 0);
        }
    }

    public int getV() {
        return this.v;
    }

    public List<Tuple2<Integer, QuerySlice>> getLinks() {
        return links;
    }

    /**
     * Add a new link of query slice to the current query slice
     * @param fonlValueIndex the index of fonl value which makes a link to the sliceLink
     * @param sliceLink the query slice which is linked to the current query slice
     */
    public void addLink(int fonlValueIndex, QuerySlice sliceLink) {
        links.add(new Tuple2<>(fonlValueIndex, sliceLink));
        sliceLink.hasParent = true;
    }

    public boolean hasParent() {
        return hasParent;
    }

    public int[] getFonlValue() {
        return fonlValue;
    }

    public Subquery subquery() {
        Subquery subquery = new Subquery();
        subquery.v = v;
        subquery.label = label;
        subquery.degree = degree;
        subquery.fonlValue = fonlValue;
        subquery.degrees = degrees;
        subquery.labels = labels;
        subquery.tc = tc;
        subquery.vTc = vTc;
        subquery.triangleIndexes = new Tuple2[triangleIndex.size()];
        int i = 0;
        for (Tuple2<Integer, Integer> triangleEdge: triangleIndex) {
            subquery.triangleIndexes[i++] = triangleEdge;
        }
        subquery.linkIndices = new int[links.size()];
        for (int j = 0; j < links.size(); j++) {
            subquery.linkIndices[j] = links.get(j)._1;
        }

        return subquery;
    }

    private int fonlIndex(int v) {
        for (int i = 0; i < fonlValue.length; i++) {
            if (v == fonlValue[i])
                return i;
        }
        return -1;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }
}
