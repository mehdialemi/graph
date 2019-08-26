package ir.ac.sbu.graph.spark.pattern.query;

import it.unimi.dsi.fastutil.ints.*;
import scala.Tuple2;

import java.util.*;

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

    public QuerySlice() {
    }

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

    public boolean hasNotProcessedLink() {
        for (Tuple2<Integer, QuerySlice> link : links) {
            if (!link._2.isProcessed())
                return true;
        }
        return false;
    }

    /**
     * Add a new link of query slice to the current query slice
     *
     * @param fonlValueIndex the index of fonl value which makes a link to the sliceLink
     * @param sliceLink      the query slice which is linked to the current query slice
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

        boolean hasTc = triangleIndex.size() + tc > 0;
        Subquery subquery = new Subquery(fonlValue.length + 1, hasTc, links.size());
        subquery.vertices[0] = v;
        subquery.labels[0] = label;
        subquery.degrees[0] = degree;
        System.arraycopy(fonlValue, 0, subquery.vertices, 1, fonlValue.length);
        System.arraycopy(labels, 0, subquery.labels, 1, labels.length);
        System.arraycopy(degrees, 0, subquery.degrees, 1, degrees.length);

        if (hasTc) {
            subquery.tc[0] = tc;
            for (Map.Entry<Integer, Integer> entry : triangleIndexCount.entrySet()) {
                subquery.tc[entry.getKey() + 1] = entry.getValue();
            }
        }

        for (int i = 0; i < links.size(); i++) {
            subquery.links[i] = links.get(i)._1;
        }

        // vertex in index 0 is connected to all other vertices in index 1 .. len - 1
        Int2ObjectMap<IntSet> cMapSet = new Int2ObjectOpenHashMap<>();
        for (Tuple2<Integer, Integer> edge : triangleIndex) {
            cMapSet.computeIfAbsent(edge._1, v -> new IntOpenHashSet()).add(edge._2);
        }
        Int2ObjectMap<IntSet> cMapSetBackup = new Int2ObjectOpenHashMap<>(cMapSet);

        int[] indices = cMapSet.keySet().toIntArray();
        for (int i = 0; i < indices.length; i++) {
            int index = indices[i];
            IntSet vIdxNeighbors = cMapSet.get(index);
            for (int vIdxNeighbor : vIdxNeighbors.toIntArray()) {
                IntSet vIdxNeighbor2 = cMapSet.get(vIdxNeighbor);
                vIdxNeighbors.retainAll(vIdxNeighbor2);
                vIdxNeighbors.add(vIdxNeighbor);
            }
        }

        for (int index : indices) {
            IntSet vIdxNeighbors = cMapSetBackup.get(index);
            vIdxNeighbors.removeAll(cMapSet.get(index));
        }

        List<int[]> list = createIdxCliques(cMapSet);
        list.addAll(createIdxCliques(cMapSetBackup));

        if (list.size() > 0) {
            subquery.cliques = new int[list.size()][];
            for (int i = 0; i < list.size(); i++) {
                subquery.cliques[i] = list.get(i);
            }
        }

       return subquery;
    }

    private List<int[]> createIdxCliques(Int2ObjectMap<IntSet> map) {
        List<int[]> cSet = new ArrayList<>();
        for (Map.Entry<Integer, IntSet> entry : map.entrySet()) {
            IntSet set = entry.getValue();
            if (set.isEmpty())
                continue;
            set.add(entry.getKey());
            cSet.add(set.toIntArray());
        }
        return cSet;
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

    @Override
    public String toString() {
        return "QuerySlice(v:" + v + ", degree: " + degree + ", label: " + label + ", tc: " + tc +
                ", vTc: " + Arrays.toString(vTc) + ", labels: " + Arrays.toString(labels) +
                ", degrees: " + Arrays.toString(degrees) + ", linkCount: " + links.size() +
                ", fonlValue: " + Arrays.toString(fonlValue) + ", triangleIndex: " + triangleIndex +
                ", triangleIndexCount: " + triangleIndexCount + ", hasParent: " + hasParent +
                ")";
    }
}
