package ir.ac.sbu.graph.spark.pattern.query;

import ir.ac.sbu.graph.types.Edge;
import it.unimi.dsi.fastutil.ints.*;

import java.util.*;

public class Query {

    private final List<QuerySlice> querySlices = new ArrayList<>();

    public Query(Map<Integer, List<Integer>> neighbors,  Map <Integer, String> labelMap) {

        // init: constructIndex edge set and store degrees in a map
        Set<Edge> edges = new HashSet<>();
        final Int2IntOpenHashMap degrees = new Int2IntOpenHashMap();
        for (Map.Entry<Integer, List<Integer>> entry : neighbors.entrySet()) {
            degrees.addTo(entry.getKey(), entry.getValue().size());
            for (Integer v : entry.getValue()) {
                edges.add(new Edge(entry.getKey(), v));
            }
        }

        // constructIndex fonl
        Int2ObjectMap<IntSet> fonl = new Int2ObjectOpenHashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : neighbors.entrySet()) {
            IntSortedSet fonlValue = new IntAVLTreeSet((v1, v2) -> {
                int d1 = degrees.getOrDefault(v1, 0);
                int d2 = degrees.getOrDefault(v2, 0);
                return d1 == d2 ? v1 - v2 : d1 - d2;
            });

            int v = entry.getKey();
            int d = degrees.get(v);
            for (int neighbor : entry.getValue()) {
                int d1 = degrees.getOrDefault(neighbor, 0);
                if (d1 < d || (d1 == d && neighbor < v))
                    continue;
                fonlValue.add(neighbor);
            }
            fonl.put(v, fonlValue);
        }

        // find triangles and constructIndex list of triangles per vertex
        Int2ObjectMap<List<Triangle>> triangles = new Int2ObjectOpenHashMap<>();
        for (Map.Entry<Integer, IntSet> entry : fonl.entrySet()) {
            int u = entry.getKey();
            int [] vertices = entry.getValue().toIntArray();
            for (int i = 0; i < vertices.length - 1; i++) {
                int v = vertices[i];
                for (int j = i + 1; j < vertices.length; j++) {
                    int w = vertices[j];
                    if (edges.contains(new Edge(v, w))) {
                        triangles.computeIfAbsent(u, val -> new ArrayList<>())
                                .add(new Triangle(u, v, w));
                    }
                }
            }
        }

        // rank vertices by their tc and deg and sort by rank descending
        Int2IntOpenHashMap ranks = new Int2IntOpenHashMap();
        for (Map.Entry<Integer, IntSet> entry : fonl.entrySet()) {
            int v = entry.getKey();
            int size = entry.getValue().size();
            int tc = triangles.getOrDefault(v, new ArrayList<>()).size();
            ranks.addTo(v, 2 * size + tc);
        }

        Int2IntSortedMap uRanks = new Int2IntAVLTreeMap((v1, v2) -> {
            int compare = ranks.getOrDefault(v2, 0) - ranks.getOrDefault(v1, 0);
            if (compare == 0)
                return v2 - v1;
            return compare;
        });

        uRanks.putAll(ranks);

        // store original value of fonl before its removeAndUpdate
        Int2ObjectMap<IntSet> freezeFonl = Int2ObjectMaps.unmodifiable(fonl);
        Int2ObjectMap<QuerySlice> querySliceMap = new Int2ObjectOpenHashMap<>();
        List<QuerySlice> querySliceList = new ArrayList<>();
        while (!uRanks.isEmpty()) {
            int v = uRanks.firstIntKey();
            IntSet vertices = fonl.get(v);

            if (vertices.size() > 0) {
                List<Triangle> triangleList = triangles.getOrDefault(v, new ArrayList<>());

                QuerySlice querySlice = new QuerySlice(v, freezeFonl.get(v).toIntArray(),
                        triangleList, labelMap, degrees);
                querySliceList.add(querySlice);
                querySliceMap.put(v, querySlice);

                for (Triangle triangle : triangleList) {
                    // remove triangle vertex (v3) from the fonl value of v2
                    // update ranks and sortedRanks for triangle v2
                    removeAndUpdate(fonl, ranks, uRanks, triangle.getV2(), triangle.getV3());
                }
            }

            uRanks.remove(v);
        }

        // find the relationship between query slices
        for (QuerySlice querySlice : querySliceList) {
            int v = querySlice.getV();
            int[] fonlValue = freezeFonl.get(v).toIntArray();
            for (int index = 0; index < fonlValue.length; index++) {
                int neighbor = fonlValue[index];
                QuerySlice neighborQuerySlice = querySliceMap.get(neighbor);
                if (neighborQuerySlice != null) {
                    if (!neighborQuerySlice.hasParent()) {
                        querySlice.addLink(index, neighborQuerySlice);
                    }
                }
            }
        }

        // find the remaining links between the fonlValues
        for (int i = 1; i < querySliceList.size(); i++) {
            QuerySlice querySlice = querySliceList.get(i);
            if (querySlice.hasParent())
                continue;

            int[] neighborFonlValue = querySlice.getFonlValue();
            boolean find = false;
            for (QuerySlice parentQuerySlice : querySliceList) {
                int[] parentFonlValue = parentQuerySlice.getFonlValue();

                for (int pIndex = 0; pIndex < parentFonlValue.length && !find; pIndex++) {
                    int fonlValue = parentFonlValue[pIndex];

                    for (int nIndex = 0; nIndex < neighborFonlValue.length && !find; nIndex++) {
                        if (fonlValue == neighborFonlValue[nIndex]) {
                            find = true;
                            parentQuerySlice.addLink(pIndex, querySlice);
                        }
                    }
                }

                if (find)
                    break;
            }

            if (!find)
                throw new RuntimeException("Could not find links for vertex: " + querySlice.getV());
        }

        for (QuerySlice querySlice : querySliceList) {
            querySlice.setProcessed(false);
        }

        // add query slices based on highest slice and its links
        for (int i = 0; i < querySliceList.size(); i++) {
            QuerySlice querySlice = querySliceList.get(i);
            if (!querySlice.isProcessed()) {
                querySlice.setProcessed(true);
                this.querySlices.add(querySlice);
            }

            for (int j = 0; j < querySliceList.size(); j++) {
                if (i == j) {
                    continue;
                }
                QuerySlice link = querySliceList.get(i);
                if (link.isProcessed())
                    continue;

                if (querySlice.getLinks().contains(link)) {
                    link.setProcessed(true);
                    this.querySlices.add(link);
                }
            }
        }

        for (QuerySlice querySlice : this.querySlices) {
            querySlice.setProcessed(false);
        }
    }

    public List<QuerySlice> getQuerySlices() {
        return querySlices;
    }

    /**
     *  remove triangle vertex (v3) from the fonl value of v2
     *  update ranks and uRanks for triangle v2
     * @param fonl the updating fonl
     * @param ranks the original rank
     * @param uRanks the updating ranks
     * @param v1
     * @param v2
     */
    private void removeAndUpdate(Int2ObjectMap<IntSet> fonl, Int2IntOpenHashMap ranks,
                                 Int2IntSortedMap uRanks, int v1, int v2) {
        IntSet v1Set = fonl.get(v1);
        if (v1Set.rem(v2)) {
            uRanks.remove(v1);
            int rank = ranks.get(v1) - 2;
            if (rank > 0) {
                ranks.put(v1, rank);
                uRanks.put(v1, rank);
            }
        }

    }

    @Override
    public String toString() {
        return "Query(" + querySlices + ")";
    }
}
