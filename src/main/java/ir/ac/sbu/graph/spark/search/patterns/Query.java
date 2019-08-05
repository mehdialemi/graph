package ir.ac.sbu.graph.spark.search.patterns;

import it.unimi.dsi.fastutil.ints.*;

import java.util.*;

public class Query {

    private final List<QuerySlice> querySlices;

    public Query(Map<Integer, List<Integer>> _neighbors,  Map <Integer, String> labelMap) {

        // sort neighbor list by their id
        Map<Integer, int[]> neighbors = new HashMap<>();
        Int2IntOpenHashMap vDegs = new Int2IntOpenHashMap();
        for (Map.Entry<Integer, List<Integer>> entry : _neighbors.entrySet()) {
            neighbors.put(entry.getKey(), new IntAVLTreeSet(entry.getValue()).toIntArray());
            vDegs.addTo(entry.getKey(), entry.getValue().size());
        }

        // create edges and sort vertex by their degree descending
        Set<Edge> edges = new HashSet<>();
        for (Map.Entry<Integer, int[]> entry : neighbors.entrySet()) {
            for (Integer v2 : entry.getValue()) {
                edges.add(new Edge(entry.getKey(), v2));
            }
        }

        // find triangles and create list of triangles per vertex
        Int2ObjectMap<List<Triangle>> v2Triangles = new Int2ObjectOpenHashMap<>();
        for (Map.Entry<Integer, int[]> entry : neighbors.entrySet()) {
            int u = entry.getKey();
            int [] neighborArray = entry.getValue();
            for (int i = 0; i < neighborArray.length - 1; i++) {
                for (int j = i + 1; j < neighborArray.length; j++) {
                    int v = neighborArray[i];
                    int w = neighborArray[j];
                    if (edges.contains(new Edge(v, w))) {
                        Triangle triangle = new Triangle(u, v, w);
                        v2Triangles.computeIfAbsent(u, val -> new ArrayList<>()).add(triangle.uTriangle());
                        v2Triangles.computeIfAbsent(v, val -> new ArrayList<>()).add(triangle.vTriangle());
                        v2Triangles.computeIfAbsent(w, val -> new ArrayList<>()).add(triangle.wTriangle());
                    }
                }
            }
        }

        List<QuerySlice> querySlices = new ArrayList<>();

        // rank vertices by their tc and deg and sort by rank descending
        VertexRanks vertexRanks = new VertexRanks(neighbors, v2Triangles);
        while (vertexRanks.hasItems()) {
            V2Rank v2Rank = vertexRanks.removeHighest();
            if (v2Rank == null)
                break;

            List<Triangle> triangles = v2Triangles.getOrDefault(v2Rank.v, new ArrayList<>());
            for (Triangle triangle : triangles) {
                Edge edge = triangle.getEdge();
                vertexRanks.decRank(edge.v1, 2);
                vertexRanks.decRank(edge.v2, 2);
            }

            querySlices.add(new QuerySlice(v2Rank.v, neighbors.getOrDefault(v2Rank.v, new int[] {}),
                    v2Triangles.getOrDefault(v2Rank.v, new ArrayList<>()), labelMap, vDegs));
        }

        Map<Integer, QuerySlice> sliceMap = new HashMap<>();
        for (QuerySlice querySlice : querySlices) {
            sliceMap.put(querySlice.getV(), querySlice);
        }

        for (QuerySlice querySlice : querySlices) {
            for (int neighbor : neighbors.get(querySlice.getV())) {
                QuerySlice slice = sliceMap.get(neighbor);
                if (slice != null && slice.getParent() == null) {
                    slice.setParent(querySlice);
                    querySlice.addChild(slice);
                }
            }
        }

        this.querySlices = querySlices;
    }

    public List<QuerySlice> getQuerySlices() {
        return querySlices;
    }

    static class V2Rank {
        int v;
        int rank;

        public V2Rank(int v, int rank) {
            this.v = v;
            this.rank = rank;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return super.equals(obj);
            V2Rank o = (V2Rank) obj;
            return this.v == o.v;
        }

        @Override
        public int hashCode() {
            return new Integer(v).hashCode();
        }
    }

    static class VertexRanks {
        SortedMap<V2Rank, V2Rank> v2Ranks = new TreeMap<>((o1, o2) -> o2.rank - o1.rank);

        public VertexRanks(Map<Integer, int[]> neighbors, Int2ObjectMap<List<Triangle>> v2Triangles) {
            for (Map.Entry<Integer, List<Triangle>> v2Triangle : v2Triangles.entrySet()) {
                int v = v2Triangle.getKey();
                int rank = v2Triangle.getValue().size() + neighbors.get(v).length;
                V2Rank v2Rank = new V2Rank(v, rank);
                v2Ranks.put(v2Rank, v2Rank);
            }
        }

        public V2Rank removeHighest() {
            if (v2Ranks.isEmpty())
                return null;
            return v2Ranks.firstKey();
        }

        public void decRank(int v, int amount) {
            V2Rank v2Rank = v2Ranks.get(new V2Rank(v, amount));
            if (v2Rank == null)
                return;

            if (v2Rank.rank <= 0) {
                v2Ranks.remove(v2Rank);
                return;
            }

            v2Rank.rank -= amount;
            if (v2Rank.rank <= 0) {
                v2Ranks.remove(v2Rank);
                return;
            }
            v2Ranks.put(v2Rank, v2Rank);
        }

        public boolean hasItems() {
            return !v2Ranks.isEmpty();
        }
    }

}
