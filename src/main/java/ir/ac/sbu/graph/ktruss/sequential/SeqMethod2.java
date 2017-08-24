package ir.ac.sbu.graph.ktruss.sequential;

import ir.ac.sbu.graph.Edge;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Use edge map such that key is an {@link Edge} object and value is
 * its vertices in the existing triangles in front of edge
 */
public class SeqMethod2 extends SequentialBase {

    public SeqMethod2(Edge[] edges, int minSup) {
        super(edges, minSup);
    }

    @Override
    public void start() throws Exception {
        long tStart = System.currentTimeMillis();
        Map<Edge, Set<Integer>> edgeMap = findET(edges);
        long tt = System.currentTimeMillis();
        System.out.println("TriangleSubgraph finding in " + (tt - tStart) + " ms");

        int iteration = 0;
        long tStartLoop = System.currentTimeMillis();
        while (true) {
            long t1 = System.currentTimeMillis();
            System.out.println("iteration: " + ++iteration);
            List<Map.Entry<Edge, Set<Integer>>> invalids = new ArrayList<>();
            for (Map.Entry<Edge, Set<Integer>> edgeSetEntry : edgeMap.entrySet()) {
                if (edgeSetEntry.getValue().size() < minSup)
                    invalids.add(edgeSetEntry);
            }
            long t2 = System.currentTimeMillis();

            System.out.println("invalid size: " + invalids.size() + " duration: " + (t2 - t1) + " ms");
            if (invalids.size() == 0)
                break;

            for (Map.Entry<Edge, Set<Integer>> invalid : invalids) {
                edgeMap.remove(invalid.getKey());
                for (Integer vertex : invalid.getValue()) {
                    Edge e1 = new Edge(invalid.getKey().v1, vertex);
                    Edge e2 = new Edge(invalid.getKey().v2, vertex);

                    Set<Integer> vertices = edgeMap.get(e1);
                    if (vertices != null) {
                        vertices.remove(invalid.getKey().v2);
                        if (vertices.size() == 0)
                            edgeMap.remove(e1);
                    }

                    vertices = edgeMap.get(e2);
                    if (vertices != null) {
                        vertices.remove(invalid.getKey().v1);
                        if (vertices.size() == 0)
                            edgeMap.remove(e2);
                    }
                }
            }
            long t3 = System.currentTimeMillis();
            System.out.println("update map in " + (t3 - t2) + " ms");
        }
        long tEndLoop = System.currentTimeMillis();

        int size = edgeMap.entrySet().stream().filter(e -> e.getValue().size() > 0).collect(Collectors.toList()).size();

        long tOutput = System.currentTimeMillis();
        System.out.println("Finish ktruss, edges: " + size + ", " +
            "Total time: " + (tEndLoop - tStart) + " ms, " +
            "Loop time: " + (tEndLoop - tStartLoop) + "ms, " +
            "Count final edges time: " + (tOutput - tEndLoop));
    }

    public Map<Edge, Set<Integer>> findET(Edge[] edges) throws Exception {
        int[] degArray = new int[edges.length];
        // Construct degree array such that vertexId is the index of the array.
        for (Edge e : edges) {
            degArray[e.v1]++;
            degArray[e.v2]++;
        }

        int maxNeighborSize = edges.length / 20;
        // Fill and quickSort vertices array.
        int[] vertices = SequentialUtils.sortDegrees(degArray);

        // Construct neighborhood
        int[][] neighbors = new int[vertices.length][];
        int[] neighborCount = new int[vertices.length];
        boolean[] mark = new boolean[vertices.length];

        // Initialize neighbors array
        for(int i = 0; i < vertices.length; i ++)
            neighbors[vertices[i]] = new int[Math.min(maxNeighborSize, degArray[vertices[i]])];

        // Fill neighbors array
        for(Edge e : edges) {
            float diff = (degArray[e.v2] - degArray[e.v1]) + (e.v2 - e.v1) / (float) (e.v2 + e.v1);
            if (diff >= 0)
                neighbors[e.v1][neighborCount[e.v1]++] = e.v2;
            else
                neighbors[e.v2][neighborCount[e.v2]++] = e.v1;
        }

        Map<Edge, Set<Integer>> edgeNodes = new HashMap<>();
        for(int i = vertices.length - 1 ; i >= 0 ; i --) {
            int u = vertices[i]; // get current vertex id as u

            // since vertices are sorted based on degrees in async order, don't process all deg = 0 or deg = 1 vertices
            if (degArray[u] == 1)
                break;

            // mark all neighbors of u
            for (int j = 0; j < neighborCount[u]; j++)
                mark[neighbors[u][j]] = true;
            // iterate over neighbors of u
            int w = 0;
            for (int j = 0; j < neighborCount[u]; j++) {
                int v = neighbors[u][j];  // one neighbor of u

                // iterate over neighbors of v
                for (int k = 0; k < neighborCount[v]; k++) {
                    w = neighbors[v][k];  // one neighbor of v
                    if (mark[w]) {  // if w is marked, a triangle with {u,v,w} is found.
                        Edge e1 = new Edge(u, v);
                        Edge e2 = new Edge(u, w);
                        Edge e3 = new Edge(v, w);

                        // add w in front of e1 {u,v}
                        Set<Integer> nodes = edgeNodes.get(e1);
                        if (nodes == null) {
                            nodes = new HashSet<>();
                            edgeNodes.put(e1, nodes);
                        }
                        nodes.add(w);

                        // add v in front of e2 {u,w}
                        nodes = edgeNodes.get(e2);
                        if (nodes == null) {
                            nodes = new HashSet<>();
                            edgeNodes.put(e2, nodes);
                        }
                        nodes.add(v);

                        // add u in front of e3 {v,w}
                        nodes = edgeNodes.get(e3);
                        if (nodes == null) {
                            nodes = new HashSet<>();
                            edgeNodes.put(e3, nodes);
                        }
                        nodes.add(u);
                    }
                }
            }
            for (int j = 0; j < neighborCount[u]; j++)
                mark[neighbors[u][j]] = false;
        }
        return edgeNodes;
    }

}
