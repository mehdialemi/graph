package ir.ac.sbu.graph.ktruss.sequential;

import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Find triangles.
 */
public class TriangleSequential {

    public static void main(String[] args) throws Exception {
        String inputPath = "/home/mehdi/graph-data/com-amazon.ungraph.txt";
//        String inputPath = "/home/mehdi/graph-data/cit-Patents.txt";
//        String inputPath = "/home/mehdi/graph-data/Email-EuAll.txt";

        FileInputStream inputStream = new FileInputStream(inputPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        List<Edge> list = reader.lines().filter(line -> !line.startsWith("#"))
            .map(line -> line.split("\\s+"))
            .map(split -> new Edge(Integer.parseInt(split[0]), Integer.parseInt(split[1])))
            .filter(e -> e.v1 != e.v2)
            .collect(Collectors.toList());

        Edge[] edges = list.toArray(new Edge[0]);
        Map<Edge, List<Integer>> edgeTriangles = findET(edges);
    }

    public static Map<Edge, List<Integer>> findET(Edge[] edges) throws Exception {
        int[] degArray = new int[edges.length];
        // Construct degree array such that vertexId is the index of the array.
        for (Edge e : edges) {
            degArray[e.v1]++;
            degArray[e.v2]++;
        }

        int maxNeighborSize = edges.length / 20;
        // Fill and sort vertices array.
        int[] vertices = sort(degArray);

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

        Map<Edge, List<Integer>> edgeNodes = new HashMap<>();
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
                        List<Integer> nodes = edgeNodes.get(e1);
                        if (nodes == null) {
                            nodes = new ArrayList<>();
                            edgeNodes.put(e1, nodes);
                        }
                        nodes.add(w);

                        // add v in front of e2 {u,w}
                        nodes = edgeNodes.get(e2);
                        if (nodes == null) {
                            nodes = new ArrayList<>();
                            edgeNodes.put(e2, nodes);
                        }
                        nodes.add(v);

                        // add u in front of e3 {v,w}
                        nodes = edgeNodes.get(e3);
                        if (nodes == null) {
                            nodes = new ArrayList<>();
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

    public static Tuple2<List<int[]>, Set<Integer>[]> findEdgeTriangles(final Edge[] edges) throws Exception {
        // find max vertex Id in parallel
        long t1 = System.currentTimeMillis();
        int max = -1;
        for(Edge e : edges) {
            if (e.v1 > max)
                max = e.v1;
            if (e.v2 > max)
                max = e.v2;
        }
        long t2 = System.currentTimeMillis();
        System.out.println("Finding max in " + (t2 - t1) + " ms");

        int[] degArray = new int[max + 1];
        long t3 = System.currentTimeMillis();
        System.out.println("Construct degArray (AtomicInteger) in " + (t3 - t2) + " ms");

        // Construct degree array such that vertexId is the index of the array in parallel
        for (Edge e : edges) {
            degArray[e.v1]++;
            degArray[e.v2]++;
        }
        long t4 = System.currentTimeMillis();
        System.out.println("Fill degArray in " + (t4 - t3) + " ms");

        // Fill and sort vertices array.
        int[] vertices = sort(degArray);
        long t5 = System.currentTimeMillis();
        System.out.println("Sort degArray in " + (t5 - t4) + " ms");

        // Construct neighborhood
        List<Integer>[] neighbors = new List[vertices.length];

        // Initialize neighbors array
        for(int i = 0; i < vertices.length; i ++) {
            neighbors[vertices[i]] = new ArrayList<>();
        }
        long t6 = System.currentTimeMillis();
        System.out.println("Construct neighbors in " + (t6 - t5) + " ms");

        // Fill neighbors array
        for(int i = 0 ; i < edges.length ; i ++) {
            Edge e = edges[i];
            float diff = (degArray[e.v2] - degArray[e.v1]) + (e.v2 - e.v1) / (float) (e.v2 + e.v1);
            if (diff >= 0) {
                neighbors[e.v1].add(e.v2);
                neighbors[e.v1].add(i);
            } else {
                neighbors[e.v2].add(e.v1);
                neighbors[e.v2].add(i);
            }
        }
        long t7 = System.currentTimeMillis();
        System.out.println("Fill neighbors in " + (t7 - t6) + " ms");

        Set<Integer>[] eTriangles = new Set[edges.length];
        long t8 = System.currentTimeMillis();
        System.out.println("Construct eTriangles in " + (t8 - t7) + " ms");

        List<int[]> triangles = new ArrayList<>();

        long t9 = System.currentTimeMillis();
        System.out.println("Ready to triangle in " + (t9 - t8) + " ms");
        int triangleIndex = 0;
        for(int i = vertices.length - 1 ; i >= 0 ; i --) {
            int u = vertices[i]; // get current vertex id as u

            // since vertices are sorted based on degrees in async order, don't process all deg = 0 or deg = 1 vertices
            if (degArray[u] == 1)
                break;

            // construct a map of nodes to their edges
            List<Integer> uNeighbors = neighbors[u];
            Map<Integer, Integer> unEdges = new HashMap<>(uNeighbors.size() / 2);
            for(int ni = 0 ; ni < uNeighbors.size() ; ni += 2)
                unEdges.put(uNeighbors.get(ni), uNeighbors.get(ni + 1));

            // iterate over neighbors of u using edge info
            for(int j = 0 ; j < uNeighbors.size() ; j += 2) {
                int v = uNeighbors.get(j);
                int uv = uNeighbors.get(j + 1);
                // iterate over neighbors of v
                List<Integer> vNeighbors = neighbors[v];
                for(int k = 0 ; k < vNeighbors.size() ; k += 2) {
                    int w = vNeighbors.get(k);
                    Integer uw = unEdges.get(w);
                    if (uw != null) {
                        int vw = vNeighbors.get(k + 1);
                        if (eTriangles[uv] == null)
                            eTriangles[uv] = new HashSet<>();
                        eTriangles[uv].add(triangleIndex);

                        if (eTriangles[vw] == null)
                            eTriangles[vw] = new HashSet<>();
                        eTriangles[vw].add(triangleIndex);

                        if (eTriangles[uw] == null)
                            eTriangles[uw] = new HashSet<>();
                        eTriangles[uw].add(triangleIndex);

                        triangles.add(new int[]{uv, vw, uw});
                        triangleIndex ++;
                    }
                }
            }
        }

        long t10 = System.currentTimeMillis();
        System.out.println("Triangle finished in " + (t10 - t9) + " ms");
        return new Tuple2<>(triangles, eTriangles);
    }


    public static int[] sort(int[] degArray) {
        int min = degArray[0];
        int max = degArray[0];
        int length = max;
        for (int i = 1; i < degArray.length; i++) {
            if (degArray[i] < min) {
                min = degArray[i];
            } else if (degArray[i] > max) {
                max = degArray[i];
            }
            if (degArray[i] != 0)
                length = i + 1;
        }

        // init array of frequencies
        int[] counts = new int[max - min + 1];

        // init the frequencies
        int[] vertices = new int[length];
        for (int i = 0; i < vertices.length; i++) {
            int index = degArray[i] - min;
            counts[index]++;
        }

        counts[0]--;
        for (int i = 1; i < counts.length; i++) {
            counts[i] = counts[i] + counts[i - 1];
        }

        for (int i = vertices.length - 1; i >= 0; i--) {
            int index = counts[degArray[i] - min]--;
            vertices[index] = i;
        }

        return vertices;
    }

}
